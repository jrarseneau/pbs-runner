#!/usr/bin/env python3
"""
pbs-runner — Orchestrates proxmox-backup-client runs from a YAML config.

Key features:
- Fresh ZFS snapshots per run (use → back up → clean up)
- Union (bind-mount) mode: when snapshot=true and split_subfolders=false
  * Takes a recursive ZFS snapshot of the parent dataset
  * Binds the parent snapshot to a temp dir
  * Binds each child dataset’s snapshot into the right subpath
  * Adds --include-dev for each child bind
  * Backs up ONE pxar covering everything
- Per-repo environment injection (PBS_* env vars)
- Grouping: bundle | per-app-id | per-namespace
- Dry-run: prints plan, union mounts, include-dev, and full commands
- Robust logging and optional notifications (Healthchecks, Discord)
"""

import argparse
import collections
import datetime as dt
import json
import logging
import logging.handlers
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Tuple, Dict, Optional

try:
    import yaml
except Exception:
    print("Missing dependency: pyyaml. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(2)

# =========================
# Shell & small utilities
# =========================

def sh(cmd, *, capture=False, env: Optional[Dict[str, str]] = None):
    """Run a command (list) with optional env overrides. Returns (rc, stdout, stderr)."""
    import shlex
    try:
        merged_env = os.environ.copy()
        if env:
            merged_env.update(env)
        if capture:
            logging.debug("SH: %s", shlex.join(cmd))
            cp = subprocess.run(cmd, shell=False, check=False,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=merged_env)
            logging.debug("SH rc=%s, stdout=%r, stderr=%r", cp.returncode, cp.stdout, cp.stderr)
            return cp.returncode, cp.stdout, cp.stderr
        else:
            logging.debug("SH: %s", shlex.join(cmd))
            cp = subprocess.run(cmd, shell=False, check=False, env=merged_env)
            logging.debug("SH rc=%s", cp.returncode)
            return cp.returncode, "", ""
    except FileNotFoundError as e:
        logging.error("Command not found: %s", e)
        return 127, "", str(e)

def is_dir(p: Path):
    try:
        return p.is_dir()
    except Exception:
        return False

def list_immediate_subdirs(p: Path):
    try:
        return sorted([x for x in p.iterdir() if x.is_dir()])
    except Exception as e:
        logging.warning("List subdirs failed for %s: %s", p, e)
        return []

def ensure_parent_dir(f: Path):
    try:
        f.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.error("Create dir %s failed: %s", f.parent, e)

# =========================
# Label / ID / Namespace sanitization
# =========================

_LABEL_CHARS_RE = re.compile(r"[^A-Za-z0-9_-]+")

def sanitize_label(s: str) -> str:
    """PBS-safe token: only [A-Za-z0-9_-], must start with alnum."""
    if not s:
        return "x"
    s = _LABEL_CHARS_RE.sub("_", s)
    if not s[0].isalnum():
        s = "x_" + s
    return s

def sanitize_namespace(ns: str) -> str:
    parts = [sanitize_label(p) for p in ns.split("/") if p != ""]
    return "/".join(parts)

def make_label_from_path(p: Path) -> str:
    base = p.name.strip() or p.anchor.strip("/").replace("/", "_") or "root"
    return sanitize_label(base)

def dedupe_labels(labels: List[str]) -> List[str]:
    counts = collections.Counter(labels)
    used = {}
    out = []
    for lbl in labels:
        if counts[lbl] == 1 and lbl not in used:
            used[lbl] = 1
            out.append(lbl)
        else:
            idx = used.get(lbl, 0) + 1
            used[lbl] = idx
            out.append(f"{lbl}-{idx}")
    return out

# =========================
# Templating helpers
# =========================

def expand_template(tpl: str, *, name: str, section: str, host: str) -> str:
    now = dt.datetime.now()
    repl = {
        "name": name,
        "section": section,
        "host": host,
        "YYYY": f"{now.year:04d}",
        "MM": f"{now.month:02d}",
        "DD": f"{now.day:02d}",
    }
    out = tpl
    for k, v in repl.items():
        out = out.replace("{" + k + "}", v)
    return out

# =========================
# Logging
# =========================

def setup_logging(cfg_defaults: dict):
    log_cfg = (cfg_defaults or {}).get("log", {}) or {}
    level_name = (log_cfg.get("level") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(ch)

    log_file = log_cfg.get("file")
    if log_file:
        path = Path(log_file)
        ensure_parent_dir(path)
        fh = logging.handlers.RotatingFileHandler(
            str(path),
            maxBytes=int(log_cfg.get("rotate_max_bytes", 10 * 1024 * 1024)),
            backupCount=int(log_cfg.get("rotate_backups", 5))
        )
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        root.addHandler(fh)

# =========================
# Notifications
# =========================

def http_get(url: str, timeout=10):
    import urllib.request
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, r.read()
    except Exception as e:
        logging.warning("HTTP GET failed for %s: %s", url, e)
        return None, None

def http_post_json(url: str, payload: dict, timeout=10):
    import urllib.request
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except Exception as e:
        logging.warning("HTTP POST failed for %s: %s", url, e)
        return None, None

def notify_healthchecks(hc_url: str, event: str):
    if not hc_url:
        return
    suffix = {"start": "/start", "success": "", "failure": "/fail"}.get(event, "")
    url = hc_url.rstrip("/") + suffix
    logging.info("Healthchecks ping: %s", url)
    http_get(url)

def notify_discord(dc_url: str, content: str, prefix: str = "", notify_on=None, event: str = ""):
    if not dc_url:
        return
    notify_on = notify_on or []
    if notify_on and event and event not in notify_on:
        logging.debug("Discord skip event=%s not in %s", event, notify_on)
        return
    msg = f"{prefix} {content}".strip()
    status, _ = http_post_json(dc_url, {"content": msg})
    if not status or status < 200 or status >= 300:
        logging.warning("Discord notify failed (status=%s).", status)

# =========================
# ZFS helpers
# =========================

def zfs_available() -> bool:
    return sh(["bash", "-lc", "command -v zfs >/dev/null 2>&1"], capture=True)[0] == 0

def zfs_list_name_mount() -> List[Tuple[str, str]]:
    rc, out, _ = sh(["bash", "-lc", "zfs list -H -o name,mountpoint"], capture=True)
    if rc != 0:
        return []
    rows = []
    for line in out.splitlines():
        if not line.strip():
            continue
        name, mnt = line.split("\t")
        rows.append((name, mnt))
    return rows

def dataset_for_path(path: Path) -> Tuple[Optional[str], Optional[str]]:
    rows = zfs_list_name_mount()
    p = str(path.resolve())
    best = (None, None, 0)
    for ds, mnt in rows:
        if mnt == "-" or not mnt:
            continue
        if p == mnt or (p + "/").startswith(mnt.rstrip("/") + "/"):
            score = len(mnt)
            if score > best[2]:
                best = (ds, mnt, score)
    return best[0], best[1]

def descendant_datasets(parent_ds: str, parent_mnt: str) -> List[Tuple[str, str, str]]:
    """Return [(ds, mnt, relpath)] for all descendants incl. parent (relpath='' for parent)."""
    rows = zfs_list_name_mount()
    out = []
    pm = parent_mnt.rstrip("/")
    for ds, mnt in rows:
        if mnt == "-" or not mnt:
            continue
        if mnt == parent_mnt or mnt.startswith(pm + "/"):
            rel = mnt.replace(pm, "", 1).lstrip("/")
            out.append((ds, mnt, rel))
    # Sort parents before children to mount parent first
    out.sort(key=lambda x: (x[1].count("/"), x[1]))
    return out

def snapshot_path_for(ds_mnt: str, tag: str, target_subpath: Optional[str] = None) -> Path:
    """Return absolute snapshot path for mountpoint and tag, optionally append subpath."""
    base = Path(ds_mnt) / ".zfs" / "snapshot" / tag
    if target_subpath:
        return base / target_subpath
    return base

def create_snapshot(ds: str, tag: str, recursive=False) -> bool:
    args = ["zfs", "snapshot"] + (["-r"] if recursive else []) + [f"{ds}@{tag}"]
    rc, _, err = sh(args, capture=True)
    if rc == 0:
        logging.info("Created snapshot %s@%s%s", ds, tag, " (-r)" if recursive else "")
        return True
    logging.warning("Failed to create snapshot on %s: %s", ds, err.strip())
    return False

def destroy_snapshot(ds: str, tag: str, recursive=False):
    args = ["zfs", "destroy"] + (["-r"] if recursive else []) + [f"{ds}@{tag}"]
    logging.info("Destroy snapshot: %s@%s%s", ds, tag, " (-r)" if recursive else "")
    sh(args, capture=True)

def build_snapshot_tag(section_name: str) -> str:
    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"pbsbkp-{socket.gethostname()}-{section_name}-{ts}"

# =========================
# Union (bind) helpers
# =========================

def mktemp_dir(root: Path, prefix="pbs-union-") -> Path:
    # simple mktemp-like unique dir
    for _ in range(100):
        ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        p = root / f"{prefix}{ts}"
        try:
            p.mkdir(parents=True, exist_ok=False)
            return p
        except FileExistsError:
            time.sleep(0.01)
    p = root / f"{prefix}{os.getpid()}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def mount_bind(src: Path, dst: Path, readonly=True, dry=False) -> int:
    if dry:
        logging.info("[DRY RUN] mount --bind %s %s", src, dst)
        if readonly:
            logging.info("[DRY RUN] mount -o remount,ro,bind %s", dst)
        return 0
    rc, _, err = sh(["mount", "--bind", str(src), str(dst)], capture=True)
    if rc != 0:
        logging.warning("mount --bind failed: %s -> %s: %s", src, dst, err.strip())
        return rc
    if readonly:
        sh(["mount", "-o", "remount,ro,bind", str(dst)], capture=True)
    return 0

def umount_path(p: Path, dry=False) -> int:
    if dry:
        logging.info("[DRY RUN] umount %s", p)
        return 0
    rc, _, _ = sh(["umount", str(p)], capture=True)
    if rc != 0:
        rc, _, _ = sh(["umount", "-l", str(p)], capture=True)
    return rc

# =========================
# Planning model
# =========================

class PxarEntry:
    def __init__(
        self,
        label: str,
        src_path: Path,
        repositories: List[str],
        namespace: Optional[str],
        backup_id_override: Optional[str],
        note: str = "",
        snapshot_required: bool = False,
        warned: bool = False,
        include_devs: Optional[List[Path]] = None,
        union_root: Optional[Path] = None,
        cleanup_unmounts: Optional[List[Path]] = None,
        cleanup_union_root: Optional[Path] = None,
        destroy_snapshot_spec: Optional[Tuple[str, str, bool]] = None
    ):
        self.label = label
        self.src_path = src_path
        self.repositories = repositories
        self.namespace = namespace
        self.backup_id_override = backup_id_override
        self.note = note
        self.snapshot_required = snapshot_required
        self.warned = warned
        self.include_devs = include_devs or []
        self.union_root = union_root
        self.cleanup_unmounts = cleanup_unmounts or []
        self.cleanup_union_root = cleanup_union_root
        self.destroy_snapshot_spec = destroy_snapshot_spec  # (parent_ds, tag, recursive_bool)

    def as_arg(self) -> str:
        return f"{self.label}.pxar:{self.src_path}"

# =========================
# Repo env helpers
# =========================

def masked(s: Optional[str]) -> str:
    if not s:
        return ""
    if len(s) <= 4:
        return "*" * len(s)
    return s[:2] + "*" * (len(s) - 4) + s[-2:]

def repo_env_from_cfg(repos_cfg: Dict[str, dict], alias: str) -> Optional[Dict[str, str]]:
    meta = repos_cfg.get(alias)
    if not meta:
        logging.error("Repository alias '%s' not found in config 'repositories' section.", alias)
        return None
    repo = (meta.get("repository") or "").strip()
    pwd = (meta.get("password") or "").strip()
    fp = (meta.get("fingerprint") or "").strip()
    if not repo or not pwd:
        logging.error("Repository '%s' missing required fields (repository/password).", alias)
        return None
    env = {"PBS_REPOSITORY": repo, "PBS_PASSWORD": pwd}
    if fp:
        env["PBS_FINGERPRINT"] = fp
    return env

# =========================
# Core planner
# =========================

def plan_for_folder(folder_cfg, section_defaults, global_defaults, *, dry_run=False, section_name="host"):
    """
    - If snapshot=true & split_subfolders=false & union_mode!=off:
        * Detect parent dataset and descendants
        * Create a single recursive ZFS snapshot on parent
        * Build a union mount under union_mount_root
        * Bind parent snapshot at union root, then bind each child dataset snapshot into the correct subpath
        * Set include_devs for each child bind path
        * Create ONE pxar pointing at union root
    - Else: legacy behavior (split or not), mapping snapshot paths per item.
    """
    warnings = []
    entries: List[PxarEntry] = []

    eff = dict(global_defaults or {})
    eff.update(section_defaults or {})
    eff.update({k: v for k, v in folder_cfg.items() if k != "path"})

    path = Path(folder_cfg["path"]).resolve()
    repositories = folder_cfg.get("repositories") or []
    if not repositories:
        logging.info("Skipping %s (no repositories listed).", path)
        return entries, [], warnings

    if not is_dir(path):
        msg = f"Path not found or not a directory: {path}"
        logging.warning(msg)
        warnings.append(msg)
        return entries, [], warnings

    snapshot = bool(eff.get("snapshot", False))
    split = bool(eff.get("split_subfolders", False) or (eff.get("recursive", None) is False))
    fallback = bool(eff.get("fallback_to_live", True))
    label_override = eff.get("label")
    label_prefix = eff.get("label_prefix")
    skip_hidden = bool(eff.get("skip_hidden_subfolders", True))
    base_namespace = eff.get("namespace", None) or None
    backup_grouping = (eff.get("backup_grouping") or "bundle").strip()
    backup_id_tpl = eff.get("backup_id_template") or "{host}-{name}"
    ns_tpl = eff.get("namespace_template") or ""
    union_mode = (eff.get("union_mode") or "auto").lower()    # auto | off
    union_root_base = Path(eff.get("union_mount_root") or "/tmp")

    host = socket.gethostname()

    # ======= Union mode (default) when snapshot & not split =======
    if snapshot and not split and union_mode != "off":
        zfs_ok = zfs_available()
        if not zfs_ok:
            if fallback:
                logging.warning("ZFS unavailable; union mode not possible; fallback to live.")
            else:
                warnings.append(f"ZFS unavailable & fallback_to_live=false for {path}")
        else:
            parent_ds, parent_mnt = dataset_for_path(path)
            if not parent_ds or not parent_mnt:
                if fallback:
                    logging.warning("No dataset for %s; union not possible; fallback to live.", path)
                else:
                    warnings.append(f"No dataset & no fallback for {path}")
            else:
                # Enumerate descendants including parent
                desc = descendant_datasets(parent_ds, parent_mnt)
                tag = build_snapshot_tag(section_name)

                # Snapshot recursively (or plan it)
                if dry_run:
                    logging.info("[DRY RUN] would create recursive snapshot %s@%s (-r)", parent_ds, tag)
                    created_rec_snap = True
                else:
                    created_rec_snap = create_snapshot(parent_ds, tag, recursive=True)

                if not created_rec_snap:
                    if fallback:
                        logging.warning("Recursive snapshot failed; fallback to live for %s", path)
                    else:
                        warnings.append(f"Recursive snapshot failed & no fallback for {path}")
                else:
                    # Build union root
                    union_root = mktemp_dir(union_root_base)
                    cleanup_unmounts: List[Path] = []
                    include_devs: List[Path] = []

                    # Bind parent snapshot
                    parent_snap_path = snapshot_path_for(parent_mnt, tag)
                    if dry_run:
                        logging.info("[DRY RUN] would create union dir: %s", union_root)
                        mount_bind(parent_snap_path, union_root, readonly=True, dry=True)
                    else:
                        rc = mount_bind(parent_snap_path, union_root, readonly=True, dry=False)
                        if rc != 0:
                            logging.warning("Union mount failed; cleaning up and falling back to live for %s", path)
                            try:
                                union_root.rmdir()
                            except Exception:
                                pass
                            destroy_snapshot(parent_ds, tag, recursive=True)
                            parent_ds = None  # signal failure
                        else:
                            cleanup_unmounts.append(union_root)  # parent bind unmount last

                    if parent_ds:
                        # Bind each child dataset snapshot to union/<rel>
                        for ds, mnt, rel in desc:
                            if rel == "":
                                continue  # parent already bound
                            union_sub = union_root / rel
                            if dry_run:
                                logging.info("[DRY RUN] would ensure dir: %s", union_sub)
                            else:
                                union_sub.mkdir(parents=True, exist_ok=True)
                            ds_snap = snapshot_path_for(mnt, tag)
                            if dry_run:
                                mount_bind(ds_snap, union_sub, readonly=True, dry=True)
                                include_devs.append(union_sub)
                                cleanup_unmounts.insert(0, union_sub)  # unmount children first
                            else:
                                rc = mount_bind(ds_snap, union_sub, readonly=True, dry=False)
                                if rc == 0:
                                    include_devs.append(union_sub)
                                    cleanup_unmounts.insert(0, union_sub)
                                else:
                                    logging.warning("Failed to bind child snapshot %s -> %s; child data will be missing.", ds_snap, union_sub)

                        # Single pxar at union root
                        base_label = sanitize_label(label_override or make_label_from_path(path))
                        note = f"union at {union_root} from {parent_ds}@{tag}"
                        entry = PxarEntry(
                            label=base_label,
                            src_path=union_root,
                            repositories=repositories,
                            namespace=(base_namespace if base_namespace not in ("", None) else None),
                            backup_id_override=None,
                            note=note,
                            snapshot_required=False,
                            warned=False,
                            include_devs=include_devs,
                            union_root=union_root,
                            cleanup_unmounts=cleanup_unmounts,
                            cleanup_union_root=union_root,
                            destroy_snapshot_spec=(parent_ds, tag, True) if not dry_run else None
                        )
                        entries.append(entry)
                        return entries, [], warnings  # union path returns single entry

    # ======= Non-union path (legacy behavior) =======
    created_snaps: List[str] = []
    zfs_ok = zfs_available()
    tag = build_snapshot_tag(section_name)

    candidates = [path]
    if split:
        subs = [p for p in list_immediate_subdirs(path) if is_dir(p)]
        if skip_hidden:
            subs = [p for p in subs if not p.name.startswith(".")]
        if subs:
            candidates = subs
        else:
            logging.info("No subfolders under %s; using folder itself", path)

    # map dataset per candidate
    dataset_map = {}
    ds_to_mnt: Dict[str, str] = {}
    ds_created_snap: Dict[str, str] = {}

    if snapshot and zfs_ok:
        for cand in candidates:
            ds, mnt = dataset_for_path(cand)
            dataset_map[cand] = (ds, mnt)
            if ds and mnt:
                ds_to_mnt[ds] = mnt
        for ds in sorted(ds_to_mnt.keys()):
            if dry_run:
                logging.info("[DRY RUN] would create snapshot %s@%s", ds, tag)
                ds_created_snap[ds] = f"{ds}@{tag}"
            else:
                if create_snapshot(ds, tag, recursive=False):
                    ds_created_snap[ds] = f"{ds}@{tag}"
                    created_snaps.append(f"{ds}@{tag}")
                else:
                    logging.warning("Snapshot create failed for dataset %s", ds)
    elif snapshot and not zfs_ok:
        logging.warning("ZFS not available but snapshot=true; fallback_to_live=%s", fallback)

    for cand in candidates:
        warned = False
        snapshot_required = False
        src_path = cand
        note = ""
        if snapshot:
            if not zfs_ok:
                if fallback:
                    warned = True
                    note = "zfs unavailable; fallback to live"
                else:
                    snapshot_required = True
                    warnings.append(f"ZFS unavailable & fallback_to_live=false for {cand}")
            else:
                ds, mnt = dataset_map.get(cand, (None, None))
                if ds and mnt and ds in ds_created_snap:
                    snap_path = snapshot_path_for(mnt, tag)
                    if dry_run:
                        src_path = snap_path
                        note = f"[DRY RUN] using snapshot {ds}@{tag}"
                    else:
                        if snap_path and is_dir(snap_path):
                            src_path = snap_path
                            note = f"using snapshot {ds}@{tag}"
                        else:
                            if fallback:
                                warned = True
                                note = f"{ds}@{tag} created but .zfs mapping missing; fallback to live"
                            else:
                                snapshot_required = True
                                warnings.append(f"Snapshot mapping failed & no fallback for {cand}")
                else:
                    if fallback:
                        warned = True
                        note = "no dataset or snapshot; fallback to live"
                    else:
                        snapshot_required = True
                        warnings.append(f"No dataset/snapshot & no fallback for {cand}")

        base_label = make_label_from_path(cand if split else path)
        if label_prefix and split:
            base_label = sanitize_label(f"{label_prefix}-{base_label}")
        custom_label = (label_override if not split else base_label)
        label_final = sanitize_label(custom_label or base_label)

        entry_ns: Optional[str] = (base_namespace if base_namespace not in ("", None) else None)
        entry_bid: Optional[str] = None

        item_name = make_label_from_path(cand)
        if backup_grouping.lower() == "per-app-id":
            raw = expand_template(backup_id_tpl, name=item_name, section=section_name, host=host)
            entry_bid = sanitize_label(raw)
        elif backup_grouping.lower() == "per-namespace":
            rawns = expand_template(ns_tpl, name=item_name, section=section_name, host=host)
            entry_ns = sanitize_namespace(rawns) if rawns else None

        entries.append(PxarEntry(
            label=label_final,
            src_path=src_path,
            repositories=repositories,
            namespace=entry_ns,
            backup_id_override=entry_bid,
            note=note,
            snapshot_required=snapshot_required,
            warned=(warned and snapshot)
        ))

    return entries, created_snaps, warnings

def consolidate_and_dedupe(entries: List[PxarEntry]) -> List[PxarEntry]:
    labels = [e.label for e in entries]
    fixed = dedupe_labels(labels)
    for e, lbl in zip(entries, fixed):
        if e.label != lbl:
            logging.info("De-duped label %s -> %s", e.label, lbl)
        e.label = lbl
    return entries

# =========================
# Build & run per (repo_alias, namespace, backup-id)
# =========================

def build_pbc_command(entries: List[PxarEntry], ns: Optional[str], backup_id: str,
                      section_cfg, global_defaults, pbc_binary: str) -> List[str]:
    eff = {}
    eff.update(global_defaults or {})
    eff.update(section_cfg or {})
    backup_type = eff.get("backup_type") or "host"
    extra_args = eff.get("extra_args") or []

    args = [pbc_binary, "backup"]
    for e in entries:
        args.append(f"{e.label}.pxar:{e.src_path}")
    # include-dev for any union-mounted children
    include_paths = []
    for e in entries:
        for inc in (e.include_devs or []):
            include_paths.append(str(inc))
    for inc in include_paths:
        args.extend(["--include-dev", inc])

    if ns:
        args.extend(["--ns", ns])
    if backup_id:
        args.extend(["--backup-id", backup_id])
    if backup_type:
        args.extend(["--backup-type", backup_type])
    args.extend(extra_args)
    return args

def run_command(args: List[str], env: Dict[str, str]) -> int:
    import shlex
    safe_env_log = {
        "PBS_REPOSITORY": env.get("PBS_REPOSITORY", ""),
        "PBS_PASSWORD": masked(env.get("PBS_PASSWORD")),
        "PBS_FINGERPRINT": env.get("PBS_FINGERPRINT", ""),
    }
    logging.info("Env: PBS_REPOSITORY=%s PBS_PASSWORD=%s PBS_FINGERPRINT=%s",
                 safe_env_log["PBS_REPOSITORY"], safe_env_log["PBS_PASSWORD"], safe_env_log["PBS_FINGERPRINT"])
    logging.info("Executing: %s", shlex.join(args))
    rc, _, _ = sh(args, capture=False, env=env)
    logging.info("proxmox-backup-client exit code: %s", rc)
    return rc

def print_plan_for_group(repo_alias: str, ns: Optional[str], backup_id: str, entries: List[PxarEntry], section_name: str):
    ns_disp = ns if ns is not None else "(root)"
    logging.info("=== PLAN for section: %s | repo-alias: %s | ns: %s | bid: %s ===", section_name, repo_alias, ns_disp, backup_id)
    for e in entries:
        flag = " (REQUIRES SNAPSHOT BUT UNAVAILABLE)" if e.snapshot_required else ""
        warn = " [FALLBACK TO LIVE]" if e.warned else ""
        note = f"  # {e.note}" if e.note else ""
        logging.info("  - %s.pxar:%s%s%s%s", e.label, e.src_path, flag, warn, note)
        for inc in (e.include_devs or []):
            logging.info("    --include-dev %s", inc)

# =========================
# Main
# =========================

def main():
    ap = argparse.ArgumentParser(description="pbs-runner: Plan & run proxmox-backup-client backups from YAML config")
    ap.add_argument("-c", "--config", required=True, help="Path to YAML config")
    ap.add_argument("--section", default="host", help="Top-level section to run (host|vm|ct). Default: host")
    ap.add_argument("--dry-run", action="store_true", help="Print actions & commands without executing")
    ap.add_argument("--pbc-binary", default=None, help="Override path/name of proxmox-backup-client")
    args = ap.parse_args()

    cfg_path = Path(args.config)
    if not cfg_path.exists():
        print(f"Config not found: {cfg_path}", file=sys.stderr)
        sys.exit(2)
    cfg = yaml.safe_load(cfg_path.read_text()) or {}

    global_defaults = cfg.get("defaults", {}) or {}
    setup_logging(global_defaults)

    notifications = cfg.get("notifications", {}) or {}
    hc_url = (notifications.get("healthcheck_url") or "").strip()
    dc_url = (notifications.get("discord_webhook") or "").strip()
    dc_evts = notifications.get("discord_notify_on") or []
    dc_pref = notifications.get("discord_prefix") or ""

    repositories_cfg = cfg.get("repositories", {}) or {}
    if not repositories_cfg:
        logging.warning("No 'repositories' configured; folders referencing repos will fail.")

    section = cfg.get(args.section, {}) or {}
    section_defaults = {k: v for k, v in section.items() if k != "folders"}
    folders = section.get("folders", []) or []

    pbc_binary = args.pbc_binary or global_defaults.get("pbc_binary") or "proxmox-backup-client"

    if not folders:
        logging.info("No folders under section '%s'. Nothing to do.", args.section)
        sys.exit(0)

    # notifications: start
    if not args.dry_run:
        notify_healthchecks(hc_url, "start")
        notify_discord(dc_url, f"Backup run started on {socket.gethostname()} (section: {args.section})",
                       prefix=dc_pref, notify_on=dc_evts, event="start")

    all_entries: List[PxarEntry] = []
    all_warnings: List[str] = []
    union_cleanups: List[Tuple[List[Path], Optional[Path], Optional[Tuple[str, str, bool]]]] = []  # (umounts, union_root, destroy_spec)

    try:
        for fcfg in folders:
            if isinstance(fcfg, str):
                fcfg = {"path": fcfg}
            if "path" not in fcfg:
                msg = f"Folder entry missing 'path': {fcfg}"
                logging.warning(msg)
                all_warnings.append(msg)
                continue
            entries, _created_snaps_unused, warns = plan_for_folder(
                fcfg, section_defaults, global_defaults, dry_run=args.dry_run, section_name=args.section
            )
            for e in entries:
                if e.cleanup_unmounts or e.cleanup_union_root or e.destroy_snapshot_spec:
                    union_cleanups.append((e.cleanup_unmounts, e.cleanup_union_root, e.destroy_snapshot_spec))
            all_entries.extend(entries)
            all_warnings.extend(warns)

        if not all_entries:
            logging.info("No eligible folders (none had 'repositories' or all skipped).")
            sys.exit(0)

        all_entries = consolidate_and_dedupe(all_entries)

        default_backup_id = section_defaults.get("backup_id") or socket.gethostname()
        groups: Dict[Tuple[str, Optional[str], str], List[PxarEntry]] = collections.defaultdict(list)
        for e in all_entries:
            eff_bid = e.backup_id_override or default_backup_id
            for repo_alias in e.repositories:
                groups[(repo_alias, e.namespace, eff_bid)].append(e)

        exit_code = 0

        for (repo_alias, ns, bid), entries in groups.items():
            had_fallback = any(e.warned for e in entries)
            print_plan_for_group(repo_alias, ns, bid, entries, args.section)

            env = repo_env_from_cfg(repositories_cfg, repo_alias)
            if env is None:
                logging.error("Skipping group (repo-alias=%s) due to missing/invalid repository config.", repo_alias)
                exit_code = exit_code or 2
                continue

            cmd = build_pbc_command(entries, ns, bid, section_defaults, global_defaults, pbc_binary)

            if args.dry_run:
                import shlex
                logging.info("DRY RUN - would set env: PBS_REPOSITORY=%s PBS_PASSWORD=%s PBS_FINGERPRINT=%s",
                             env.get("PBS_REPOSITORY", ""),
                             masked(env.get("PBS_PASSWORD")),
                             env.get("PBS_FINGERPRINT", ""))
                logging.info("DRY RUN - would execute: %s", shlex.join(cmd))
                continue

            rc = run_command(cmd, env=env)
            if rc != 0:
                exit_code = rc
                logging.error("Backup FAILED for repo-alias=%s ns=%s bid=%s (rc=%s).", repo_alias, ns or "(root)", bid, rc)
                notify_healthchecks(hc_url, "failure")
                notify_discord(
                    dc_url,
                    f"❌ Backup FAILED on {socket.gethostname()} repo={repo_alias} ns={ns or '(root)'} bid={bid} (section: {args.section}) rc={rc}",
                    prefix=dc_pref, notify_on=dc_evts, event="failure"
                )
            else:
                if had_fallback:
                    msg = f"⚠️ Backup SUCCEEDED with FALLBACK(S) on {socket.gethostname()} repo={repo_alias} ns={ns or '(root)'} bid={bid} (section: {args.section}). See logs."
                    logging.warning(msg)
                    notify_discord(dc_url, msg, prefix=dc_pref, notify_on=dc_evts, event="fallback")
                else:
                    logging.info("Backup SUCCESS for repo=%s ns=%s bid=%s.", repo_alias, ns or "(root)", bid)

        if not args.dry_run:
            if exit_code == 0:
                notify_healthchecks(hc_url, "success")
                notify_discord(dc_url,
                               f"✅ Backup SUCCESS on {socket.gethostname()} (section: {args.section})",
                               prefix=dc_pref, notify_on=dc_evts, event="success")

        if args.dry_run:
            sys.exit(0)
        else:
            sys.exit(exit_code)

    finally:
        # Cleanup: unmount union binds and remove union roots; destroy recursive snapshots
        for umounts, union_root, destroy_spec in union_cleanups:
            for m in umounts:          # children first
                umount_path(m, dry=args.dry_run)
            if union_root:
                umount_path(union_root, dry=args.dry_run)  # parent last
                if args.dry_run:
                    logging.info("[DRY RUN] rmdir %s", union_root)
                else:
                    try:
                        union_root.rmdir()
                    except Exception:
                        pass

            if destroy_spec:
                ds, tag, rec = destroy_spec
                if args.dry_run:
                    logging.info("[DRY RUN] would destroy snapshot %s@%s%s", ds, tag, " (-r)" if rec else "")
                else:
                    destroy_snapshot(ds, tag, recursive=rec)

        if not args.dry_run:
            notify_discord(dc_url, f"Backup run finished on {socket.gethostname()} (section: {args.section})",
                           prefix=dc_pref, notify_on=dc_evts, event="finish")

if __name__ == "__main__":
    main()
