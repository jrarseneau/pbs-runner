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
import fcntl
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

try:
    import xml.etree.ElementTree as ET
except Exception:
    print("Missing dependency: xml. This should be part of Python standard library.", file=sys.stderr)
    sys.exit(2)

# =========================
# Shell & small utilities
# =========================

# Global to hold the lock file descriptor
_lockfile_fd = None

def acquire_single_instance_lock(lock_path: str = "/var/run/pbs-runner.lock"):
    """
    Acquire an exclusive lock to ensure only one instance of pbs-runner runs at a time.
    Uses fcntl file locking which is automatically released when the process exits.

    Returns: file descriptor (stored globally) or exits with error code 3 if lock fails.
    """
    global _lockfile_fd

    # Ensure parent directory exists
    lock_file = Path(lock_path)
    try:
        lock_file.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"ERROR: Cannot create lock directory {lock_file.parent}: {e}", file=sys.stderr)
        sys.exit(3)

    try:
        # Open lock file (create if doesn't exist)
        _lockfile_fd = open(str(lock_file), 'w')

        # Try to acquire exclusive lock (non-blocking)
        fcntl.flock(_lockfile_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

        # Write our PID to the file (for debugging purposes)
        _lockfile_fd.write(f"{os.getpid()}\n")
        _lockfile_fd.flush()

        logging.debug("Acquired single-instance lock: %s", lock_path)

    except BlockingIOError:
        # Another instance is already running
        print(f"ERROR: Another instance of pbs-runner is already running.", file=sys.stderr)
        print(f"Lock file: {lock_path}", file=sys.stderr)
        sys.exit(3)
    except Exception as e:
        print(f"ERROR: Failed to acquire lock {lock_path}: {e}", file=sys.stderr)
        sys.exit(3)

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

def sh_with_logging(cmd, *, env: Optional[Dict[str, str]] = None):
    """
    Run a command and stream its output line-by-line to the logging system.
    This ensures output appears in both console and log files.
    Returns exit code.
    """
    import shlex
    try:
        merged_env = os.environ.copy()
        if env:
            merged_env.update(env)

        logging.debug("SH_WITH_LOGGING: %s", shlex.join(cmd))

        # Start the process with pipes for stdout/stderr
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Merge stderr into stdout
            text=True,
            bufsize=1,  # Line buffered
            env=merged_env
        )

        # Stream output line-by-line
        if process.stdout:
            for line in process.stdout:
                line = line.rstrip('\n\r')
                if line:  # Skip empty lines
                    logging.info(line)

        # Wait for process to complete
        rc = process.wait()
        logging.debug("SH_WITH_LOGGING rc=%s", rc)
        return rc

    except FileNotFoundError as e:
        logging.error("Command not found: %s", e)
        return 127
    except Exception as e:
        logging.error("Command execution failed: %s", e)
        return 1

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

def expand_template(tpl: str, *, name: str = "", section: str = "", host: str = "", vm: str = "") -> str:
    now = dt.datetime.now()
    repl = {
        "name": name,
        "section": section,
        "host": host,
        "vm": vm,  # VM name for vm: section backups
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

def cleanup_orphaned_snapshots(hostname: str, max_age_hours: int = 24, dry_run: bool = False) -> Tuple[int, int]:
    """
    Clean up orphaned pbsbkp snapshots older than max_age_hours.

    This function identifies snapshots created by previous pbs-runner runs that were
    not properly cleaned up due to crashes, kills, or system reboots.

    Args:
        hostname: Current hostname (snapshots are hostname-specific)
        max_age_hours: Maximum age in hours before snapshot is considered orphaned
        dry_run: If True, only log what would be deleted

    Returns:
        Tuple of (total_found, total_deleted)
    """
    if not zfs_available():
        logging.debug("ZFS not available, skipping orphaned snapshot cleanup")
        return 0, 0

    # List all snapshots matching our pattern
    rc, out, err = sh(["zfs", "list", "-t", "snapshot", "-H", "-o", "name,creation"], capture=True)
    if rc != 0:
        logging.warning("Failed to list ZFS snapshots for cleanup: %s", err.strip())
        return 0, 0

    prefix = f"pbsbkp-{hostname}-"
    now = dt.datetime.now()
    threshold = now - dt.timedelta(hours=max_age_hours)

    total_found = 0
    total_deleted = 0

    for line in out.splitlines():
        if not line.strip():
            continue

        parts = line.split("\t")
        if len(parts) < 2:
            continue

        snapshot_name = parts[0].strip()
        creation_str = parts[1].strip()

        # Check if this is one of our pbsbkp snapshots
        if "@" not in snapshot_name:
            continue

        dataset, tag = snapshot_name.split("@", 1)
        if not tag.startswith(prefix):
            continue

        total_found += 1

        # Parse timestamp from snapshot name: pbsbkp-{hostname}-{section}-{YYYYMMDD-HHMMSS}
        # Example: pbsbkp-myserver-host-20251108-143022
        try:
            # Extract the timestamp portion (last part after last dash group)
            # Format: YYYYMMDD-HHMMSS
            parts = tag.rsplit("-", 2)
            if len(parts) >= 2:
                date_part = parts[-2]  # YYYYMMDD
                time_part = parts[-1]  # HHMMSS

                # Parse the timestamp
                timestamp_str = f"{date_part}-{time_part}"
                snapshot_time = dt.datetime.strptime(timestamp_str, "%Y%m%d-%H%M%S")

                # Check if snapshot is older than threshold
                if snapshot_time < threshold:
                    age_hours = (now - snapshot_time).total_seconds() / 3600
                    if dry_run:
                        logging.info("[DRY RUN] would delete orphaned snapshot %s (age: %.1f hours)",
                                   snapshot_name, age_hours)
                        total_deleted += 1
                    else:
                        logging.info("Deleting orphaned snapshot %s (age: %.1f hours)",
                                   snapshot_name, age_hours)
                        # Determine if snapshot is recursive (has child snapshots)
                        # For safety, we'll use non-recursive delete and let ZFS handle dependencies
                        rc_del, _, err_del = sh(["zfs", "destroy", snapshot_name], capture=True)
                        if rc_del == 0:
                            total_deleted += 1
                        else:
                            logging.warning("Failed to delete orphaned snapshot %s: %s",
                                          snapshot_name, err_del.strip())
                else:
                    age_hours = (now - snapshot_time).total_seconds() / 3600
                    logging.debug("Snapshot %s is recent (age: %.1f hours), keeping",
                                snapshot_name, age_hours)
            else:
                logging.debug("Could not parse timestamp from snapshot name: %s", tag)

        except ValueError as e:
            logging.debug("Could not parse timestamp from snapshot %s: %s", snapshot_name, e)
            continue

    if total_found > 0:
        if dry_run:
            logging.info("Orphaned snapshot cleanup [DRY RUN]: found %d pbsbkp snapshots, would delete %d",
                       total_found, total_deleted)
        else:
            logging.info("Orphaned snapshot cleanup: found %d pbsbkp snapshots, deleted %d",
                       total_found, total_deleted)
    else:
        logging.debug("No orphaned pbsbkp snapshots found")

    return total_found, total_deleted

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
# VM / libvirt helpers
# =========================

def virsh_available() -> bool:
    """Check if virsh command is available."""
    return sh(["bash", "-lc", "command -v virsh >/dev/null 2>&1"], capture=True)[0] == 0

def get_vm_state(vm_name: str, virsh_uri: str = "qemu:///system") -> str:
    """
    Get VM state via virsh.
    Returns: 'running', 'shut off', 'paused', or 'unknown'
    """
    if not virsh_available():
        return "unknown"

    rc, out, _ = sh(["virsh", "-c", virsh_uri, "domstate", vm_name], capture=True)
    if rc == 0:
        state = out.strip().lower()
        return state
    return "unknown"

def parse_disk_element(disk_elem):
    """
    Parse a libvirt <disk> element and extract source path and metadata.

    Examples:
    <disk type='file' device='disk'>
      <source file='/mnt/user/domains/vm1/vdisk1.qcow2'/>
      <target dev='vda' bus='virtio'/>
      <driver name='qemu' type='qcow2'/>
    </disk>

    <disk type='block' device='disk'>
      <source dev='/dev/zvol/tank/vms/vm1/disk0'/>
      <target dev='vdb' bus='virtio'/>
    </disk>

    Returns dict with disk info or None if not a valid disk
    """
    disk_device = disk_elem.get('device', 'disk')
    if disk_device != 'disk':
        # Skip cdrom, floppy, etc.
        return None

    disk_type = disk_elem.get('type')  # 'file' or 'block'

    source_elem = disk_elem.find('source')
    if source_elem is None:
        return None

    # Get source path
    source_path = None
    if disk_type == 'file':
        source_path = source_elem.get('file')
    elif disk_type == 'block':
        source_path = source_elem.get('dev')

    if not source_path:
        return None

    # Get target device name
    target_elem = disk_elem.find('target')
    target_dev = target_elem.get('dev') if target_elem is not None else 'unknown'

    # Get driver type (qcow2, raw, etc.)
    driver_elem = disk_elem.find('driver')
    driver_type = driver_elem.get('type') if driver_elem is not None else 'raw'

    return {
        'source_path': Path(source_path),
        'disk_type': disk_type,      # 'file' or 'block'
        'target_dev': target_dev,    # 'vda', 'vdb', etc.
        'format': driver_type,       # 'qcow2', 'raw', etc.
    }

def parse_vm_xml(xml_file: Path) -> Optional[dict]:
    """
    Parse libvirt VM XML and extract VM name and disk information.
    Returns dict with VM details or None if parsing fails.
    """
    try:
        tree = ET.parse(str(xml_file))
        root = tree.getroot()

        # Extract VM name
        name_elem = root.find('name')
        if name_elem is None or not name_elem.text:
            logging.warning("No <name> in %s, skipping", xml_file)
            return None

        vm_name = name_elem.text.strip()

        # Extract all disk sources
        disks = []
        for disk_elem in root.findall('.//devices/disk'):
            disk_info = parse_disk_element(disk_elem)
            if disk_info:
                disks.append(disk_info)

        if not disks:
            logging.debug("VM %s has no disks, skipping", vm_name)
            return None

        return {
            'name': vm_name,
            'config_file': xml_file,
            'disks': disks,
        }
    except Exception as e:
        logging.warning("Failed to parse %s: %s", xml_file, e)
        return None

def should_exclude_vm(vm_name: str, filters: dict) -> bool:
    """
    Check if VM should be excluded based on include_only/exclude_vms filters.
    """
    import fnmatch

    exclude = filters.get('exclude_vms', [])
    include = filters.get('include_only', [])

    # If include_only specified, must match at least one pattern
    if include:
        if not any(fnmatch.fnmatch(vm_name, pattern) for pattern in include):
            logging.debug("VM %s excluded (not in include_only)", vm_name)
            return True

    # Check exclusions
    if any(fnmatch.fnmatch(vm_name, pattern) for pattern in exclude):
        logging.debug("VM %s excluded (matches exclude pattern)", vm_name)
        return True

    return False

def extract_zvol_dataset(zvol_path: Path) -> Optional[str]:
    """
    Extract dataset name from zvol device path.
    /dev/zvol/tank/vms/vm1/disk0 -> tank/vms/vm1/disk0
    """
    path_str = str(zvol_path)
    if '/dev/zvol/' in path_str:
        return path_str.replace('/dev/zvol/', '')
    return None

def discover_vms_from_libvirt(auto_discover_cfg: dict, section_defaults: dict) -> List[dict]:
    """
    Scan libvirt config directory and parse VM XMLs.
    Returns list of VM entries ready for backup planning.
    """
    config_path = auto_discover_cfg.get('config_path', '/etc/libvirt/qemu')
    config_dir = Path(config_path)

    if not config_dir.exists() or not config_dir.is_dir():
        logging.warning("libvirt config path %s not found or not a directory, skipping auto-discovery", config_dir)
        return []

    vm_entries = []
    repositories = auto_discover_cfg.get('repositories', [])

    if not repositories:
        logging.warning("No repositories specified in auto_discover, VMs will be skipped")
        return []

    for xml_file in sorted(config_dir.glob("*.xml")):
        vm_data = parse_vm_xml(xml_file)

        if vm_data and not should_exclude_vm(vm_data['name'], auto_discover_cfg):
            vm_data['repositories'] = repositories
            vm_data['namespace'] = auto_discover_cfg.get('namespace')
            vm_entries.append(vm_data)
            logging.info("Discovered VM: %s with %d disk(s)", vm_data['name'], len(vm_data['disks']))

    return vm_entries

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
        destroy_snapshot_spec: Optional[Tuple[str, str, bool]] = None,
        archive_type: str = "pxar"  # pxar, img, conf, log
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
        self.archive_type = archive_type

    def as_arg(self) -> str:
        return f"{self.label}.{self.archive_type}:{self.src_path}"

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

    # Track which datasets have been assigned for cleanup to avoid duplicates
    ds_assigned_for_cleanup = set()

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

        # Determine if we need to cleanup snapshot for this entry
        destroy_spec = None
        if snapshot and zfs_ok:
            ds, mnt = dataset_map.get(cand, (None, None))
            if ds and ds in ds_created_snap and ds not in ds_assigned_for_cleanup:
                # Assign this snapshot for cleanup (only once per dataset)
                destroy_spec = (ds, tag, False) if not dry_run else None
                ds_assigned_for_cleanup.add(ds)

        entries.append(PxarEntry(
            label=label_final,
            src_path=src_path,
            repositories=repositories,
            namespace=entry_ns,
            backup_id_override=entry_bid,
            note=note,
            snapshot_required=snapshot_required,
            warned=(warned and snapshot),
            destroy_snapshot_spec=destroy_spec
        ))

    return entries, created_snaps, warnings

def plan_vm_backup(vm_entry: dict, section_defaults: dict, global_defaults: dict,
                   dry_run=False, section_name="vm") -> Tuple[List[PxarEntry], List[Tuple[str, str, bool]], List[str]]:
    """
    Plan backup for a single VM.

    Strategy:
    1. For each disk, determine if it's file-based or zvol
    2. For file disks: find parent ZFS dataset and snapshot it
    3. For zvol disks: snapshot the zvol directly
    4. Build snapshot paths for each disk
    5. Fallback to live if snapshot fails and fallback_to_live=true
    6. Return PxarEntry list (one per disk + one for config)

    Returns: (entries, snapshots_created, warnings)
        snapshots_created: [(dataset, tag, recursive), ...]
    """
    eff = dict(global_defaults or {})
    eff.update(section_defaults or {})

    snapshot = bool(eff.get("snapshot", False))
    fallback = bool(eff.get("fallback_to_live", True))
    check_state = bool(eff.get("check_vm_state", True))
    warn_if_running = bool(eff.get("warn_if_running", True))

    vm_name = vm_entry['name']
    config_file = vm_entry['config_file']
    disks = vm_entry['disks']
    repositories = vm_entry['repositories']
    base_namespace = vm_entry.get('namespace')

    # Sanitize VM name for use as backup-id (PBS requires no spaces, special chars)
    vm_backup_id = sanitize_label(vm_name)
    if vm_backup_id != vm_name:
        logging.info("VM name '%s' sanitized to '%s' for backup-id", vm_name, vm_backup_id)

    if not repositories:
        logging.info("VM %s has no repositories, skipping", vm_name)
        return [], [], []

    warnings = []
    entries = []
    snapshots_created = []  # Track (dataset, tag, recursive) for cleanup

    # Check VM state
    if check_state:
        virsh_uri = eff.get('libvirt', {}).get('virsh_uri', 'qemu:///system')
        state = get_vm_state(vm_name, virsh_uri)
        if state == 'running' and warn_if_running:
            logging.warning("VM %s is currently running - backing up live VM", vm_name)
        elif state != 'unknown':
            logging.info("VM %s state: %s", vm_name, state)

    # Build snapshot tag
    tag = build_snapshot_tag(f"{section_name}-{vm_name}")
    zfs_ok = zfs_available()

    # === Strategy: Handle each disk independently, snapshot as needed ===
    disk_to_snapshot_path = {}  # Maps original disk path -> snapshot path
    dataset_snapshots = {}      # Track which datasets/zvols we've snapshotted

    for disk_info in disks:
        source_path = disk_info['source_path']
        disk_type = disk_info['disk_type']

        # Check if source path exists
        if not source_path.exists():
            msg = f"VM {vm_name}: disk {source_path} does not exist"
            logging.error(msg)
            if not fallback:
                warnings.append(msg)
                continue

        if snapshot and zfs_ok:
            if disk_type == 'block' and '/dev/zvol/' in str(source_path):
                # It's a zvol: /dev/zvol/tank/vms/vm1/disk0
                zvol_ds = extract_zvol_dataset(source_path)
                if zvol_ds:
                    # Snapshot the zvol itself
                    if zvol_ds not in dataset_snapshots:
                        if dry_run:
                            logging.info("[DRY RUN] would snapshot zvol %s@%s", zvol_ds, tag)
                            dataset_snapshots[zvol_ds] = True
                        else:
                            if create_snapshot(zvol_ds, tag, recursive=False):
                                dataset_snapshots[zvol_ds] = True
                                snapshots_created.append((zvol_ds, tag, False))
                            else:
                                logging.warning("Failed to snapshot zvol %s", zvol_ds)

                    if dataset_snapshots.get(zvol_ds):
                        # Snapshot path for zvol: /dev/zvol/<dataset>@<snapshot>
                        snap_path = Path(f"/dev/zvol/{zvol_ds}@{tag}")
                        disk_to_snapshot_path[source_path] = snap_path
                    else:
                        # Snapshot failed
                        if fallback:
                            logging.warning("Zvol snapshot failed for %s, using live disk", source_path)
                            disk_to_snapshot_path[source_path] = source_path
                        else:
                            warnings.append(f"Zvol snapshot failed & no fallback for {vm_name} disk {source_path}")
                else:
                    # Not a zvol format we recognize
                    if fallback:
                        logging.warning("Cannot parse zvol path %s, using live disk", source_path)
                        disk_to_snapshot_path[source_path] = source_path
                    else:
                        warnings.append(f"Cannot parse zvol path & no fallback for {vm_name} disk {source_path}")

            elif disk_type == 'file':
                # Regular file: /mnt/user/domains/vm1/vdisk1.qcow2
                # Find parent dataset
                ds, mnt = dataset_for_path(source_path.parent)
                if ds and mnt:
                    # Snapshot the dataset containing the file
                    if ds not in dataset_snapshots:
                        if dry_run:
                            logging.info("[DRY RUN] would snapshot %s@%s", ds, tag)
                            dataset_snapshots[ds] = True
                        else:
                            if create_snapshot(ds, tag, recursive=False):
                                dataset_snapshots[ds] = True
                                snapshots_created.append((ds, tag, False))
                            else:
                                logging.warning("Failed to snapshot dataset %s", ds)

                    if dataset_snapshots.get(ds):
                        # Build snapshot path via .zfs/snapshot
                        rel_path = source_path.relative_to(mnt)
                        snap_path = snapshot_path_for(mnt, tag, str(rel_path))
                        disk_to_snapshot_path[source_path] = snap_path
                    else:
                        # Snapshot failed
                        if fallback:
                            logging.warning("Dataset snapshot failed for %s, using live disk", source_path)
                            disk_to_snapshot_path[source_path] = source_path
                        else:
                            warnings.append(f"Dataset snapshot failed & no fallback for {vm_name} disk {source_path}")
                else:
                    # No ZFS dataset found
                    if fallback:
                        logging.warning("No ZFS dataset for %s, using live disk", source_path)
                        disk_to_snapshot_path[source_path] = source_path
                    else:
                        warnings.append(f"No ZFS dataset & no fallback for {vm_name} disk {source_path}")
            else:
                # Unknown disk type
                if fallback:
                    logging.warning("Unknown disk type %s for %s, using live", disk_type, source_path)
                    disk_to_snapshot_path[source_path] = source_path
                else:
                    warnings.append(f"Unknown disk type & no fallback for {vm_name} disk {source_path}")

        elif snapshot and not zfs_ok:
            # ZFS not available but snapshot requested
            if fallback:
                logging.warning("ZFS unavailable for VM %s disk %s, using live", vm_name, source_path)
                disk_to_snapshot_path[source_path] = source_path
            else:
                warnings.append(f"ZFS unavailable & no fallback for {vm_name} disk {source_path}")
        else:
            # snapshot=false, use live
            disk_to_snapshot_path[source_path] = source_path

    # === Build PxarEntry for each disk ===
    for disk_info in disks:
        source_path = disk_info['source_path']
        if source_path not in disk_to_snapshot_path:
            continue  # Skipped due to error

        backup_path = disk_to_snapshot_path[source_path]
        target_dev = disk_info['target_dev']

        # Label: use drive-<target> format (e.g., drive-vda, drive-scsi0)
        # This matches Proxmox VE backup naming convention
        label = f"drive-{target_dev}"
        label = sanitize_label(label)

        is_using_live = (backup_path == source_path and snapshot)

        entry = PxarEntry(
            label=label,
            src_path=backup_path,
            repositories=repositories,
            namespace=base_namespace,
            backup_id_override=vm_backup_id,  # Each VM = separate backup-id (sanitized)
            note=f"VM {vm_name} disk {target_dev} ({source_path})" + (" [LIVE]" if is_using_live else ""),
            snapshot_required=False,
            warned=is_using_live,
            archive_type="img",  # Disk images use .img type (PBS creates .img.fidx on server)
        )
        entries.append(entry)

    # === Add VM config file ===
    if config_file and config_file.exists():
        # Use sanitized VM name as label (e.g., Windows_11)
        # PBS archive-name can only contain alphanumerics, hyphens, and underscores
        # Result: Windows_11.conf.blob on server
        config_label = vm_backup_id

        config_entry = PxarEntry(
            label=config_label,
            src_path=config_file,
            repositories=repositories,
            namespace=base_namespace,
            backup_id_override=vm_backup_id,  # Use sanitized backup-id
            note=f"VM {vm_name} configuration",
            snapshot_required=False,
            warned=False,
            archive_type="conf",  # Config files use .conf type (PBS creates .conf.blob on server)
        )
        entries.append(config_entry)
    else:
        logging.warning("VM config file not found: %s", config_file)

    return entries, snapshots_created, warnings

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
        args.append(e.as_arg())
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
    rc = sh_with_logging(args, env=env)
    logging.info("proxmox-backup-client exit code: %s", rc)
    return rc

def print_plan_for_group(repo_alias: str, ns: Optional[str], backup_id: str, entries: List[PxarEntry], section_name: str):
    ns_disp = ns if ns is not None else "(root)"
    logging.info("=== PLAN for section: %s | repo-alias: %s | ns: %s | bid: %s ===", section_name, repo_alias, ns_disp, backup_id)
    for e in entries:
        flag = " (REQUIRES SNAPSHOT BUT UNAVAILABLE)" if e.snapshot_required else ""
        warn = " [FALLBACK TO LIVE]" if e.warned else ""
        note = f"  # {e.note}" if e.note else ""
        logging.info("  - %s.%s:%s%s%s%s", e.label, e.archive_type, e.src_path, flag, warn, note)
        for inc in (e.include_devs or []):
            logging.info("    --include-dev %s", inc)

# =========================
# Main
# =========================

def main():
    ap = argparse.ArgumentParser(description="pbs-runner: Plan & run proxmox-backup-client backups from YAML config")
    ap.add_argument("-c", "--config", required=True, help="Path to YAML config")
    ap.add_argument("--type", default=None, help="Backup type to run (vm|ct|host). Default: all types defined in config")
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

    # Acquire single-instance lock to prevent concurrent runs
    # Lock is automatically released when process exits
    acquire_single_instance_lock()

    # Clean up orphaned snapshots from previous failed runs
    hostname = socket.gethostname()
    cleanup_orphaned_snapshots(hostname, max_age_hours=24, dry_run=args.dry_run)

    notifications = cfg.get("notifications", {}) or {}
    hc_url = (notifications.get("healthcheck_url") or "").strip()
    dc_url = (notifications.get("discord_webhook") or "").strip()
    dc_evts = notifications.get("discord_notify_on") or []
    dc_pref = notifications.get("discord_prefix") or ""

    repositories_cfg = cfg.get("repositories", {}) or {}
    if not repositories_cfg:
        logging.warning("No 'repositories' configured; folders referencing repos will fail.")

    # Determine which types to process
    if args.type:
        types_to_process = [args.type]
    else:
        # Auto-detect all valid types from config
        valid_types = ['ct', 'host', 'vm']  # alphabetical order
        types_to_process = []
        for type_name in valid_types:
            section = cfg.get(type_name, {}) or {}
            if not section:
                continue

            # Check if type has content to process
            if type_name == 'vm':
                auto_discover_cfg = section.get("auto_discover", {}) or {}
                manual_vms = section.get("manual_vms", []) or []
                if auto_discover_cfg or manual_vms:
                    types_to_process.append(type_name)
            else:
                # host or ct: check for folders
                folders = section.get("folders", []) or []
                if folders:
                    types_to_process.append(type_name)

        if not types_to_process:
            logging.info("No valid types with content found in config. Nothing to do.")
            sys.exit(0)

        logging.info("Auto-detected types to process: %s", ", ".join(types_to_process))

    pbc_binary = args.pbc_binary or global_defaults.get("pbc_binary") or "proxmox-backup-client"

    # notifications: start (send once before processing all types)
    if not args.dry_run:
        notify_healthchecks(hc_url, "start")
        types_display = ", ".join(types_to_process)
        notify_discord(dc_url, f"Backup run started on {socket.gethostname()} (types: {types_display})",
                       prefix=dc_pref, notify_on=dc_evts, event="start")

    # Accumulate cleanups and exit codes across all types
    all_union_cleanups: List[Tuple[List[Path], Optional[Path], Optional[Tuple[str, str, bool]]]] = []  # (umounts, union_root, destroy_spec)
    all_vm_snapshots_to_cleanup: List[Tuple[str, str, bool]] = []  # (dataset, tag, recursive) for VMs
    overall_exit_code = 0
    any_type_had_fallback = False

    try:
        # Process each type in alphabetical order
        for current_type in types_to_process:
            logging.info("")
            logging.info("=" * 70)
            logging.info("Processing type: %s", current_type)
            logging.info("=" * 70)
            logging.info("")

            section = cfg.get(current_type, {}) or {}

            # Handle different section types
            is_vm_section = (current_type == "vm")

            if is_vm_section:
                # VM section uses auto_discover instead of folders
                section_defaults = {k: v for k, v in section.items() if k not in ["auto_discover", "manual_vms"]}
                auto_discover_cfg = section.get("auto_discover", {}) or {}
                manual_vms = section.get("manual_vms", []) or []

                if not auto_discover_cfg and not manual_vms:
                    logging.info("No auto_discover or manual_vms configured for type '%s'. Skipping.", current_type)
                    continue
            else:
                # Host/CT section uses folders
                section_defaults = {k: v for k, v in section.items() if k != "folders"}
                folders = section.get("folders", []) or []

                if not folders:
                    logging.info("No folders for type '%s'. Skipping.", current_type)
                    continue

            all_entries: List[PxarEntry] = []
            all_warnings: List[str] = []
            union_cleanups: List[Tuple[List[Path], Optional[Path], Optional[Tuple[str, str, bool]]]] = []  # (umounts, union_root, destroy_spec)
            vm_snapshots_to_cleanup: List[Tuple[str, str, bool]] = []  # (dataset, tag, recursive) for VMs

            if is_vm_section:
                # === VM Section: Auto-discover and backup VMs ===
                discovered_vms = []

                # Auto-discover VMs from libvirt
                if auto_discover_cfg and auto_discover_cfg.get('enabled', True):
                    discovered_vms = discover_vms_from_libvirt(auto_discover_cfg, section_defaults)
                    logging.info("Auto-discovered %d VM(s)", len(discovered_vms))

                # TODO: Add manual_vms support in future

                # Plan backup for each discovered VM
                for vm_entry in discovered_vms:
                    # Expand namespace template if specified
                    ns_template = vm_entry.get('namespace', '')
                    if ns_template:
                        expanded_ns = expand_template(
                            ns_template,
                            vm=vm_entry['name'],
                            host=socket.gethostname(),
                            section=current_type
                        )
                        vm_entry['namespace'] = sanitize_namespace(expanded_ns) if expanded_ns else None

                    entries, snaps, warns = plan_vm_backup(
                        vm_entry, section_defaults, global_defaults,
                        dry_run=args.dry_run, section_name=current_type
                    )
                    all_entries.extend(entries)
                    all_warnings.extend(warns)
                    vm_snapshots_to_cleanup.extend(snaps)

            else:
                # === Host/CT Section: Process folders ===
                for fcfg in folders:
                    if isinstance(fcfg, str):
                        fcfg = {"path": fcfg}
                    if "path" not in fcfg:
                        msg = f"Folder entry missing 'path': {fcfg}"
                        logging.warning(msg)
                        all_warnings.append(msg)
                        continue
                    entries, _created_snaps_unused, warns = plan_for_folder(
                        fcfg, section_defaults, global_defaults, dry_run=args.dry_run, section_name=current_type
                    )
                    for e in entries:
                        if e.cleanup_unmounts or e.cleanup_union_root or e.destroy_snapshot_spec:
                            union_cleanups.append((e.cleanup_unmounts, e.cleanup_union_root, e.destroy_snapshot_spec))
                    all_entries.extend(entries)
                    all_warnings.extend(warns)

            if not all_entries:
                logging.info("No eligible entries for type '%s' (none had 'repositories' or all skipped).", current_type)
                continue

            all_entries = consolidate_and_dedupe(all_entries)

            default_backup_id = section_defaults.get("backup_id") or socket.gethostname()
            groups: Dict[Tuple[str, Optional[str], str], List[PxarEntry]] = collections.defaultdict(list)
            for e in all_entries:
                eff_bid = e.backup_id_override or default_backup_id
                for repo_alias in e.repositories:
                    groups[(repo_alias, e.namespace, eff_bid)].append(e)

            type_exit_code = 0

            for (repo_alias, ns, bid), entries in groups.items():
                had_fallback = any(e.warned for e in entries)
                print_plan_for_group(repo_alias, ns, bid, entries, current_type)

                env = repo_env_from_cfg(repositories_cfg, repo_alias)
                if env is None:
                    logging.error("Skipping group (repo-alias=%s) due to missing/invalid repository config.", repo_alias)
                    type_exit_code = type_exit_code or 2
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
                    type_exit_code = rc
                    logging.error("Backup FAILED for repo-alias=%s ns=%s bid=%s (rc=%s).", repo_alias, ns or "(root)", bid, rc)
                    # Note: Per-group failure notification, but not healthchecks ping
                    notify_discord(
                        dc_url,
                        f"❌ Backup FAILED on {socket.gethostname()} repo={repo_alias} ns={ns or '(root)'} bid={bid} (type: {current_type}) rc={rc}",
                        prefix=dc_pref, notify_on=dc_evts, event="failure"
                    )
                else:
                    if had_fallback:
                        msg = f"⚠️ Backup SUCCEEDED with FALLBACK(S) on {socket.gethostname()} repo={repo_alias} ns={ns or '(root)'} bid={bid} (type: {current_type}). See logs."
                        logging.warning(msg)
                        notify_discord(dc_url, msg, prefix=dc_pref, notify_on=dc_evts, event="fallback")
                        any_type_had_fallback = True
                    else:
                        logging.info("Backup SUCCESS for repo=%s ns=%s bid=%s.", repo_alias, ns or "(root)", bid)

            # Accumulate cleanups from this type
            all_union_cleanups.extend(union_cleanups)
            all_vm_snapshots_to_cleanup.extend(vm_snapshots_to_cleanup)

            # Track exit code (continue processing other types even if this one failed)
            if type_exit_code != 0:
                overall_exit_code = type_exit_code
                logging.error("Type '%s' completed with errors (exit code: %s)", current_type, type_exit_code)
            else:
                logging.info("Type '%s' completed successfully", current_type)

        # After processing all types, send final notifications
        if not args.dry_run:
            if overall_exit_code == 0:
                notify_healthchecks(hc_url, "success")
                types_display = ", ".join(types_to_process)
                notify_discord(dc_url,
                               f"✅ Backup SUCCESS on {socket.gethostname()} (types: {types_display})",
                               prefix=dc_pref, notify_on=dc_evts, event="success")
            else:
                notify_healthchecks(hc_url, "failure")
                types_display = ", ".join(types_to_process)
                notify_discord(dc_url,
                               f"❌ Backup FAILED on {socket.gethostname()} (types: {types_display}) - see logs for details",
                               prefix=dc_pref, notify_on=dc_evts, event="failure")

        if args.dry_run:
            sys.exit(0)
        else:
            sys.exit(overall_exit_code)

    finally:
        # Cleanup: unmount union binds and remove union roots; destroy recursive snapshots
        for umounts, union_root, destroy_spec in all_union_cleanups:
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

        # Cleanup VM snapshots
        for ds, tag, rec in all_vm_snapshots_to_cleanup:
            if args.dry_run:
                logging.info("[DRY RUN] would destroy VM snapshot %s@%s%s", ds, tag, " (-r)" if rec else "")
            else:
                destroy_snapshot(ds, tag, recursive=rec)

        if not args.dry_run:
            types_display = ", ".join(types_to_process)
            notify_discord(dc_url, f"Backup run finished on {socket.gethostname()} (types: {types_display})",
                           prefix=dc_pref, notify_on=dc_evts, event="finish")

if __name__ == "__main__":
    main()
