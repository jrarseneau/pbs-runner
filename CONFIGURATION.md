# CONFIGURATION — pbs-runner

This document describes every configuration option supported by `pbs_runner.py`.  
See **config.example.yaml** for a fully annotated example you can copy.

---

## Structure

```yaml
repositories:    # Top-level repository definitions (credentials & target)
defaults:        # Global defaults, inheritable by sections & folders
notifications:   # Optional Healthchecks & Discord settings
host:            # Section: host backups (current focus)
  backup_id:
  backup_type:
  folders:       # List of folders to back up; each can override defaults/section settings
vm:              # Section reserved for future VM backups
ct:              # Section reserved for future container backups
```

---

## `repositories` (required for real runs)

Define each Proxmox Backup Server (PBS) repository **once**, then reference by alias in folders.

```yaml
repositories:
  <alias>:
    repository:  "user@pbs.example:datastore"  # PBS_REPOSITORY
    password:    "supersecret"                 # PBS_PASSWORD
    fingerprint: "aa:bb:cc:..."                # PBS_FINGERPRINT (optional but recommended)
```

- pbs-runner does **not** pass `--repository` on the CLI; it sets `PBS_*` env vars per command based on the alias(es) used by folders.
- If a folder references an unknown alias, that group is skipped with an error.

> **Security**: Your config contains secrets. Restrict permissions:
> ```bash
> chmod 600 config.yaml
> ```

---

## `defaults` (global)

Applies to all sections/folders unless overridden.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `snapshot` | bool | `false` | Create ZFS snapshots and back up from snapshot paths. |
| `split_subfolders` | bool | `false` | If `true`, each immediate subfolder becomes its own item. |
| `fallback_to_live` | bool | `true` | If snapshotting fails/unavailable, back up from live paths and warn. |
| `skip_hidden_subfolders` | bool | `true` | When splitting, ignore subfolders starting with `.`. |
| `backup_grouping` | enum | `"bundle"` | How to group pxars into PBS snapshots: `"bundle"`, `"per-app-id"`, `"per-namespace"`. |
| `backup_id_template` | str | `"{host}-{name}"` | Template used when `backup_grouping="per-app-id"`. Tokens: `{host}`, `{name}`, `{section}`, `{YYYY}`, `{MM}`, `{DD}`. |
| `namespace` | str | `""` | Base namespace for items (empty = root). |
| `namespace_template` | str | `"apps/{name}"` | Template used when `backup_grouping="per-namespace"`. Same tokens as above. |
| `extra_args` | list[str] | `[]` | Extra CLI args appended to `proxmox-backup-client backup`. |
| `pbc_binary` | str | `"proxmox-backup-client"` | Path to PBC binary. |
| `log.file` | str | `"pbs_backup.log"` | Rotating log file path. |
| `log.level` | str | `"INFO"` | Log level. |
| `log.rotate_max_bytes` | int | `10485760` | Rotate at ~10MB. |
| `log.rotate_backups` | int | `5` | Keep N rotated logs. |

### Union mode (bind-mounted unified view)

When `snapshot: true` **and** `split_subfolders: false`, default behavior is to create a **unified, read-only tree** by
bind-mounting the **parent snapshot** plus **each child dataset snapshot** under a temporary directory, and then back up that single
directory as **one pxar**, adding `--include-dev` for each child bind.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `union_mode` | enum | `"auto"` | `"auto"` enables union behavior; `"off"` disables union (legacy behavior). |
| `union_mount_root` | str | `"/tmp"` | Where the runner creates a unique temp dir (mktemp-like) for the union mount. |

> If `union_mode: off`, backing up the parent dataset snapshot will not include child dataset contents (ZFS behavior). Use `split_subfolders: true`
> or list each child dataset as its own folder entry.

---

## `notifications` (optional)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `healthcheck_url` | str | `""` | Healthchecks.io check URL. Pings `/start`, `/fail`, and success baseline. |
| `discord_webhook` | str | `""` | Discord Webhook URL. |
| `discord_notify_on` | list[str] | `["failure","fallback"]` | Any of: `"start"`, `"success"`, `"failure"`, `"finish"`, `"fallback"`. |
| `discord_prefix` | str | `"[pbs-runner]"` | Prefix text for Discord messages. |

---

## Sections: `host`, `vm`, `ct`

Only one section is executed per run via `--type` (default `host`).

Common section-level keys (override `defaults` for that section):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `backup_id` | str | hostname | PBS backup-id when not overridden by `per-app-id` mode. |
| `backup_type` | str | `"host"` | PBS backup type (`"host"`, reserved future: `"vm"`, `"ct"`). |
| Any default key | - | - | Any key under `defaults` can be set here to override it for the section. |

### `folders` list (per section)

Each entry can be either a string (interpreted as `path`) or a mapping with overrides.

**Required**  
- `path` (string): absolute path  
- `repositories` (list of strings): **aliases** referencing `repositories` at the top level

**Optional overrides** (same meaning as in `defaults`):  
`snapshot`, `split_subfolders`, `fallback_to_live`, `skip_hidden_subfolders`,  
`backup_grouping`, `backup_id_template`, `namespace`, `namespace_template`,  
`label`, `label_prefix`, `union_mode`, `union_mount_root`

**Notes**
- **Labels**: The left side of `label.pxar:/path` must match PBS rules. The runner sanitizes automatically, but custom labels should be alnum/`_`/`-` and start with an alnum.
- **Namespaces**: PBS does **not** auto-create; pre-create them or use `per-app-id` if you want per-app retention with least privilege.
- **Excludes**: Place `.pxarexclude` files inside your directories (PBC reads them automatically).

---

## Behavior summary

- If `snapshot: true`:
  - The runner creates **ZFS snapshots** (per dataset, or **recursive** in union mode), uses the **snapshot paths** as sources,
    and **cleans up** the snapshots after the run.
- If `split_subfolders: true`:
  - Folder expands to immediate subfolders. Combine with `backup_grouping: per-app-id` for per-app retention.
- If `split_subfolders: false` and `union_mode: auto`:
  - The runner builds a **temporary union** mount and backs up **one pxar**, adding `--include-dev` for child binds automatically.
- **Grouping into commands**:
  - Items are grouped by `(repository-alias, namespace, backup-id)`, producing one `proxmox-backup-client backup` per group.
- **Per-repo environment**:
  - Each command is executed with `PBS_REPOSITORY`, `PBS_PASSWORD`, and optional `PBS_FINGERPRINT` set from the chosen alias.

---

## Dry-run

`--dry-run` prints (without executing):
- Snapshots it would create (including `-r`)
- Union directories it would create
- All bind mounts and corresponding `--include-dev`
- The exact `proxmox-backup-client` commands with grouping
- The cleanup steps (unmounts, rmdir, snapshot destroys)

