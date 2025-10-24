# Proxmox Backup Server Runner (pbs-runner) 

`pbs-runner.py` is a batteries-included orchestrator for the **Proxmox Backup Client** (PBC).
It turns a declarative YAML config into consistent, auditable backup runs — with ZFS snapshots,
multi-repo env injection, a **unified read-only snapshot view** (union mode), robust logging,
and optional notifications.

---

## Highlights

- **Fresh ZFS snapshots** per run (use → back up → clean up).
- **Union (bind-mount) mode**: when `snapshot: true` and `split_subfolders: false`, pbs-runner
  takes a **recursive ZFS snapshot** and assembles a **single, read-only tree** (parent + child datasets)
  so you get **one pxar** that includes everything — adding `--include-dev` automatically.
- **Per-folder repositories** with env injection: `PBS_REPOSITORY`, `PBS_PASSWORD`, `PBS_FINGERPRINT`.
- **Backup grouping**: `bundle`, `per-app-id`, or `per-namespace`.
- **Dry-run** prints the exact plan: snapshots, union mounts, `--include-dev`, and full commands.
- **Logging**: console + rotating file.
- **Notifications**: Healthchecks.io & Discord (optional).

> **Excludes**: Use `.pxarexclude` files in your folders ([native PBC feature](https://pbs.proxmox.com/docs/backup-client.html#excluding-files-directories-from-a-backup)).

---

## Quick start

1) Install:
```bash
pip install pyyaml
```

2) Copy configuration:
```bash
cp config.example.yaml config.yaml
chmod 600 config.yaml
```

3) Dry-run (recommended):
```bash
./pbs-runner.py -c config.yaml --dry-run
```

4) Run:
```bash
./pbs-runner.py -c config.yaml
```

---

## Usage

```bash
./pbs-runner.py -c /path/to/config.yaml [--type host|vm|ct] [--dry-run] [--pbc-binary /path/to/proxmox-backup-client]
```

- Runs **one backup type per invocation** (default: `host`).
- `--pbc-binary` overrides the client path if needed.

---

## Configuration

All options, behaviors, and examples are documented in **[CONFIGURATION.md](./CONFIGURATION.md)**.  
A fully annotated **config.example.yaml** is also provided separately.

---

## License

GNU Public License v3
