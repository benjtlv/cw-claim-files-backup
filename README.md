# cw-claim-files-backup

Standalone deploy repo for the **Claim Files storage backup** Render Cron Job.

Mirrors the live `Claim Files` Supabase Storage bucket (source / prod project)
into a same-named `Claim Files` bucket in a **separate backup Supabase project**.

- **Additive only** — never deletes from the backup (a safety net against upstream deletion).
- **Read-only** on the source.
- **Idempotent** — unchanged files are skipped, so re-runs are safe.

The scripts here are copied verbatim from `claimwarrior/claim-warriors`
(`scripts/`); this repo exists only so Render has a small, dedicated source to
deploy from. Update the source of truth there and re-sync here when it changes.

## Render Cron Job

| Setting | Value |
|---|---|
| Name | `claim-files-backup-sync` |
| Runtime | Python |
| Build | `pip install -r scripts/requirements.txt` |
| Start | `python scripts/backup-claims-sync.py --execute` |
| Schedule | `0 */6 * * *` (every 6h, UTC) |

The first run performs a full seed (copies the whole bucket, files up to 5 GB);
later scheduled runs copy only new/changed files.

## Environment variables (set in Render — never commit)

| Var | Description |
|---|---|
| `SOURCE_SUPABASE_URL` | Source (prod) project URL |
| `SOURCE_SUPABASE_SERVICE_ROLE_KEY` | Source service-role key (secret) |
| `SOURCE_BUCKET` | `Claim Files` |
| `BACKUP_SUPABASE_URL` | Backup project URL |
| `BACKUP_SUPABASE_SERVICE_ROLE_KEY` | Backup service-role key (secret) |
| `BACKUP_BUCKET` | `Claim Files` |

## Local usage

```bash
pip install -r scripts/requirements.txt

# Dry run (default — previews what would copy, no writes):
python scripts/backup-claims-sync.py

# Real sync (what the cron runs):
python scripts/backup-claims-sync.py --execute
```

Success ends with a summary block: `COMPLETE -- SCHEDULED SYNC SWEEP`.
Any failures are listed and written to `backup-failures-<timestamp>.json`.
