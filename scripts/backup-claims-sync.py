#!/usr/bin/env python3
"""
backup-claims-sync.py -- recurring additive sync of the Claim Files bucket
into the backup Supabase project.

Meant to run on a schedule (a Render Cron Job every 6 hours -- see render.yaml).
Each run sweeps every claim folder in the live "Claim Files" bucket, compares it
against the backup project's bucket, and copies anything that is NEW or CHANGED.
Unchanged files are skipped after a cheap list() compare, so a sweep with no new
activity transfers nothing.

ADDITIVE ONLY: files removed from the source are intentionally left untouched in
the backup. The backup is a safety net, so an upstream deletion (accidental or
otherwise) can never erase the backed-up copy.

Use backup-claims-full-copy.py first to seed the backup; this script keeps it
current from then on. (Both share the same engine, so behaviour is identical --
this one is just the schedule-friendly entry point with sync-oriented logging.)

DRY RUN BY DEFAULT -- the scheduled job runs it with --execute.

USAGE
-----
  # Preview what a sweep would copy:
  python scripts/backup-claims-sync.py

  # Perform the sync (this is what the cron job runs):
  python scripts/backup-claims-sync.py --execute

Credentials come from environment variables (or the matching --flags):
  SOURCE_SUPABASE_URL / SOURCE_SUPABASE_SERVICE_ROLE_KEY / SOURCE_BUCKET
  BACKUP_SUPABASE_URL / BACKUP_SUPABASE_SERVICE_ROLE_KEY / BACKUP_BUCKET

EXIT CODES
----------
  0  Sweep completed (or dry run) with no failures
  1  Fatal error before work started (bad credentials, unreachable bucket)
  2  Partial failure -- some files failed (see backup-failures-<ts>.json)
"""

import argparse
import sys

from supabase_backup_mirror import add_common_args, run_mirror, setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="backup-claims-sync.py",
        description=(
            "Recurring additive sync of the live Claim Files bucket into the "
            "backup Supabase project. DRY RUN by default -- pass --execute to copy."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    add_common_args(parser)
    args = parser.parse_args()

    logger = setup_logging(args.log_file)
    sys.exit(run_mirror(args, mode_label="SCHEDULED SYNC SWEEP", logger=logger))


if __name__ == "__main__":
    main()
