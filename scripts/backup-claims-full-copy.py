#!/usr/bin/env python3
"""
backup-claims-full-copy.py -- one-time full copy of the Claim Files bucket
into the backup Supabase project.

Run this ONCE to seed the backup bucket with everything currently in the live
"Claim Files" bucket. It downloads every object from the source project and
uploads it (at the identical key) into the backup project's bucket, creating
that bucket if it does not exist yet.

It is safe to re-run: files already present and unchanged in the backup are
skipped, so a second run only fills in whatever the first run missed. After the
initial copy, the recurring job is handled by backup-claims-sync.py.

DRY RUN BY DEFAULT -- nothing is written until you pass --execute.

USAGE
-----
  # See what WOULD be copied (no writes):
  python scripts/backup-claims-full-copy.py

  # Actually copy everything:
  python scripts/backup-claims-full-copy.py --execute

  # Limit to specific claim folders (handy for a first test):
  python scripts/backup-claims-full-copy.py --folders WC-123,WC-456 --execute

Credentials come from environment variables (or the matching --flags):
  SOURCE_SUPABASE_URL / SOURCE_SUPABASE_SERVICE_ROLE_KEY / SOURCE_BUCKET
  BACKUP_SUPABASE_URL / BACKUP_SUPABASE_SERVICE_ROLE_KEY / BACKUP_BUCKET
See supabase_backup_mirror.py for the full contract.

EXIT CODES
----------
  0  Completed (or dry run) with no failures
  1  Fatal error before work started (bad credentials, unreachable bucket)
  2  Partial failure -- some files failed (see backup-failures-<ts>.json)
"""

import argparse
import sys

from supabase_backup_mirror import add_common_args, run_mirror, setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="backup-claims-full-copy.py",
        description=(
            "One-time full copy of the live Claim Files bucket into the backup "
            "Supabase project. DRY RUN by default -- pass --execute to copy."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    add_common_args(parser)
    args = parser.parse_args()

    logger = setup_logging(args.log_file)
    sys.exit(run_mirror(args, mode_label="INITIAL FULL COPY", logger=logger))


if __name__ == "__main__":
    main()
