#!/usr/bin/env python3
"""
supabase_backup_mirror.py -- cross-project Supabase Storage backup engine.

Shared engine behind the two runnable backup scripts:
  - backup-claims-full-copy.py   one-time full copy (seed the backup bucket)
  - backup-claims-sync.py        recurring additive sync (runs on a schedule)

WHAT IT DOES
------------
Mirrors every object in a SOURCE bucket (the live "Claim Files" bucket in the
main Supabase project) into a BACKUP bucket in a DIFFERENT Supabase project.

Supabase's server-side /object/copy endpoint only works WITHIN a single project,
so a cross-project copy must DOWNLOAD each file from the source and UPLOAD it to
the backup. That is exactly what this engine does, streaming through a temp file
so memory stays flat regardless of file size.

POLICY -- ADDITIVE ONLY (a safe backup, never a destructive mirror)
-------------------------------------------------------------------
  * A file is copied when it is MISSING in the backup, or when it has CHANGED
    (different size, or different content hash when both sides expose a plain
    MD5 ETag).
  * Files that are UNCHANGED are skipped (cheap list() compare, no transfer).
  * Files DELETED from the source are NEVER deleted from the backup. The backup
    is a safety net -- an accidental or malicious deletion upstream can never
    wipe the backed-up copy.

The source project is only ever READ from (list + download). All writes go to
the separate backup project.

CONFIGURATION (env vars; CLI flags override)
--------------------------------------------
  SOURCE_SUPABASE_URL                 e.g. https://upbbqaqnegncoetxuhwk.supabase.co
  SOURCE_SUPABASE_SERVICE_ROLE_KEY    service role key for the SOURCE project
  SOURCE_BUCKET                       default: "Claim Files"

  BACKUP_SUPABASE_URL                 e.g. https://<backup-ref>.supabase.co
  BACKUP_SUPABASE_SERVICE_ROLE_KEY    service role key for the BACKUP project
  BACKUP_BUCKET                       default: "Claim Files"

This module is not meant to be run directly -- use the two entry scripts.
"""

import json
import logging
import os
import re
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

import requests

# ─── Constants ──────────────────────────────────────────────────────────────

LIST_PAGE_SIZE    = 100               # Supabase Storage default max per page
REQUEST_TIMEOUT   = 30                # seconds for small JSON calls (list/bucket)
DOWNLOAD_TIMEOUT  = 600               # seconds for a single file download
UPLOAD_TIMEOUT    = 600               # seconds for a single file upload
DOWNLOAD_CHUNK    = 256 * 1024        # 256 KB streaming chunk
SPOOL_MAX_BYTES   = 8 * 1024 ** 2     # keep <=8 MB in RAM, spill larger to disk
RETRY_ATTEMPTS    = 4
RETRY_BACKOFF     = 2.0               # base seconds; doubles each retry
DEFAULT_CONCURRENCY = 6               # parallel download+upload pipelines

DEFAULT_SOURCE_BUCKET = "Claim Files"
DEFAULT_BACKUP_BUCKET = "Claim Files"

MD5_ETAG_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)


# ─── Errors ─────────────────────────────────────────────────────────────────

class StorageError(Exception):
    """Non-retryable storage API error (surfaced to the caller)."""


class _Retryable(Exception):
    """Internal: transient API error worth retrying."""


# ─── Logging ────────────────────────────────────────────────────────────────

def setup_logging(log_file: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger("supabase-backup-mirror")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.setLevel(logging.DEBUG)
    logger.addHandler(sh)

    if log_file:
        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)

    return logger


# ─── Helpers ────────────────────────────────────────────────────────────────

def _fmt_size(size_bytes) -> str:
    size = float(size_bytes or 0)
    if not size:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _clean_etag(etag: Optional[str]) -> Optional[str]:
    if not etag:
        return None
    e = etag.strip().strip('"').strip()
    return e or None


def _is_md5_etag(etag: Optional[str]) -> bool:
    return bool(etag and MD5_ETAG_RE.match(etag))


def _is_changed(src: dict, dst: dict) -> bool:
    """
    Decide whether a source file differs from the backup copy.

    Primary signal is byte size. Content hash is used as a secondary signal ONLY
    when both sides expose a plain MD5 ETag -- multipart/resumable uploads use
    composite ETags that never match across projects even for identical bytes,
    so comparing those would cause endless needless re-copies.
    """
    if int(src.get("size") or 0) != int(dst.get("size") or 0):
        return True
    se, de = src.get("etag"), dst.get("etag")
    if _is_md5_etag(se) and _is_md5_etag(de) and se != de:
        return True
    return False


def _new_stats() -> dict:
    return {
        "found":     0,   # source files discovered
        "new":       0,   # copied because missing in backup
        "updated":   0,   # copied because changed
        "unchanged": 0,   # skipped (already identical)
        "oversized": 0,   # skipped (exceeds --max-file-size-mb)
        "planned":   0,   # dry-run: would copy
        "failed":    0,
        "failed_paths": [],
    }


def _accumulate(total: dict, part: dict) -> None:
    for k in ("found", "new", "updated", "unchanged", "oversized", "planned", "failed"):
        total[k] = total.get(k, 0) + part.get(k, 0)
    total.setdefault("failed_paths", []).extend(part.get("failed_paths", []))


# ─── Storage client (one per project) ───────────────────────────────────────

class StorageClient:
    """Read/write wrapper around one project's Supabase Storage REST API."""

    def __init__(self, url: str, key: str, bucket: str, logger: logging.Logger, label: str):
        self.base_url = url.rstrip("/")
        self.bucket   = bucket
        self.label    = label            # "source" / "backup" for log lines
        self.log      = logger
        self.session  = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {key}",
            "apikey":        key,
        })

    # -- low level ----------------------------------------------------------

    def _backoff(self, attempt: int, ctx: str, exc: Exception) -> None:
        wait = RETRY_BACKOFF * (2 ** (attempt - 1))
        self.log.warning(
            f"  [{self.label}] {ctx} attempt {attempt}/{RETRY_ATTEMPTS} failed: "
            f"{exc}. Retrying in {wait:.0f}s..."
        )
        time.sleep(wait)

    def _json_request(self, method: str, url: str, ctx: str, **kwargs) -> requests.Response:
        """Send a small JSON request with retry on 5xx/429/network errors."""
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                resp = self.session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            except requests.RequestException as exc:
                if attempt == RETRY_ATTEMPTS:
                    raise StorageError(f"{ctx}: {exc}") from exc
                self._backoff(attempt, ctx, exc)
                continue

            if resp.ok:
                return resp
            retryable = resp.status_code in (408, 429) or resp.status_code >= 500
            detail = f"{resp.status_code} {resp.reason}: {resp.text[:300]}"
            if not retryable or attempt == RETRY_ATTEMPTS:
                raise StorageError(f"{ctx}: {detail}")
            self._backoff(attempt, ctx, _Retryable(detail))
        raise StorageError(f"{ctx}: exhausted retries")

    # -- bucket -------------------------------------------------------------

    def get_bucket(self) -> Optional[dict]:
        """Return the bucket's config dict, or None if it does not exist."""
        enc = quote(self.bucket, safe="")
        try:
            resp = self.session.get(
                f"{self.base_url}/storage/v1/bucket/{enc}", timeout=REQUEST_TIMEOUT
            )
        except requests.RequestException:
            return None
        return resp.json() if resp.ok else None

    def ensure_bucket(self, dry_run: bool, file_size_limit=None) -> str:
        """Create the bucket (private) if it does not already exist. Idempotent."""
        if self.get_bucket() is not None:
            return "exists"
        if dry_run:
            return "would-create"

        payload = {
            "id":                self.bucket,
            "name":              self.bucket,
            "public":            False,
            "file_size_limit":   file_size_limit,   # match the source bucket's cap
            "allowed_mime_types": None,
        }
        self._json_request(
            "POST", f"{self.base_url}/storage/v1/bucket",
            ctx=f"create bucket '{self.bucket}'", json=payload,
        )
        return "created"

    def set_bucket_limit(self, file_size_limit) -> None:
        """Set the bucket's file_size_limit (PUT). Raises StorageError on failure."""
        enc = quote(self.bucket, safe="")
        payload = {
            "id":                 self.bucket,
            "public":             False,
            "file_size_limit":    file_size_limit,
            "allowed_mime_types": None,
        }
        self._json_request(
            "PUT", f"{self.base_url}/storage/v1/bucket/{enc}",
            ctx=f"update bucket '{self.bucket}' size cap", json=payload,
        )

    # -- list ---------------------------------------------------------------

    def _list_page(self, prefix: str, offset: int) -> list:
        enc = quote(self.bucket, safe="")
        url = f"{self.base_url}/storage/v1/object/list/{enc}"
        payload = {
            "prefix": prefix,
            "limit":  LIST_PAGE_SIZE,
            "offset": offset,
            "sortBy": {"column": "name", "order": "asc"},
        }
        resp = self._json_request(
            "POST", url, ctx=f"list(prefix='{prefix}', offset={offset})", json=payload,
        )
        return resp.json()

    def list_all(self, prefix: str) -> list:
        """Paginate every item one directory level under `prefix`."""
        items, offset = [], 0
        while True:
            page = self._list_page(prefix, offset)
            if not page:
                break
            items.extend(page)
            if len(page) < LIST_PAGE_SIZE:
                break
            offset += LIST_PAGE_SIZE
        return items

    def walk_folder(self, prefix: str) -> list:
        """
        Recursively walk `prefix` and return a flat list of file entries:
            {"path": "<full key>", "size": int, "etag": str|None, "mimetype": str|None}
        Sub-directories (id is None) are descended into; files are collected.
        """
        results = []
        queue = [prefix.rstrip("/")]
        while queue:
            current = queue.pop(0)
            for item in self.list_all(current):
                name = item.get("name", "")
                full = f"{current}/{name}" if current else name
                if item.get("id") is None:
                    queue.append(full)
                else:
                    meta = item.get("metadata") or {}
                    results.append({
                        "path":     full,
                        "size":     meta.get("size") or 0,
                        "etag":     _clean_etag(meta.get("eTag")),
                        "mimetype": meta.get("mimetype"),
                    })
        return results

    # -- transfer -----------------------------------------------------------

    def download_to(self, key: str, fileobj) -> Optional[str]:
        """Stream object `key` into `fileobj`. Returns the response Content-Type."""
        enc = quote(self.bucket, safe="")
        ekey = quote(key, safe="/")
        url = f"{self.base_url}/storage/v1/object/authenticated/{enc}/{ekey}"
        ctx = f"download {key}"
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                fileobj.seek(0)
                fileobj.truncate()
                with self.session.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True) as resp:
                    if not resp.ok:
                        retryable = resp.status_code in (408, 429) or resp.status_code >= 500
                        detail = f"{resp.status_code} {resp.reason}: {resp.text[:200]}"
                        if not retryable:
                            raise StorageError(f"{ctx}: {detail}")
                        raise _Retryable(detail)
                    ctype = resp.headers.get("Content-Type")
                    for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK):
                        if chunk:
                            fileobj.write(chunk)
                    return ctype
            except (_Retryable, requests.RequestException) as exc:
                if attempt == RETRY_ATTEMPTS:
                    raise StorageError(f"{ctx}: {exc}") from exc
                self._backoff(attempt, ctx, exc)
        return None

    def upload(self, key: str, fileobj, content_type: Optional[str] = None) -> None:
        """Upload `fileobj` to object `key`, overwriting if present (x-upsert)."""
        enc = quote(self.bucket, safe="")
        ekey = quote(key, safe="/")
        url = f"{self.base_url}/storage/v1/object/{enc}/{ekey}"
        headers = {
            "Content-Type": content_type or "application/octet-stream",
            "x-upsert":     "true",
        }
        ctx = f"upload {key}"
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                fileobj.seek(0)
                resp = self.session.post(
                    url, data=fileobj, headers=headers, timeout=UPLOAD_TIMEOUT
                )
                if resp.ok:
                    return
                retryable = resp.status_code in (408, 429) or resp.status_code >= 500
                detail = f"{resp.status_code} {resp.reason}: {resp.text[:200]}"
                if not retryable:
                    raise StorageError(f"{ctx}: {detail}")
                raise _Retryable(detail)
            except (_Retryable, requests.RequestException) as exc:
                if attempt == RETRY_ATTEMPTS:
                    raise StorageError(f"{ctx}: {exc}") from exc
                self._backoff(attempt, ctx, exc)


# ─── Mirror logic ───────────────────────────────────────────────────────────

def _process_files(
    source:      StorageClient,
    backup:      StorageClient,
    src_files:   list,
    dst_index:   dict,
    label:       str,
    dry_run:     bool,
    concurrency: int,
    max_bytes:   int,
    logger:      logging.Logger,
) -> dict:
    """Compare a set of source files to the backup index and copy what is new/changed."""
    stats = _new_stats()
    stats["found"] = len(src_files)

    tasks = []   # (file_dict, reason)
    for f in src_files:
        if max_bytes and int(f.get("size") or 0) > max_bytes:
            logger.warning(
                f"  SKIP oversized ({_fmt_size(f['size'])} > {_fmt_size(max_bytes)}): {f['path']}"
            )
            stats["oversized"] += 1
            continue
        dst = dst_index.get(f["path"])
        if dst is None:
            tasks.append((f, "new"))
        elif _is_changed(f, dst):
            tasks.append((f, "changed"))
        else:
            stats["unchanged"] += 1

    if not tasks:
        return stats

    if dry_run:
        for f, reason in tasks:
            logger.info(f"  [DRY] {reason.upper():7} {f['path']}  ({_fmt_size(f['size'])})")
        stats["planned"] = len(tasks)
        return stats

    def do_one(task):
        f, reason = task
        key = f["path"]
        tmp = tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_BYTES, mode="w+b")
        try:
            ctype = source.download_to(key, tmp)
            backup.upload(key, tmp, ctype or f.get("mimetype"))
            return True, key, reason, None
        except Exception as exc:   # noqa: BLE001 -- record and continue
            return False, key, reason, str(exc)
        finally:
            tmp.close()

    for start in range(0, len(tasks), concurrency):
        batch = tasks[start:start + concurrency]
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {pool.submit(do_one, t): t for t in batch}
            for fut in as_completed(futures):
                ok, key, reason, err = fut.result()
                if ok:
                    stats["new" if reason == "new" else "updated"] += 1
                    logger.info(f"  OK   {reason.upper():7} {key}")
                else:
                    stats["failed"] += 1
                    stats["failed_paths"].append({"key": key, "reason": reason, "error": err})
                    logger.error(f"  ERR  {reason.upper():7} {key}: {err}")

    return stats


def mirror_folder(
    source:      StorageClient,
    backup:      StorageClient,
    folder:      str,
    dry_run:     bool,
    concurrency: int,
    max_bytes:   int,
    logger:      logging.Logger,
) -> dict:
    """Mirror one top-level folder (a claim) and everything beneath it."""
    src_files = source.walk_folder(folder)
    if not src_files:
        logger.info(f"  (no files under '{folder}/')")
        return _new_stats()
    dst_index = {f["path"]: f for f in backup.walk_folder(folder)}
    return _process_files(
        source, backup, src_files, dst_index, folder, dry_run, concurrency, max_bytes, logger
    )


# ─── CLI plumbing (shared by both entry scripts) ────────────────────────────

def add_common_args(parser) -> None:
    src = parser.add_argument_group("source project (read-only)")
    src.add_argument("--source-url", default=os.environ.get("SOURCE_SUPABASE_URL"),
                     metavar="URL", help="Source project URL (default: $SOURCE_SUPABASE_URL)")
    src.add_argument("--source-key", default=os.environ.get("SOURCE_SUPABASE_SERVICE_ROLE_KEY"),
                     metavar="KEY", help="Source service role key (default: $SOURCE_SUPABASE_SERVICE_ROLE_KEY)")
    src.add_argument("--source-bucket",
                     default=os.environ.get("SOURCE_BUCKET", DEFAULT_SOURCE_BUCKET),
                     help=f"Source bucket (default: '{DEFAULT_SOURCE_BUCKET}' or $SOURCE_BUCKET)")

    dst = parser.add_argument_group("backup project (written to)")
    dst.add_argument("--backup-url", default=os.environ.get("BACKUP_SUPABASE_URL"),
                     metavar="URL", help="Backup project URL (default: $BACKUP_SUPABASE_URL)")
    dst.add_argument("--backup-key", default=os.environ.get("BACKUP_SUPABASE_SERVICE_ROLE_KEY"),
                     metavar="KEY", help="Backup service role key (default: $BACKUP_SUPABASE_SERVICE_ROLE_KEY)")
    dst.add_argument("--backup-bucket",
                     default=os.environ.get("BACKUP_BUCKET", DEFAULT_BACKUP_BUCKET),
                     help=f"Backup bucket (default: '{DEFAULT_BACKUP_BUCKET}' or $BACKUP_BUCKET)")

    run = parser.add_argument_group("execution")
    run.add_argument("--execute", action="store_true",
                     help="Perform the copy. Without this flag the script is a DRY RUN.")
    run.add_argument("--folders", metavar="LIST",
                     help="Comma-separated top-level folder names to limit to (default: whole bucket).")
    run.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, metavar="N",
                     help=f"Parallel transfer pipelines (default: {DEFAULT_CONCURRENCY}).")
    run.add_argument("--max-file-size-mb", type=float, default=0, metavar="MB",
                     help="Skip (and log) files larger than this many MB. Default 0 = no limit.")
    run.add_argument("--log-file", metavar="PATH",
                     help="Also append all log output to this file.")


def _validate(args, logger) -> None:
    missing = []
    if not args.source_url:  missing.append("source URL (--source-url / $SOURCE_SUPABASE_URL)")
    if not args.source_key:  missing.append("source key (--source-key / $SOURCE_SUPABASE_SERVICE_ROLE_KEY)")
    if not args.backup_url:  missing.append("backup URL (--backup-url / $BACKUP_SUPABASE_URL)")
    if not args.backup_key:  missing.append("backup key (--backup-key / $BACKUP_SUPABASE_SERVICE_ROLE_KEY)")
    if missing:
        for m in missing:
            logger.error(f"Missing required credential: {m}")
        sys.exit(1)
    if args.source_url.rstrip("/") == args.backup_url.rstrip("/") \
            and args.source_bucket == args.backup_bucket:
        logger.error("Source and backup point at the same bucket in the same project. Refusing to run.")
        sys.exit(1)


def run_mirror(args, mode_label: str, logger: logging.Logger) -> int:
    """Orchestrate a full mirror pass. Returns a process exit code."""
    _validate(args, logger)
    dry_run = not args.execute

    source = StorageClient(args.source_url, args.source_key, args.source_bucket, logger, "source")
    backup = StorageClient(args.backup_url, args.backup_key, args.backup_bucket, logger, "backup")
    max_bytes = int(args.max_file_size_mb * 1024 * 1024) if args.max_file_size_mb else 0

    # Match the backup bucket's size cap to the source bucket so it can hold
    # everything the source can.
    src_cfg = source.get_bucket()
    if src_cfg is None:
        logger.error(f"Source bucket '{args.source_bucket}' not found or unreachable.")
        return 1
    src_limit = src_cfg.get("file_size_limit")

    # Ensure the backup bucket exists before we try to write to it.
    try:
        state = backup.ensure_bucket(dry_run, file_size_limit=src_limit)
    except StorageError as exc:
        logger.warning(
            f"Creating backup bucket with the source's {src_limit}-byte size cap failed "
            f"({exc}); retrying with the backup project's default cap."
        )
        try:
            state = backup.ensure_bucket(dry_run, file_size_limit=None)
        except StorageError as exc2:
            logger.error(f"Could not create backup bucket '{args.backup_bucket}': {exc2}")
            return 1
    logger.info(
        f"Backup bucket '{args.backup_bucket}': {state}  "
        f"(source size cap: {_fmt_size(src_limit) if src_limit else 'project default'})"
    )

    # Best-effort: make the backup bucket's size cap match the source so it can
    # hold large files. This only sticks once the backup project's GLOBAL upload
    # limit (dashboard: Storage > Settings) is >= that cap; until then it warns.
    if not dry_run and src_limit:
        cur = (backup.get_bucket() or {}).get("file_size_limit")
        if cur != src_limit:
            try:
                backup.set_bucket_limit(src_limit)
                logger.info(f"  Raised backup bucket size cap to {_fmt_size(src_limit)}.")
            except StorageError as exc:
                logger.warning(
                    f"  Backup bucket cap is still {_fmt_size(cur) if cur else 'the project default'} "
                    f"(could not set {_fmt_size(src_limit)}: {exc}). Files larger than the backup "
                    f"project's global 'Upload file size limit' will be rejected -- raise it in the "
                    f"backup project dashboard (Storage > Settings) to back up large files."
                )

    # Discover top-level folders (claims) + any files sitting at the bucket root.
    try:
        root_items = source.list_all("")
    except StorageError as exc:
        logger.error(f"Could not list source bucket '{args.source_bucket}': {exc}")
        return 1

    root_folders = sorted(
        i["name"] for i in root_items if i.get("id") is None and i.get("name")
    )
    root_files = [
        {
            "path":     i.get("name", ""),
            "size":     (i.get("metadata") or {}).get("size") or 0,
            "etag":     _clean_etag((i.get("metadata") or {}).get("eTag")),
            "mimetype": (i.get("metadata") or {}).get("mimetype"),
        }
        for i in root_items if i.get("id") is not None and i.get("name")
    ]

    if args.folders:
        wanted = {f.strip() for f in args.folders.split(",") if f.strip()}
        root_folders = [f for f in root_folders if f in wanted]
        root_files = [] if "(root files)" not in wanted else root_files
        missing = wanted - set(root_folders) - ({"(root files)"} if root_files else set())
        for m in sorted(missing):
            logger.warning(f"  Requested folder '{m}' not found at bucket root -- skipping")

    job_count = len(root_folders) + (1 if root_files else 0)

    logger.info("=" * 72)
    logger.info(f"Claim Files backup -- {mode_label}")
    logger.info(f"  Mode:      {'DRY RUN (pass --execute to copy)' if dry_run else 'LIVE'}")
    logger.info(f"  Source:    {args.source_url}  bucket '{args.source_bucket}'")
    logger.info(f"  Backup:    {args.backup_url}  bucket '{args.backup_bucket}'")
    logger.info(f"  Policy:    additive only (never deletes from backup)")
    if max_bytes:
        logger.info(f"  Max size:  {_fmt_size(max_bytes)} (larger files skipped)")
    logger.info(f"  Jobs:      {job_count}  ({len(root_folders)} claim folder(s)"
                f"{', + root files' if root_files else ''})")
    logger.info(f"  Started:   {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 72)

    totals = _new_stats()
    job_errors = 0
    idx = 0

    for folder in root_folders:
        idx += 1
        logger.info(f"\n[{idx}/{job_count}] claim folder '{folder}/'")
        try:
            stats = mirror_folder(source, backup, folder, dry_run, args.concurrency, max_bytes, logger)
        except StorageError as exc:
            logger.error(f"  FATAL on folder '{folder}': {exc}")
            totals["failed_paths"].append({"key": f"{folder}/", "reason": "folder", "error": str(exc)})
            job_errors += 1
            continue
        _accumulate(totals, stats)
        logger.info(
            f"  -> {stats['found']} found | {stats['new']} new | {stats['updated']} updated | "
            f"{stats['unchanged']} unchanged | {stats['oversized']} oversized | "
            f"{stats['planned']} planned | {stats['failed']} failed"
        )

    if root_files:
        idx += 1
        logger.info(f"\n[{idx}/{job_count}] bucket-root files")
        try:
            dst_root = {
                f["path"]: f
                for f in (
                    {
                        "path":     i.get("name", ""),
                        "size":     (i.get("metadata") or {}).get("size") or 0,
                        "etag":     _clean_etag((i.get("metadata") or {}).get("eTag")),
                        "mimetype": (i.get("metadata") or {}).get("mimetype"),
                    }
                    for i in backup.list_all("") if i.get("id") is not None and i.get("name")
                )
            }
            stats = _process_files(
                source, backup, root_files, dst_root, "(root files)",
                dry_run, args.concurrency, max_bytes, logger,
            )
        except StorageError as exc:
            logger.error(f"  FATAL on root files: {exc}")
            totals["failed_paths"].append({"key": "(root files)", "reason": "folder", "error": str(exc)})
            job_errors += 1
        else:
            _accumulate(totals, stats)
            logger.info(
                f"  -> {stats['found']} found | {stats['new']} new | {stats['updated']} updated | "
                f"{stats['unchanged']} unchanged | {stats['oversized']} oversized | "
                f"{stats['planned']} planned | {stats['failed']} failed"
            )

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("\n" + "=" * 72)
    logger.info(f"{'DRY RUN ' if dry_run else ''}COMPLETE -- {mode_label}")
    logger.info(f"  Jobs:            {job_count}")
    logger.info(f"  Files found:     {totals['found']}")
    if dry_run:
        logger.info(f"  Would copy:      {totals['planned']}  (re-run with --execute)")
    else:
        logger.info(f"  New copied:      {totals['new']}")
        logger.info(f"  Updated copied:  {totals['updated']}")
    logger.info(f"  Unchanged:       {totals['unchanged']}  (skipped -- already in backup)")
    if totals["oversized"]:
        logger.info(f"  Oversized:       {totals['oversized']}  (skipped -- over --max-file-size-mb)")
    logger.info(f"  Failed:          {totals['failed']}")
    logger.info(f"  Job errors:      {job_errors}")

    failures = totals["failed_paths"]
    if failures:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        fail_file = f"backup-failures-{ts}.json"
        try:
            with open(fail_file, "w", encoding="utf-8") as f:
                json.dump(failures, f, indent=2)
            logger.error(f"  {len(failures)} failure(s) recorded -> {fail_file}")
        except OSError as exc:
            logger.error(f"  Could not write failure manifest: {exc}")
    logger.info("=" * 72)

    if totals["failed"] > 0 or job_errors > 0:
        return 2
    return 0
