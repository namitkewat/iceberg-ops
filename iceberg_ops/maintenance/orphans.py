"""
iceberg_ops.maintenance.orphans
===============================
Remove storage files not referenced by any current Iceberg metadata.

Public API
----------
``remove_orphan_files(table, *, older_than_hours=72, sweep=("data", "metadata"), dry_run=True)``
    Build the set of file names the table's metadata references, then
    walk the sweep directories on the local filesystem and report (or,
    with ``dry_run=False``, delete) any file whose basename is not
    referenced AND whose mtime is older than *older_than_hours*.

The referenced set covers every file reachable from table metadata:
manifest lists, manifests, and every live manifest entry of every
snapshot (data files, position-delete files, equality-delete files and
deletion-vector Puffin files), plus table statistics files, partition
statistics files, every metadata JSON in the metadata log and the
current metadata JSON.

Safety rails
------------
* ``dry_run`` defaults to ``True``; pass ``dry_run=False`` to delete.
* Tables with ``gc.enabled=false`` are refused, in dry-run too.
* Sweep directories must stay inside the table location. The walk does
  not follow directory symlinks or Windows junctions, and a file whose
  real path lies outside the table location is never a candidate.
* Names are matched case-insensitively after Unicode NFC
  normalisation, so a file whose metadata spelling differs only in
  case or normalisation form from its on-disk name is kept.
* Never run this against a location shared with another table: files
  the other table references would look like orphans here.

Local filesystem only
---------------------
This implementation supports local locations only: ``file://`` URIs
and plain paths, including Windows drive-letter paths. Tables on
``s3://`` / ``gs://`` / ``abfs://`` raise :class:`NotImplementedError`.
"""

from __future__ import annotations

import datetime
import logging
import os
import unicodedata
from dataclasses import dataclass, field
from urllib.parse import urlparse

from pyiceberg.table import Table
from pyiceberg.utils.properties import property_as_bool

logger = logging.getLogger(__name__)


# Suffixes Iceberg actually writes. Files in the sweep tree that don't
# match one of these are left alone - they're more likely user-deposited
# than true orphans, and silently deleting them would surprise operators.
_ICEBERG_SUFFIXES: tuple[str, ...] = (
    ".parquet",
    ".avro",
    ".metadata.json",
    ".puffin",
    ".stats",
)

# Below this age threshold, files written by an in-flight (not yet
# committed) writer may be mistaken for orphans.
_MIN_SAFE_AGE_HOURS = 24


@dataclass
class OrphanCleanupResult:
    """
    Result of an orphan file cleanup operation.

    Attributes
    ----------
    referenced_count : int
        Number of distinct file basenames referenced by table metadata
        (manifest lists, manifests, data and delete files, statistics
        files, and metadata JSON files), counted after case folding and
        Unicode NFC normalisation.
    orphan_count : int
        Number of orphan files found. In a non-dry run this counts every
        candidate, including any listed in ``failed_paths``.
    orphan_bytes : int
        Total bytes of orphan files found.
    orphan_paths : list[str]
        Local paths of orphan files found, in directory walk order.
    deleted : bool
        True if at least one file was actually deleted (never True in a
        dry run).
    failed_paths : list[str]
        Orphan paths whose deletion raised :class:`OSError`. Always empty
        in a dry run.
    """

    referenced_count: int
    orphan_count: int
    orphan_bytes: int
    orphan_paths: list[str] = field(default_factory=list)
    deleted: bool = False
    failed_paths: list[str] = field(default_factory=list)


def _is_drive_path(location: str) -> bool:
    """True if *location* is a local path with a drive (Windows only).

    ``urlparse`` reads the drive letter of a path like ``C:/wh/t`` as
    the scheme ``c``. PyIceberg treats such locations as local files,
    and so do the maintenance helpers. On POSIX ``os.path.splitdrive``
    never returns a drive, so this is always False there.
    """
    return bool(os.path.splitdrive(location)[0])


def _file_uri_to_local_path(uri: str) -> str:
    """Convert a ``file://`` URI or a local path to a local filesystem path."""
    if _is_drive_path(uri):
        return os.path.normpath(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        # On Windows, urlparse puts the drive path in netloc for URIs
        # like ``file://C:\Users\...``. Reassemble correctly.
        if parsed.netloc:
            path = parsed.netloc + parsed.path
        else:
            path = parsed.path
        # Strip leading slash before drive letter on Windows (e.g. /C:/)
        if os.name == "nt" and path.startswith("/") and len(path) > 2 and path[2] == ":":
            path = path[1:]
        return os.path.normpath(path)
    return uri


def _name_key(name: str) -> str:
    """Return the comparison key for a file name.

    Case-folded and NFC-normalised, because Windows and default macOS
    filesystems resolve names case-insensitively (macOS also across
    Unicode normalisation forms). Folding can only make more files
    match, so it is safe on case-sensitive filesystems too.
    """
    return unicodedata.normalize("NFC", name).casefold()


def _basename(path: str) -> str:
    """Return the comparison key of the trailing component of *path*,
    accepting either a local filesystem path or a ``file://`` URI."""
    return _name_key(os.path.basename(_file_uri_to_local_path(path)))


def _is_inside(root_real: str, path: str) -> bool:
    """True if the real path of *path* lies inside *root_real*."""
    try:
        return os.path.commonpath([root_real, os.path.realpath(path)]) == root_real
    except ValueError:  # different drives on Windows
        return False


def _has_iceberg_suffix(name: str) -> bool:
    return any(name.endswith(s) for s in _ICEBERG_SUFFIXES)


def _build_referenced_basenames(table: Table) -> set[str]:
    """Build the set of file basenames referenced by table metadata.

    Walks the manifests directly rather than going through
    ``table.inspect``, which materialises per-file metrics:

    1. For every snapshot: its manifest list, each of its manifests
       (deduplicated by path), and the file path of every live entry
       in those manifests. Entries cover data files, position-delete
       files, equality-delete files and deletion-vector Puffin files.
    2. Every table statistics file and partition statistics file.
    3. Every metadata JSON in the metadata log, plus the current
       ``metadata_location``.

    Parameters
    ----------
    table : Table
        A freshly refreshed table.

    Returns
    -------
    set of str
        Comparison keys (case-folded, NFC-normalised basenames) of
        every referenced file.
    """
    io = table.io
    metadata = table.metadata
    refs: set[str] = set()
    seen_manifests: set[str] = set()

    for snapshot in metadata.snapshots:
        refs.add(_basename(snapshot.manifest_list))
        for manifest in snapshot.manifests(io):
            if manifest.manifest_path in seen_manifests:
                continue
            seen_manifests.add(manifest.manifest_path)
            refs.add(_basename(manifest.manifest_path))
            for entry in manifest.fetch_manifest_entry(io):
                refs.add(_basename(entry.data_file.file_path))

    for stats in metadata.statistics:
        refs.add(_basename(stats.statistics_path))
    for partition_stats in metadata.partition_statistics:
        refs.add(_basename(partition_stats.statistics_path))
    for log_entry in metadata.metadata_log:
        refs.add(_basename(log_entry.metadata_file))

    if table.metadata_location:
        refs.add(_basename(table.metadata_location))

    return refs


def _resolve_sweep_dirs(table_root: str, sweep: tuple[str, ...]) -> list[str]:
    """Resolve *sweep* entries against *table_root*, refusing escapes.

    Parameters
    ----------
    table_root : str
        Local path of the table location.
    sweep : tuple of str
        Relative subdirectories to walk.

    Returns
    -------
    list of str
        Directories to walk (``table_root`` joined with each entry),
        one per sweep entry.

    Raises
    ------
    ValueError
        If an entry is absolute or resolves (following symlinks)
        outside *table_root*.
    """
    root_real = os.path.realpath(table_root)
    targets: list[str] = []
    for sweep_dir in sweep:
        if os.path.isabs(sweep_dir) or os.path.splitdrive(sweep_dir)[0]:
            raise ValueError(
                f"sweep entries must be relative to the table location, got {sweep_dir!r}"
            )
        target = os.path.normpath(os.path.join(table_root, sweep_dir))
        target_real = os.path.realpath(target)
        try:
            inside = os.path.commonpath([root_real, target_real]) == root_real
        except ValueError:  # different drives on Windows
            inside = False
        if not inside:
            raise ValueError(f"sweep entry {sweep_dir!r} resolves outside the table location")
        targets.append(target)
    return targets


def remove_orphan_files(
    table: Table,
    *,
    older_than_hours: int = 72,
    sweep: tuple[str, ...] = ("data", "metadata"),
    dry_run: bool = True,
) -> OrphanCleanupResult:
    """
    Find, and optionally delete, files no longer referenced by the table.

    The referenced set is every manifest list, every manifest, every
    live manifest entry (data files, position-delete files,
    equality-delete files and deletion-vector Puffin files) of every
    snapshot, every table and partition statistics file, every metadata
    JSON in the metadata log, and the current metadata JSON. Each
    ``sweep`` directory is then walked recursively; a file is treated
    as an orphan iff its basename is **not** in the referenced set, its
    extension matches a known Iceberg suffix (``.parquet`` / ``.avro``
    / ``.metadata.json`` / ``.puffin`` / ``.stats``), and its mtime
    predates the cutoff.

    Matching is by basename and is conservative: a file is kept if any
    referenced path shares its name, even in another directory. Names
    are compared case-insensitively after Unicode NFC normalisation,
    because Windows and default macOS filesystems resolve them that
    way. The walk does not follow directory symlinks or Windows
    junctions, and a file whose real path lies outside the table
    location is never a candidate.

    Never run this against a location shared with another table. Files
    the other table references would look like orphans here.

    Parameters
    ----------
    table : Table
        The Iceberg table to clean.
    older_than_hours : int, optional
        Files newer than this are skipped. Default ``72``. Values below
        ``24`` log a warning because files of in-flight writers may be
        deleted.
    sweep : tuple of str, optional
        Subdirectories of ``table.location()`` to walk. Default
        ``("data", "metadata")``. Entries must be relative and stay
        inside the table location.
    dry_run : bool, optional
        If True (the default), report orphans without deleting. Pass
        ``dry_run=False`` to delete them.

    Returns
    -------
    OrphanCleanupResult
        Statistics about referenced and orphan files. ``deleted`` is
        ``True`` only when at least one file was actually removed;
        ``failed_paths`` lists orphans whose deletion failed.

    Raises
    ------
    NotImplementedError
        If ``table.location()`` uses a non-local scheme (e.g.
        ``s3://``). Plain paths, including Windows drive-letter paths,
        are local.
    ValueError
        If ``older_than_hours`` is negative, a ``sweep`` entry is
        absolute or escapes the table location, or the table has
        ``gc.enabled=false`` (raised in dry-run too).
    """
    if older_than_hours < 0:
        raise ValueError(f"older_than_hours must be >= 0, got {older_than_hours!r}")

    location = table.location()
    parsed = urlparse(location)
    if not _is_drive_path(location) and parsed.scheme not in ("", "file"):
        raise NotImplementedError(
            f"Orphan cleanup for '{parsed.scheme}://' is not yet supported. "
            f"Only local filesystems (file://) are currently implemented."
        )

    table.refresh()

    if not property_as_bool(table.properties, "gc.enabled", True):
        raise ValueError(
            f"Refusing to remove orphan files from {'.'.join(table.name())}: "
            "gc.enabled=false (files may be shared with other tables)"
        )

    if older_than_hours < _MIN_SAFE_AGE_HOURS:
        logger.warning(
            "older_than_hours=%s is below %d; orphan cleanup may delete files of "
            "in-flight writers.",
            older_than_hours,
            _MIN_SAFE_AGE_HOURS,
        )

    table_root = _file_uri_to_local_path(location) if location else ""
    sweep_targets = _resolve_sweep_dirs(table_root, sweep) if table_root else []
    if not table_root or not os.path.isdir(table_root):
        logger.info(
            "No local table directory found for '%s'. Skipping orphan cleanup.",
            table.name(),
        )
        return OrphanCleanupResult(referenced_count=0, orphan_count=0, orphan_bytes=0)

    referenced = _build_referenced_basenames(table)
    cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=older_than_hours)

    orphan_paths: list[str] = []
    orphan_bytes = 0
    root_real = os.path.realpath(table_root)

    for target in sweep_targets:
        if not os.path.isdir(target):
            continue
        for root, dirs, files in os.walk(target):
            # Never descend through a directory symlink or a Windows
            # junction: os.walk skips only what islink() reports, and
            # islink() is False for junctions.
            dirs[:] = [
                d
                for d in dirs
                if not (os.path.islink(p := os.path.join(root, d)) or os.path.isjunction(p))
            ]
            for fname in files:
                if not _has_iceberg_suffix(fname):
                    continue
                if _name_key(fname) in referenced:
                    continue
                full = os.path.join(root, fname)
                try:
                    st = os.stat(full)
                except OSError as exc:
                    logger.debug("Skipping unreadable file %s: %s", full, exc)
                    continue
                mtime = datetime.datetime.fromtimestamp(st.st_mtime, tz=datetime.UTC)
                if mtime >= cutoff:
                    continue
                if not _is_inside(root_real, full):
                    logger.debug("Skipping %s: it resolves outside the table location", full)
                    continue
                orphan_paths.append(full)
                orphan_bytes += st.st_size

    if not orphan_paths:
        logger.info(
            "No orphan files found for '%s' (%d referenced).",
            table.name(),
            len(referenced),
        )
        return OrphanCleanupResult(
            referenced_count=len(referenced),
            orphan_count=0,
            orphan_bytes=0,
        )

    if dry_run:
        logger.info(
            "Orphan dry-run for '%s': found %d orphan files (%d bytes).",
            table.name(),
            len(orphan_paths),
            orphan_bytes,
        )
        return OrphanCleanupResult(
            referenced_count=len(referenced),
            orphan_count=len(orphan_paths),
            orphan_bytes=orphan_bytes,
            orphan_paths=orphan_paths,
            deleted=False,
        )

    deleted_paths: list[str] = []
    failed_paths: list[str] = []
    for path in orphan_paths:
        try:
            os.remove(path)
        except OSError as exc:
            logger.warning("Failed to delete orphan file %s: %s", path, exc)
            failed_paths.append(path)
        else:
            logger.debug("Deleted orphan file: %s", path)
            deleted_paths.append(path)

    logger.info(
        "Removed %d of %d orphan files from '%s' (%d failed).",
        len(deleted_paths),
        len(orphan_paths),
        table.name(),
        len(failed_paths),
    )

    return OrphanCleanupResult(
        referenced_count=len(referenced),
        orphan_count=len(orphan_paths),
        orphan_bytes=orphan_bytes,
        orphan_paths=orphan_paths,
        deleted=bool(deleted_paths),
        failed_paths=failed_paths,
    )
