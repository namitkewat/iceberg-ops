"""
iceberg_ops.data_migration.checkpoint
=====================================
Resumable-migration bookkeeping for the cross-table data migrator.

A :class:`CheckpointStore` persists per-batch progress to a JSON file
on local disk. The streaming :class:`~iceberg_ops.data_migration.DataMigrator`
writes one entry per committed batch; on restart, the migrator skips
batches whose index is at or below ``last_committed_batch`` and picks
up where the previous run died.

File format (version 1)
-----------------------
.. code-block:: json

   {
     "version": 1,
     "source_snapshot_id": 12345,
     "last_committed_batch": 42,
     "dest_snapshot_ids": [98765, 98766],
     "completed": false,
     "started_at": "2026-05-05T10:30:00Z",
     "job": {"batch_rows": 50000, "write_mode": "append", "...": "..."}
   }

Fields:

``version``
    File-format version. Currently ``1``. The store refuses to load
    a file with any other version - bump this and add migration
    logic when the format evolves.
``source_snapshot_id``
    Iceberg snapshot ID of the source table the migration reads
    (or ``null`` if the source had no snapshot). On resume,
    :class:`~iceberg_ops.data_migration.DataMigrator` reads the
    source at this snapshot, so the resumed batches line up with the
    ones already committed.
``last_committed_batch``
    Index of the last successfully committed batch, or ``null`` if
    nothing has been committed yet.
``dest_snapshot_ids``
    Every destination snapshot the committed batches created, in
    commit order (one batch can create several snapshots). Useful
    for post-mortems.
``completed``
    ``true`` once :meth:`CheckpointStore.mark_completed` has been
    called. The file remains on disk after completion so the user
    can audit the snapshot list; :meth:`CheckpointStore.reset`
    removes it. ``DataMigrator`` refuses to reuse a completed
    checkpoint.
``started_at``
    ISO 8601 UTC timestamp of the most recent
    :meth:`CheckpointStore.start` call. Updated on every fresh
    initialisation; *not* updated when the file is merely re-loaded
    by a resuming migrator.
``job`` (optional)
    The identity of the migration job that started the checkpoint:
    source and destination table UUIDs, row filter, ``batch_rows``,
    write mode, upsert keys and reconciler settings, plus a random
    job ID and the destination snapshot that was current before the
    job wrote anything. ``null`` (or absent) when the caller did not
    record one. ``DataMigrator`` refuses to resume a checkpoint
    whose job differs from the current settings, or that has no job.

Resume rules
------------
:class:`~iceberg_ops.data_migration.DataMigrator` resumes from an
existing checkpoint file only when all of these hold; otherwise it
raises :class:`CheckpointError` before writing anything:

- the checkpoint is not ``completed``;
- it carries a ``job`` that matches the current settings;
- the stored ``source_snapshot_id`` still exists in the source
  table and agrees with any explicit ``source_snapshot``;
- the destination's current history still holds the snapshots of
  the checkpointed batches (a rollback of the destination would
  otherwise make the resume skip rows that are no longer there).

:meth:`CheckpointStore.reset` forgets which batches are in the
destination: roll the destination back to the job's
``dest_snapshot_before`` (or delete those rows) before you reset and
re-run, or an append writes them again.

Atomicity
---------
Every write goes through ``<path>.tmp``, which is flushed and
``fsync``-ed before :func:`os.replace` moves it over the live file.
The replace is atomic on POSIX and on Windows when source and
destination are on the same filesystem (which is the only case we
support). On POSIX the parent directory is ``fsync``-ed as well so
the rename itself survives a power loss.

Concurrency
-----------
Single-writer only. Two ``DataMigrator`` runs pointed at the same
checkpoint will overwrite each other's progress with no
interlocking. Use one checkpoint file per migration job.
"""

from __future__ import annotations

import contextlib
import datetime
import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Bump when the JSON layout changes. CheckpointStore refuses any other
# version - see _load_from_disk().
_FILE_VERSION = 1


class CheckpointError(RuntimeError):
    """Raised when a checkpoint file cannot be used.

    Covers a corrupt, unreadable or unsupported-version file, and the
    resume refusals of :class:`~iceberg_ops.data_migration.DataMigrator`
    (completed checkpoint, job mismatch, missing source snapshot,
    checkpointed batches no longer in the destination).

    The message identifies the offending path and suggests a remedy
    (typically: delete the file or call
    :meth:`CheckpointStore.reset`). When batches were already
    committed, a resume refusal also names the destination snapshot
    to roll back to first, since a re-run after ``reset()`` writes
    them again. Subclasses :class:`RuntimeError` so callers that
    catch runtime failures also catch it.
    """


class CheckpointStore:
    """JSON-file-backed checkpoint for resumable data migrations.

    The store is intentionally lightweight: the constructor does not
    touch disk. Call :meth:`start` to initialise a fresh checkpoint,
    or call any accessor (:meth:`last_committed` / :meth:`is_committed`
    / :attr:`dest_snapshot_ids`) on an existing file to lazy-load it.

    Parameters
    ----------
    path : str or pathlib.Path
        Filesystem path where the checkpoint JSON lives. Parent
        directory is created on first write if it does not exist.

    Examples
    --------
    Fresh migration::

        store = CheckpointStore("/tmp/migrate.json")
        store.start(source_snapshot_id=12345)
        for batch_idx, batch in enumerate(reader):
            if store.is_committed(batch_idx):
                continue
            dest_snap = write_one_batch(batch)
            store.mark_committed(batch_idx, dest_snap)
        store.mark_completed()

    Resuming after a crash::

        store = CheckpointStore("/tmp/migrate.json")
        # No start() - file already exists; lazy-load on first use.
        last = store.last_committed()  # e.g. 42
        for batch_idx, batch in enumerate(reader):
            if store.is_committed(batch_idx):
                continue                # skips 0..42
            ...
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, path: str | Path) -> None:
        self._path: Path = Path(path)
        # Sibling .tmp file used for the atomic-write trick. Putting
        # it next to the live file keeps both on the same filesystem
        # so os.replace stays atomic.
        self._tmp_path: Path = self._path.with_suffix(self._path.suffix + ".tmp")
        # Lazy: don't touch disk until any method is called.
        self._state: dict[str, Any] | None = None

    # ------------------------------------------------------------------
    # Disk I/O (private)
    # ------------------------------------------------------------------

    def _atomic_write(self, state: dict[str, Any]) -> None:
        """Persist *state* via an fsync-ed tmp file + ``os.replace``."""
        # Ensure parent directory exists; first write creates the file.
        self._path.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(state, indent=2, sort_keys=True)
        with open(self._tmp_path, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(self._tmp_path, self._path)
        if os.name == "posix":
            # Persist the rename itself. Windows cannot open a
            # directory for fsync, and NTFS journals the rename.
            dir_fd = os.open(self._path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

    def _load_from_disk(self) -> dict[str, Any]:
        """Read state from disk and validate it.

        Returns a fresh in-memory state if the file does not exist
        (without writing); the caller is responsible for persisting
        when there is something worth saving.
        """
        if not self._path.exists():
            return self._fresh_state()
        try:
            text = self._path.read_text(encoding="utf-8")
        except OSError as exc:
            raise CheckpointError(f"Cannot read checkpoint at {self._path!s}: {exc}") from exc
        try:
            state = json.loads(text)
        except json.JSONDecodeError as exc:
            raise CheckpointError(
                f"Checkpoint file {self._path!s} is not valid JSON "
                f"({exc.msg} at line {exc.lineno}). Delete it (or "
                "call .reset()) to start fresh."
            ) from exc
        if not isinstance(state, dict):
            raise CheckpointError(
                f"Checkpoint file {self._path!s} top-level value is "
                f"{type(state).__name__}, expected object."
            )
        version = state.get("version")
        if version != _FILE_VERSION:
            raise CheckpointError(
                f"Checkpoint file {self._path!s} has version "
                f"{version!r}; this build of iceberg-ops supports "
                f"only version {_FILE_VERSION}. Delete the file (or "
                "call .reset()) and re-run the migration."
            )
        for required in (
            "last_committed_batch",
            "dest_snapshot_ids",
            "completed",
            "started_at",
            "source_snapshot_id",
        ):
            if required not in state:
                raise CheckpointError(
                    f"Checkpoint file {self._path!s} is missing required key {required!r}."
                )
        # ``job`` is optional: files written without one load with None.
        state.setdefault("job", None)
        return state

    @staticmethod
    def _fresh_state(
        source_snapshot_id: int | None = None,
        job: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a brand-new state dict (not yet on disk)."""
        return {
            "version": _FILE_VERSION,
            "source_snapshot_id": source_snapshot_id,
            "last_committed_batch": None,
            "dest_snapshot_ids": [],
            "completed": False,
            "started_at": _now_utc_iso(),
            "job": dict(job) if job is not None else None,
        }

    def _ensure_loaded(self) -> dict[str, Any]:
        """Lazy-load on first access; reuse the cached dict thereafter."""
        if self._state is None:
            self._state = self._load_from_disk()
        return self._state

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(
        self,
        source_snapshot_id: int | None = None,
        *,
        job: dict[str, Any] | None = None,
    ) -> None:
        """Initialise a fresh checkpoint, overwriting any previous file.

        Use this for new migrations. To *resume* an existing
        migration, skip :meth:`start` entirely - the store will
        lazy-load the existing JSON on the first accessor call.

        Parameters
        ----------
        source_snapshot_id : int, optional
            Iceberg snapshot ID of the source table the migration
            reads. A resuming ``DataMigrator`` reads at this snapshot.
        job : dict, optional
            JSON-serialisable identity of the migration job (see the
            module docstring). A resuming ``DataMigrator`` compares it
            with its own settings and refuses on any difference.

        Notes
        -----
        :meth:`start` always overwrites - the caller is expected to
        have already decided "fresh run vs. resume". The data
        migrator makes that decision with :meth:`exists`: an
        existing file means resume, a missing one means a fresh run.
        """
        self._state = self._fresh_state(source_snapshot_id=source_snapshot_id, job=job)
        self._atomic_write(self._state)
        logger.debug(
            "CheckpointStore.start: %s (source_snapshot_id=%s)",
            self._path,
            source_snapshot_id,
        )

    def reset(self) -> None:
        """Delete the checkpoint file and clear in-memory state.

        Idempotent - safe to call when no file exists. After reset,
        the next mutator call lazy-creates a fresh state.
        """
        for p in (self._path, self._tmp_path):
            with contextlib.suppress(FileNotFoundError):
                p.unlink()
        self._state = None
        logger.debug("CheckpointStore.reset: %s removed.", self._path)

    def mark_completed(self) -> None:
        """Mark the migration as complete (success).

        The file remains on disk after completion so the user can
        audit ``dest_snapshot_ids``; a follow-up call to
        :meth:`reset` removes it.
        """
        state = self._ensure_loaded()
        state["completed"] = True
        self._atomic_write(state)

    # ------------------------------------------------------------------
    # Per-batch progress
    # ------------------------------------------------------------------

    def is_committed(self, batch_idx: int) -> bool:
        """Return ``True`` if ``batch_idx`` was already committed.

        Always ``False`` before any :meth:`mark_committed` call.
        """
        last = self.last_committed()
        return last is not None and batch_idx <= last

    def mark_committed(self, batch_idx: int, dest_snapshot_id: int | list[int]) -> None:
        """Record that ``batch_idx`` produced ``dest_snapshot_id`` and persist.

        Lazily creates the checkpoint file if it does not exist yet -
        callers are *not* required to call :meth:`start` first if
        they don't care about ``source_snapshot_id``.

        Parameters
        ----------
        batch_idx : int
            Zero-based batch index. Must be strictly greater than
            :meth:`last_committed`; the streaming loop in
            :class:`DataMigrator` commits in monotonically increasing
            order, and out-of-order calls indicate a bug.
        dest_snapshot_id : int or list of int
            Destination snapshot ID(s) the write of this batch
            created, in commit order. An upsert batch can create
            several snapshots, and a no-op upsert none (pass ``[]``).
            Every ID is appended to :attr:`dest_snapshot_ids`.

        Raises
        ------
        ValueError
            If ``batch_idx`` is not strictly greater than
            ``last_committed_batch``.
        """
        state = self._ensure_loaded()
        last = state["last_committed_batch"]
        if last is not None and batch_idx <= last:
            raise ValueError(
                f"batch_idx {batch_idx} is not strictly greater than "
                f"last_committed_batch {last}; checkpoints must "
                "advance monotonically."
            )
        ids = dest_snapshot_id if isinstance(dest_snapshot_id, list) else [dest_snapshot_id]
        state["last_committed_batch"] = int(batch_idx)
        state["dest_snapshot_ids"].extend(int(i) for i in ids)
        self._atomic_write(state)

    def last_committed(self) -> int | None:
        """Return the last committed batch index, or ``None`` if none."""
        state = self._ensure_loaded()
        last = state["last_committed_batch"]
        return None if last is None else int(last)

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------

    @property
    def path(self) -> Path:
        """Filesystem path the checkpoint persists to."""
        return self._path

    def exists(self) -> bool:
        """``True`` if the checkpoint file is currently on disk."""
        return self._path.exists()

    @property
    def source_snapshot_id(self) -> int | None:
        """Source snapshot recorded by the most recent :meth:`start`."""
        sid = self._ensure_loaded().get("source_snapshot_id")
        return None if sid is None else int(sid)

    @property
    def job(self) -> dict[str, Any] | None:
        """Job identity recorded by the most recent :meth:`start`.

        Returns a copy, so mutating it does not change the store.
        ``None`` when the checkpoint was started without a job.
        """
        job = self._ensure_loaded().get("job")
        return None if job is None else dict(job)

    @property
    def started_at(self) -> str | None:
        """ISO 8601 UTC timestamp of the most recent :meth:`start` call."""
        ts = self._ensure_loaded().get("started_at")
        return None if ts is None else str(ts)

    @property
    def completed(self) -> bool:
        """``True`` once :meth:`mark_completed` has flipped the flag."""
        return bool(self._ensure_loaded().get("completed", False))

    @property
    def dest_snapshot_ids(self) -> list[int]:
        """Snapshot IDs every committed batch produced, in commit order."""
        return [int(s) for s in self._ensure_loaded().get("dest_snapshot_ids", [])]


def _now_utc_iso() -> str:
    """ISO 8601 UTC timestamp, second precision, with ``Z`` suffix.

    Matches the format used elsewhere in iceberg-ops
    (:class:`~iceberg_ops.history.MigrationHistory`, snapshot
    listings) so timestamps line up in audit logs.
    """
    return datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
