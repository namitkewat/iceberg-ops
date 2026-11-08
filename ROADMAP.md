# Roadmap

iceberg-ops is migration-first: versioned, reviewable schema changes for
Apache Iceberg, with the migration history stored in your own catalog. The
data mover and the maintenance helpers are included but secondary.

This page describes direction, not commitments. There are no dates; items
move when they are ready or when users ask for them. To argue for an item,
or for something missing, open an issue or start a Discussion (Ideas).

## Now (0.1.x): feedback from early users

- Fix what early users and reviewers report, starting with anything that
  can lose data or produce a wrong migration.
- Catalog compatibility reports: collect how iceberg-ops behaves on real
  catalogs (REST catalogs such as Apache Polaris, Lakekeeper and Nessie;
  AWS Glue; S3 Tables; Hive) and document the results.
- Improve the documentation site and contributor guide based on feedback.
- Run the experimental Apache Polaris snapshot-expiry executor against a
  live Polaris server.

## Next (0.2): a deeper migration core

- Safer concurrent runs: claim a migration version before applying it, and
  keep an append-only history (one row per attempt) instead of one row per
  version.
- A structured runner API that returns results instead of printing them,
  and an `iceberg-ops check` command that exits non-zero in CI when
  migrations are pending or recorded as failed.
- Migration-file checksums (line-ending normalised), so an edited,
  already-applied migration is detected.
- CLI autogenerate for partition spec, sort order and table properties (the
  Python API already diffs them).
- Risk classes for generated operations, so a review can see which steps
  are metadata-only and which cannot be undone.
- Autogenerate diffs for nested types and for required / doc changes.
- Adopt an existing table: generate a baseline migration from a live table,
  then `stamp` it.
- One integration test against a REST catalog.
- A spec-correct Apache Polaris policy executor, after the live smoke run
  above.
- Data mover: a reader that bounds memory per source file, and a versioned
  checkpoint format.

## Later (0.3+): demand-driven

- Delegate orphan-file removal, snapshot expiry and compaction to
  PyIceberg's own maintenance APIs as they land (PyIceberg 0.13 and later).
  Until then the maintenance helpers receive bug fixes only.
- A DuckDB-backed verify option for the data mover.
- Staged (branch-based) data migration.
- MCP or HTTP interfaces, only if users ask for them.

## Not planned in 0.x

- A managed compaction service competing with catalog or platform
  maintenance.
- Merge-on-read or deletion-vector writes (iceberg-ops follows PyIceberg's
  write support).
- Sort-strategy or non-identity-partition compaction of our own.
- A native policy engine.
- A file-layout simulator.
- A native (Rust) accelerator.
- A bundled UI.
