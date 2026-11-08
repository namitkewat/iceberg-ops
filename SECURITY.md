# Security policy

## Supported versions

Only the latest released minor line receives security fixes.

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |
| < 0.1   | No        |

## Reporting a vulnerability

Please report vulnerabilities **privately** through GitHub private
vulnerability reporting: open the repository's **Security** tab and choose
**Report a vulnerability**, or go directly to
<https://github.com/namitkewat/iceberg-ops/security/advisories/new>.
If that button is unavailable, email <namitkewat@gmail.com> with the
subject "iceberg-ops security".

Do not open a public issue, pull request or Discussion for a suspected
vulnerability, and do not include real credentials, tokens or catalog URIs
that contain secrets in your report.

You can expect an acknowledgement within 7 days and a status update within
30 days. This is a volunteer-maintained project, so these timelines are
best-effort. Once a fix is released, the advisory is published with credit
to the reporter unless you ask otherwise.

## Scope notes

In scope:

- credential handling in the Apache Polaris client (OAuth2 client
  credentials, bearer tokens, and anything that could write them to logs,
  history rows or error messages);
- catalog property handling;
- generated migration files (for example, catalog- or user-supplied names
  that end up as code in a file written by `iceberg-ops revision
  --autogenerate`);
- path handling in the maintenance helpers (orphan-file removal,
  compaction);
- anything that could cause unintended data deletion or credential
  exposure.

Destructive-by-design operations, such as `CatalogFactory.build(reset=True)`
or calling the Python API `remove_orphan_files(table, dry_run=False)`, are
documented behaviour. If you think their safety rails are insufficient,
please open a regular issue.

Running migration code is also by design and out of scope. `status`,
`current`, `plan`, `migrate`, `rollback` and `stamp` import and execute
every `NNNN_*.py` file in the migrations directory (or its `<namespace>/`
subdirectory) with your catalog credentials, and
`revision --target-schema-arrow` imports the module you name. Review those
files like any other code, and do not run these commands on an untrusted
branch with production credentials. See the "Trust model" section of the
schema migrations guide.
