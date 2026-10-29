# ExposureIQ: operator and user-test guide

ExposureIQ is a **single-process, localhost company pilot** for organising film, TV and game
catalogues, checking known torrent hashes, reviewing observations and recording investigations.
It is not a production anti-piracy enforcement service. Historical research and fresh tracker
observations are different evidence sources.

## Start without resetting data

From the repository root, with the prepared datasets available:

```bash
uv sync
uv run python -m portal.server
# http://localhost:8800
```

Create a company and administrator account; there are no default company credentials. Use the same
account for subsequent sessions. Do not rerun acquisition, ETL or the classroom reset script just to
open the site. Missing datasets are reported as errors rather than replaced with demonstration data.

On-demand checks require reachable public UDP trackers. Shared streaming additionally requires
Docker, Java, Kafka/PostgreSQL and populated historical `public.torrents`/`public.swarm_latest`
tables. Keep the server bound to `127.0.0.1`; do not run multiple portal workers.

## Roles and boundaries

| Role | Intended actions |
|---|---|
| Administrator | Manage company access and API keys; operate the shared live collector; use analyst workflows |
| Analyst | Manage the catalogue, checks, rules, investigations and reports; run live read-only queries |
| Viewer | Read and export permitted company records; no workflow mutations |

Company records are scoped by organisation. Research metadata and the live collector are shared,
not per-company. Signing out neither erases company records nor stops the live collector. Personal
watchlists are browser-local and scoped by company/user. API keys are read-only; treat them as secrets.

## Demonstration: import to report

1. Import a UTF-8 CSV using unique headers `title,category,external_id`, or `title,category`.
   Supported categories are `movie`, `tv` and `game`. Maximum: 200 rows and 1 MB.
   Invalid imports fail atomically. For example:

   ```csv
   title,category,external_id
   Interstellar,movie,tt0816692
   The Sopranos,tv,
   Portal 2,game,
   ```

2. Review candidates and explicitly approve the correct title. Approval is an identity decision,
   not proof of rights ownership or infringement. Importing an unknown name does not discover hashes.
3. Configure an enabled title-specific threshold rule before running a portfolio check.
   A low threshold used in a demonstration is not a commercial risk recommendation.
4. Run **Check Seeders** and inspect the actual observation time, sampled hashes, responding trackers,
   failures and coverage. Portfolio checks are saved; ad-hoc checks are not automatically company history.
5. Review the saved check and any resulting alert. Unavailable/stale checks do not trigger rules.
   An unresolved alert for the same rule/title prevents duplicates.
6. Create an investigation, assign a company member, update its status and add an actual review note.
7. Export check JSON or observation CSV for measurements. Catalogue CSV contains identities and
   matching decisions, not measurements. Its `exported_at` is file-generation time.
8. Reopen saved records after signing out and in. Another member of the same company should see
   shared company records; a different company should start with empty private workflows.

Reported seeders use a maximum across trackers per record, not a sum of overlapping tracker counts.
Do not interpret them as unique people, downloads, verified infringement or revenue loss.

## Live pipeline and query editor

An administrator opens **Live pipeline & SQL**, selects 100, 500 or 2,000 historically busy known hashes
and starts collection. First observations can take 1-2 minutes; UDP tracker failures may delay them.
Check timestamps rather than assuming a retained snapshot is fresh.

| Surface | ExposureIQ identifier |
|---|---|
| Kafka topic | `tracker.scrapes.cwatch` (three partitions when created by the product) |
| PostgreSQL schema | `cwatch` |
| Parquet / JSONL | `dataset/stream/cwatch/`, `dataset/stream/jsonl_cwatch/` |
| Checkpoints | `dataset/stream/_checkpoints_cwatch/` |
| Logs | `logs/cwatch-stream.log`, `logs/cwatch-scraper.log` |

The editor runs **PostgreSQL**, not Spark SQL. It exposes `live_latest`, `live_minutes` and
`live_batches` through a dedicated non-superuser read-only connection, with a five-second statement
timeout, one-second lock timeout and 200-row result cap. Example:

```sql
SELECT title, category, seeders, scraped_at
FROM live_latest
WHERE scraped_at >= now() - interval '10 minutes'
ORDER BY scraped_at DESC
LIMIT 50;
```

The committed total is a batch-ledger total, not the current snapshot row count or audience size.
Full events are retained separately in Parquet. Stop collection preserves committed data and
checkpoints and stops owned scraper/Spark processes, not shared Docker services. Navigation does not
stop collection; a normal portal shutdown does. Restart can process buffered Kafka records.

## Reports, delivery and operational checks

Company reports summarise real saved records and support CSV/JSON/print-to-PDF workflows. Scheduled
daily/weekly summaries use UTC periods and persist one snapshot per current period, with no missed-period
backfill. The portal must remain running. Optional SMTP/webhook delivery requires operator configuration;
inspect delivery status and errors rather than assuming that a configured destination means delivery.
See the supported `CWATCH_*` settings in `portal/company.py`; never commit credentials.

The System Health UI has been removed. `/api/health` remains a minimal public check;
`/api/health/details` is administrator-only. There is no billing, email verification, MFA or
password-recovery service. Contact enquiries download locally and are not submitted by a server.

## Backup and restore

Keep private backups outside the checkout, with appropriate access controls. The database contains
company and authentication state; never upload it to the public repository.

```bash
uv run python -m portal.manage backup --file /safe/private/location/exposureiq.sqlite3
# Stop the portal before restoring:
uv run python -m portal.manage restore --file /safe/private/location/exposureiq.sqlite3
```

Use a new backup destination. `--database` selects a non-default database. The tool uses SQLite's
backup API and integrity checks; restore invalidates sessions, revokes API keys and marks invitations
used. Do not copy only an active SQLite main file and omit its WAL state.

Git does not preserve the ignored data lake, company database, `.env`, browser watchlists,
`torrent_data.zip`, or Docker volumes. Back these up separately if needed; checkpoints alone are not
an observation backup. No cleanup is required to build the report.

## Research and presentation

The supplied presentation is `showcase/Big Data Analytics.pdf`. The replacement academic/product
report is `report/ExposureIQ_Report.pdf`, with editable LaTeX and build instructions alongside it.
Its research results describe the saved October 2026 run, not measurements collected during a rebuild.

```bash
CWATCH_ENABLE_RESEARCH=1 uv run python -m portal.server
# /research.html; administrator authentication required for research APIs
uv run python -m demo.sql
```

The optional `demo/live_demo.sh start` deliberately resets the **separate classroom** stream;
it must not be used to restart ExposureIQ. See `demo/DEMO.md` before using it.

## Compatibility and deployment limits

`CWATCH_*`, `/api/contentwatch`, `X-CWatch-Request`, `cwatch.sqlite3` and the `cwatch` stream/schema
remain internal compatibility identifiers. Renaming them cosmetically would break stored state or
clients without a migration. The user-facing product is ExposureIQ.

Before any internet/commercial deployment, review data licences (especially IMDb non-commercial
inputs), replace unsuitable sources, add HTTPS, recovery and operational safeguards, and validate
tenant isolation and delivery under the deployment model. This pilot is not certified for that use.

Focused checks:

```bash
uv run python -m unittest discover -s portal -p 'test_*.py'
```
