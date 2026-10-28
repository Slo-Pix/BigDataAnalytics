# ExposureIQ local company pilot

## What this delivers

ExposureIQ turns the existing research pipeline into a company review workflow for movies, television and
games. It is a working **local pilot**, not a licensed commercial service, a piracy-removal service,
or an internet-ready multi-tenant deployment. The original research, datasets and reports remain intact.

The public website is one scrollable dark landing page: Platform, Pricing, About and Contact links
scroll to sections without replacing the page. Pricing is illustrative; no payments are collected.
Contact downloads an enquiry file for sharing manually, not a submitted lead.

## Start and sign in

From the repository root:

```bash
uv sync
uv run python -m portal.server
```

Open <http://localhost:8800>. Select **Create workspace**, enter your company, name, email and a
password of at least 12 characters. The first member of each company is its administrator. No default
account or password is installed. Email is an account identifier; the pilot does not verify ownership,
send invitation mail, or provide forgotten-password recovery. Keep access local and credentials safe.

Use one portal process. It binds to `127.0.0.1`; do not expose it through a public proxy or port-forward.
`PORTAL_PORT` changes the port. `CWATCH_APP_DB` overrides the company database path for isolated testing;
the default is `dataset/app/cwatch.sqlite3`. This private SQLite database is separate from the public
Parquet/PostgreSQL research observations. Its credentials and session tokens are hashed at rest.
Personal watchlists use company/user-scoped browser storage and do not synchronise across devices.

The product is branded ExposureIQ. Existing `CWATCH_*` environment variables, `X-CWatch-Request`,
API paths, database filenames and storage keys retain their original names for compatibility;
renaming the product does not reset company accounts, saved records or pipeline checkpoints.

### Roles

| Action | Administrator | Analyst | Viewer / API key |
|---|---|---|---|
| Read company catalogue, checks, cases, alerts and reports | Yes | Yes | Yes |
| Import/approve titles; check trackers; edit cases/rules; schedule reports | Yes | Yes | No |
| Query shared public live SQL | Yes | Yes | No |
| Start/stop shared streaming collector | Yes | No | No |
| Invite team, change roles, issue/revoke keys, view audit/detailed health | Yes | No | No |

Invitations are single-use, expire after 24 hours, and must be shared privately. At least one administrator
must remain. API keys are shown once, stored hashed, revocable, and always read-only regardless of their
creator's role. They are intended for company-record integrations, not unrestricted SQL access.

```bash
curl -H "Authorization: Bearer YOUR_READ_ONLY_KEY" \
  http://localhost:8800/api/company/portfolio
```

API reference: <http://localhost:8800/docs>. Private endpoints still require credentials. Browser/API
mutations require `X-CWatch-Request: 1`; browser origins must match the portal. Session lifetime is
eight hours. Local in-memory rate limits include 180 authenticated requests/minute/user, 120/minute/key,
20 login attempts/minute/IP, and 10 registrations/hour/IP.

## Demonstrate the complete workflow

1. **Onboard titles.** Open Company catalogue. Download the CSV template or import a UTF-8 CSV:

   ```csv
   title,category,external_id
   Inception,movie,tt1375666
   The Sopranos,tv,
   Portal 2,game,
   ```

   Accepted categories are `movie`, `tv`, `game`; identifiers are optional IMDb or Steam IDs. Imports
   are limited to 200 rows and 1 MB. Imported records stay pending, even for an apparently exact match.
   Review the candidate and explicitly approve it. Title search also allows deliberate direct addition
   of a selected match. An approval means a human selected metadata, not proof of ownership or infringement.

2. **Configure a rule.** Open Alerts & rules. Create a title-specific or all-company rule. The inclusive
   threshold uses the **maximum seeder count reported for one hash on one tracker**, never a sum.
   For a demonstration, threshold `0` will trigger when any tracker successfully responds, including a
   legitimate zero count. An unavailable check never triggers an alert.

3. **Check seeders now.** For an approved catalogue item select Check seeders. Alternatively search a
   title in Check live seeders. Each request queries five configured public UDP trackers for at most
   20 known matching hashes. The result states time, sampled/known hashes, responding/expected trackers,
   per-hash observations and explicit failures. Requests are limited to two concurrent checks globally
   and ten/minute/user. Failed trackers are **unavailable**, not zero.

   Checks initiated from an approved company catalogue item are saved and evaluated against rules.
   Ad-hoc search checks are not added to a company until the user chooses a catalogue match.
   Saved company checks can be reopened as complete JSON records from the title-check history.
   Immediate CSV exports include title/key, timestamps, coverage and methodology. Past checks describe
   that recorded time, not necessarily today's availability.

4. **Investigate rather than accuse.** Use Investigate on a catalogue item, create a case, assign an
   administrator/analyst, and add notes. Statuses are open, investigating, actioned and closed. Record
   matching/ownership/territory checks and any authorised external referral. Latest saved observations
   are referenced on the case; the original observation records remain available in check history.

5. **Review alerts.** One unresolved alert per rule/title prevents duplicate flooding. Review/resolve
   alerts and inspect each delivery channel's status/error. In-app alerts need no external setup.
   Retry delivery explicitly after fixing missing or failed operator configuration; successful deliveries
   are not resent. Rules run when a saved portfolio check occurs, not continuously over every title.

6. **Export and report.** Company reports exports the catalogue as CSV and summary as JSON. Print / Save
   PDF uses the browser's print dialog and a report-only light layout. Daily/weekly schedules save UTC
   period snapshots while the server is online, checked once per minute; they do not email reports or
   backfill all missed periods. Wait up to a minute for the first saved snapshot.

7. **Verify isolation.** Invite analyst and viewer accounts and verify the role matrix. Create a second
   company in another browser profile: it should not see the first company's portfolio, cases, checks,
   alerts, reports, team or audit entries. Public research metadata remains shared intentionally.

8. **Try the shared pipeline.** Administrator: open Live pipeline & SQL, choose 100 known hashes, start,
   wait for a committed batch, and run an example query. Stop collection after testing. Read-only SQL
   remains available over retained data. It cannot access the private SQLite company database.

   Kafka/PostgreSQL Docker services, Java, curated source data and historical PostgreSQL serving tables
   are prerequisites. Startup can take 1–2 minutes. The collector resumes its isolated topic, checkpoint,
   Parquet output and PostgreSQL schema rather than resetting research data. All local companies share
   this public infrastructure; an administrator's start/stop affects that shared collector.

The automated regression suite requires the project's Python environment but does not send emails,
webhooks or UDP tracker traffic:

```bash
uv run python -m unittest discover -s portal -p 'test_*.py'
```

## Optional notification configuration

Set operator environment variables **before** starting the portal. Do not commit credentials.

| Variable | Requirement / default |
|---|---|
| `CWATCH_SMTP_HOST` | Email server; unset disables transport |
| `CWATCH_SMTP_PORT` | `587` by default |
| `CWATCH_SMTP_FROM` | Required valid sender address |
| `CWATCH_ALERT_EMAIL` | Required valid operator-selected recipient |
| `CWATCH_SMTP_USER`, `CWATCH_SMTP_PASSWORD` | Both set or both absent |
| `CWATCH_ALERT_WEBHOOK` | Optional HTTPS URL on port 443 |

SMTP requires verified STARTTLS; implicit SMTPS on port 465 is unsupported. Webhooks require verified
TLS and exclusively public resolved addresses; private addresses and redirects are rejected. DNS
lookup timeout is four seconds and socket-operation timeout is eight seconds. Destinations are
operator-controlled and shared by this local installation, not editable customer webhook URLs.

Missing/invalid configuration remains visibly `unconfigured`, transport errors remain `failed`, and a
channel becomes `sent` only after successful transport. No external delivery is implied by an in-app
alert. The background worker processes delivery once per minute while the portal is running.

## Health, backup and recovery

- Public liveness: `/api/health`; administrator details: `/api/health/details`.
- The System Health workspace page has been removed. The administrator-only endpoint still reports
  database size, background maintenance errors, and email/webhook configuration. Configured delivery
  channels are not proof of successful delivery; review individual results in Alerts & Rules.
- Maintenance errors are logged and exposed in health, not silently reported as successful.
- Collection logs: `logs/cwatch-stream.log`, `logs/cwatch-scraper.log`.
- Stop collection using its button or shut down the portal normally; never use the classroom reset
  script to manage the company pilot.
- Application records, audit history and scheduled snapshots have no automatic expiry. The check-history
  UI is bounded, not a promise of unlimited list output. Agree a retention/deletion policy before using
  real customer information; this pilot does not implement GDPR/self-service company erasure.

Back up the live company database with SQLite's consistent backup API:

```bash
uv run python -m portal.manage backup --file /safe/location/cwatch-backup.sqlite3
```

An existing backup file is never silently overwritten. Keep it private: it contains company information
and password hashes. This command does **not** back up public Parquet data, PostgreSQL, Kafka or browser
watchlists; manage those separately. Protect backups with filesystem permissions and encrypted storage.

To restore, stop the portal, preserve a backup of the current database, then:

```bash
uv run python -m portal.manage restore --file /safe/location/cwatch-backup.sqlite3
```

Restore explicitly replaces the selected company database. Integrity/schema checks run first; restore
refuses while the configured portal port is responding. Restored sessions, invitations and API keys
are invalidated; users must sign in again and administrators must issue new invites/keys. Use
`--database /explicit/path.sqlite3` when managing a non-default database. This local port check is not
a distributed deployment lock; do not run additional server processes against the same database.

The administrative research UI/API is preserved but disabled by default. Only for a trusted local
presentation, restart with `CWATCH_ENABLE_RESEARCH=1`, sign in as administrator, then open
`/research.html`. It includes powerful legacy administrative operations and must never be published.

## How companies can use the data responsibly

The sellable value is **less analyst effort and a consistent review trail**, not a guarantee of
preventing piracy:

1. Prioritise titles with observed activity within the monitored sample.
2. Verify exact release/episode/edition, rights ownership, territory and authorised distribution.
3. Gather the actual public listing/URL and required legal evidence through authorised providers;
   a torrent hash and tracker count alone are insufficient for a takedown notice.
4. Assign a case to a rights-holder or authorised content-protection team. That team can independently
   evaluate platform reports, search delisting, hosting requests or other lawful enforcement channels.
5. Recheck the same monitored hashes after an intervention, recording time and tracker coverage.
   Changes may have unrelated causes and must not be presented as proven enforcement impact.
6. Use aggregate findings alongside legitimate sales and availability data to assess distribution
   gaps; do not equate reported seeders with lost sales.

Tracker counts overlap across trackers/releases and are not unique people, downloads, infringement
determinations or revenue losses. No copyrighted media is downloaded and no peer IP addresses are
stored. The listing catalogue ends in May 2023: this pilot does not automatically discover new releases
or provide complete global coverage. TV matching can group episodes/seasons and game editions may be
imperfect. Missing results do not prove absence of exposure.

Before any sale or internet deployment: complete source-by-source licensing review (including IMDb's
non-commercial research inputs), procure permitted current discovery feeds, establish rights/evidence
and retention policies, add verified onboarding/recovery/MFA and HTTPS, remove legacy administrative
exposure, and validate hosting, monitoring, restore procedures, notification destinations and support
commitments. The pilot has no payment processing, SLA or commercial certification.
