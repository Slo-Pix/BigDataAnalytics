# Live demo runbook (≈ 10 minutes)

Everything here is isolated from the 10 Oct dataset: the demo uses its own Kafka topic (`tracker.scrapes.demo`),
Parquet folder (`dataset/stream/demo`) and PostgreSQL schema (`demo`), so the report's numbers and
`analysis/validate.py` are unaffected. Needs internet (UDP to the public trackers).

## Before the professor arrives (2 minutes)
```bash
cd ~/Downloads/bda_pro
docker compose -f infra/docker-compose.yml up -d     # Kafka, Kafka UI, PostgreSQL, Grafana
uv run python -m portal.server                       # the project portal: http://localhost:8000
```
Open **http://localhost:8000** and press **Start demo** in the sidebar (or run `demo/live_demo.sh start`).
The portal is the single entry point: Overview → Live stream → Kafka (raw messages) → Dataset (row previews)
→ Spark SQL (run queries in the browser) → Findings → Tools (links to Grafana, Kafka UI :8080, Spark UI :4040/:4041).
The terminal console below and the Grafana dashboards show the same things in the classic tools.

## 1. The streaming pipeline, live (3 min) — http://localhost:3000/d/live-demo
- "Micro-batches applied" and the **Latest micro-batches** table grow every 10 s: each row is one Spark
  Structured Streaming micro-batch written to PostgreSQL. Point at batch ids increasing and `applied_at`.
- Path: scraper container → Kafka topic → Spark → PostgreSQL → this dashboard (refresh 5 s).
- Exactly-once: each batch id is stored in the same transaction as its data; a replayed batch is skipped.
- Show the scraper's raw traffic: `docker logs -f scraper-demo` ("round done … 2000 of 2000 hashes").
- `demo/live_demo.sh status` prints the same counts from the database.

## 2. Ad-hoc Spark SQL on the data lake (4 min)
```bash
uv run python -m demo.sql          # ~15 s to start Spark; \list shows the prepared queries
```
| Command | What it shows | Time |
|---|---|---|
| `\run 1` | count **1,153,632,847** Wikipedia rows | ≈ 10 s |
| `\run 4` | one film end to end (Dune 2021: first torrent a WEB copy 4 days before legal home release) — edit the title in `demo/queries.sql` or type your own | ≈ 2 s |
| `\run 5` | leak lag by release year (A2) | seconds |
| `\run 7` | games pirated by price band (A5) | seconds |
| `\run 10` | rows the **live demo stream** has written to Parquet — run twice, the number grows | < 1 s |
| `\run 11` | busiest swarms right now from the live stream | ≈ 1 s |
| `\run 9` | the 10 Oct stream per tracker (total 25,422,936) | seconds |

The professor can also type any SQL ending with `;`, e.g.
`SELECT category, count(*) FROM torrents GROUP BY category;`. `\tables` lists every table with its row count.

## 3. Proof it is right (2 min)
```bash
uv run python -m analysis.validate      # 26 / 26 checks, ≈ 3 min — or show analysis/results/validation.csv
```
The Kafka count in the stream checks is the value measured at 21:45 IST on 10 Oct; the original topic was lost
when the Kafka container was recreated afterwards (see `analysis/results/kafka_offsets_snapshot.json`). Parquet,
the PostgreSQL ledger and the JSONL backups are re-counted live.

## 4. Batch results (1 min) — http://localhost:3000/d/batch-analytics

## After
```bash
demo/live_demo.sh stop
```
