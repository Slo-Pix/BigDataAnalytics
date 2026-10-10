"""Project portal: a local web app that shows the running big-data system live.

  http://localhost:8800   (binds to 127.0.0.1 only — it can start/stop the demo and run Spark SQL;
                           port 8800, not 8000, so it never shares a browser origin with other local apps;
                           override with PORTAL_PORT)

Data comes live from: PostgreSQL (stream serving tables, schemas public + demo), Kafka (topics, end offsets,
latest messages), the Parquet data lake (row counts, schemas, row previews via pyarrow), Docker (container
status), and Spark SQL (a SparkSession started on the first query, with every table registered as a view).

Usage: uv run python -m portal.server
"""
import csv
import os
import json
import socket
import subprocess
import threading
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
import pyarrow.dataset as ds
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from common.paths import CURATED, ROOT, STAGING, STREAM

PG = "postgresql://bda:bda@localhost:5432/torrent"
KAFKA = "localhost:9092"
STATIC = Path(__file__).parent / "static"
LAKE_DIRS = {"staging": STAGING, "curated": CURATED, "stream": STREAM}
SERVICES = [  # name, port, url, role
    ("Apache Kafka", 9092, None, "Ingestion buffer · topic tracker.scrapes"),
    ("Kafka UI", 8080, "http://localhost:8080", "Browse topics, partitions and messages"),
    ("PostgreSQL", 5432, None, "Serving layer for Grafana and this portal"),
    ("Grafana", 3000, "http://localhost:3000", "Live, demo and batch dashboards"),
    ("Spark UI (stream job)", 4040, "http://localhost:4040", "Structured Streaming jobs, stages, micro-batches"),
    ("Spark UI (portal SQL)", 4041, "http://localhost:4041", "Spark SQL queries run from this portal"),
]

app = FastAPI(title="Torrent BDA portal")


def jsonable(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat(sep=" ", timespec="seconds") if isinstance(v, datetime) else v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    if isinstance(v, bytes):
        return v.hex()
    return v


def pg(sql: str, params=()):
    with psycopg.connect(PG, connect_timeout=3) as con:
        cur = con.execute(sql, params)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, (jsonable(x) for x in row))) for row in cur.fetchall()]


def port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


# ---------------------------------------------------------------- overview & services
@app.get("/api/services")
def services():
    try:
        out = subprocess.run(["docker", "ps", "-a", "--format", "{{json .}}"], capture_output=True, text=True, timeout=5).stdout
        containers = [json.loads(line) for line in out.splitlines() if line.strip()]
    except Exception:  # noqa: BLE001
        containers = []
    stream_pid = ROOT / "logs" / "demo_stream.pid"
    demo_running = stream_pid.exists() and subprocess.run(["kill", "-0", stream_pid.read_text().strip()],
                                                          capture_output=True).returncode == 0
    return {
        "services": [{"name": n, "port": p, "url": u, "role": r, "up": port_open(p)} for n, p, u, r in SERVICES],
        "containers": [{"name": c["Names"], "image": c["Image"], "status": c["Status"], "up": c["State"] == "running"}
                       for c in containers if c["Names"] in
                       ("kafka", "kafka-ui", "postgres", "grafana", "scraper", "scraper-wide", "scraper-demo")],
        "demo_stream_job": demo_running,
    }


@app.get("/api/overview")
def overview():
    inv = {r["table_name"]: r["rows"] for r in pg("SELECT table_name, rows FROM analytics.dataset_inventory")}
    live = pg("SELECT coalesce(sum(rows),0) AS rows, count(*) AS batches FROM public.stream_batches")[0]
    demo = pg("SELECT coalesce(sum(rows),0) AS rows, count(*) AS batches, max(applied_at) AS last FROM demo.stream_batches")[0]
    per_10min = pg("""SELECT date_trunc('hour', minute) + floor(extract(minute FROM minute) / 30) * interval '30 min' AS t,
                             sum(rows) AS rows FROM public.swarm_minute GROUP BY 1 ORDER BY 1""")
    alive = pg("SELECT round(sum(pct_alive * torrents)) AS n FROM analytics.a3_alive_by_upload_year")[0]["n"]
    return {
        "lake_rows": sum(inv.values()), "wiki_rows": inv.get("wiki_pageviews"), "torrents": inv.get("torrents"),
        "imdb_rows": sum(v for k, v in inv.items() if k.startswith("imdb_")),
        "stream_rows": live["rows"], "stream_batches": live["batches"], "alive": alive,
        "demo": demo, "collection_series": per_10min,
    }


# ---------------------------------------------------------------- live stream
@app.get("/api/stream")
def stream(schema: str = "demo"):
    if schema not in ("demo", "public"):
        raise HTTPException(400, "schema must be demo or public")
    window = "now() - interval '20 minutes'" if schema == "demo" else "(SELECT max(minute) FROM public.swarm_minute) - interval '3 hours'"
    return {
        "totals": pg(f"SELECT coalesce(sum(rows),0) AS rows, count(*) AS batches, max(applied_at) AS last FROM {schema}.stream_batches")[0],
        "torrents": pg(f"SELECT count(DISTINCT infohash) AS n FROM {schema}.swarm_latest")[0]["n"],
        "per_minute": pg(f"SELECT minute AS t, sum(rows) AS rows FROM {schema}.swarm_minute WHERE minute > {window} GROUP BY 1 ORDER BY 1"),
        "batches": pg(f"SELECT batch_id, rows, applied_at FROM {schema}.stream_batches ORDER BY batch_id DESC LIMIT 10"),
        "top": pg(f"""SELECT coalesce(t.title, l.infohash) AS title, t.category, max(l.seeders) AS seeders,
                             max(l.leechers) AS leechers, max(l.completed) AS completed, max(l.scraped_at) AS seen
                      FROM {schema}.swarm_latest l LEFT JOIN public.torrents t USING (infohash)
                      GROUP BY 1, 2 ORDER BY seeders DESC LIMIT 12"""),
        "trackers": pg(f"""SELECT tracker, sum(rows) AS rows FROM {schema}.swarm_minute WHERE minute > {window}
                           GROUP BY 1 ORDER BY 2 DESC"""),
    }


DEMO_LOCK = threading.Lock()


@app.post("/api/demo/{action}")
def demo(action: str):
    if action not in ("start", "stop"):
        raise HTTPException(400, "start or stop")
    if not DEMO_LOCK.acquire(blocking=False):
        raise HTTPException(409, "demo is already starting or stopping")

    def run():
        try:
            subprocess.run([str(ROOT / "demo" / "live_demo.sh"), action], cwd=ROOT, capture_output=True, timeout=300)
        finally:
            DEMO_LOCK.release()
    threading.Thread(target=run, daemon=True).start()
    return {"ok": True, "action": action}


# ---------------------------------------------------------------- kafka
@app.get("/api/kafka")
def kafka(topic: str | None = None, n: int = 15):
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=KAFKA, request_timeout_ms=5000, api_version_auto_timeout_ms=5000,
                             consumer_timeout_ms=1500, enable_auto_commit=False)
    try:
        topics = sorted(t for t in consumer.topics() if not t.startswith("__"))
        info = []
        for t in topics:
            parts = [TopicPartition(t, p) for p in sorted(consumer.partitions_for_topic(t) or [])]
            end, begin = consumer.end_offsets(parts), consumer.beginning_offsets(parts)
            info.append({"topic": t, "partitions": [{"partition": tp.partition, "begin": begin[tp], "end": end[tp]} for tp in parts],
                         "messages": sum(end[tp] - begin[tp] for tp in parts)})
        topic = topic or ("tracker.scrapes.demo" if "tracker.scrapes.demo" in topics else (topics[0] if topics else None))
        messages = []
        if topic:
            parts = [TopicPartition(topic, p) for p in sorted(consumer.partitions_for_topic(topic) or [])]
            consumer.assign(parts)
            end = consumer.end_offsets(parts)
            for tp in parts:
                consumer.seek(tp, max(0, end[tp] - n))
            batch = consumer.poll(timeout_ms=2000, max_records=n * len(parts))
            for tp, recs in batch.items():
                for r in recs:
                    try:
                        value = json.loads(r.value)
                    except Exception:  # noqa: BLE001
                        value = r.value.decode(errors="replace")
                    messages.append({"partition": tp.partition, "offset": r.offset, "timestamp": r.timestamp, "value": value})
            messages.sort(key=lambda m: m["timestamp"], reverse=True)
        return {"bootstrap": KAFKA, "topics": info, "topic": topic, "messages": messages[:n]}
    finally:
        consumer.close()


# ---------------------------------------------------------------- dataset (Parquet data lake)
_TABLE_CACHE = {"at": 0, "data": None}


def lake_tables():
    for layer, base in LAKE_DIRS.items():
        for p in sorted(base.iterdir()):
            if p.is_dir() and not p.name.startswith("_") and "jsonl" not in p.name:
                yield layer, p


def open_ds(path: Path):
    return ds.dataset(path, format="parquet", partitioning="hive", exclude_invalid_files=True)


@app.get("/api/tables")
def tables():
    if _TABLE_CACHE["data"] is None or time.time() - _TABLE_CACHE["at"] > 30:
        rows = []
        for layer, p in lake_tables():
            try:
                d = open_ds(p)
                rows.append({"layer": layer, "name": p.name, "rows": d.count_rows(), "files": len(d.files),
                             "size_mb": round(sum(Path(f).stat().st_size for f in d.files) / 1e6, 1),
                             "columns": [{"name": f.name, "type": str(f.type)} for f in d.schema]})
            except Exception:  # noqa: BLE001 - not a Parquet dataset (yet)
                continue
        rows.sort(key=lambda r: -r["rows"])
        _TABLE_CACHE.update(at=time.time(), data=rows)
    return _TABLE_CACHE["data"]


@app.get("/api/tables/{layer}/{name}")
def preview(layer: str, name: str, n: int = 25):
    base = LAKE_DIRS.get(layer)
    path = (base / name) if base else None
    if not path or not path.is_dir() or path.parent != base:
        raise HTTPException(404, "no such table")
    t = open_ds(path).head(min(n, 200))
    cols = t.column_names
    data = [[jsonable(v) for v in row.values()] for row in t.to_pylist()]
    return {"columns": cols, "rows": data}


# ---------------------------------------------------------------- Spark SQL
class Query(BaseModel):
    sql: str


_SPARK = {"session": None}
SPARK_LOCK = threading.Lock()


def spark_session():
    if _SPARK["session"] is None:
        from pyspark.sql import SparkSession

        from common.spark import get_spark
        from demo.sql import LAKE

        SparkSession.builder.config("spark.ui.port", "4041")  # the stream job keeps 4040
        spark = get_spark("portal-sql", mem="4g")
        spark.sparkContext.setLogLevel("ERROR")
        for name, path in LAKE.items():
            spark.read.parquet(str(path)).createOrReplaceTempView(name)
        _SPARK["session"] = spark
    return _SPARK["session"]


@app.get("/api/queries")
def queries():
    from demo.sql import prepared

    return [{"title": t, "sql": s} for t, s in prepared()]


@app.post("/api/sql")
def sql(q: Query):
    text = q.sql.strip().rstrip(";")
    if not text:
        raise HTTPException(400, "empty query")
    with SPARK_LOCK:
        t0 = time.time()
        spark = spark_session()
        started = time.time() - t0
        if "demo_swarm" in text:
            if not (STREAM / "demo").exists():
                raise HTTPException(400, "the demo stream has not written anything yet — start it on the Live stream page")
            spark.read.parquet(str(STREAM / "demo")).createOrReplaceTempView("demo_swarm")
        t1 = time.time()
        try:
            df = spark.sql(text)
            rows = df.limit(200).collect()
        except Exception as e:  # noqa: BLE001
            raise HTTPException(400, str(e).splitlines()[0][:500]) from None
        return {"columns": df.columns, "rows": [[jsonable(v) for v in r] for r in rows],
                "seconds": round(time.time() - t1, 2), "spark_startup_seconds": round(started, 1)}


# ---------------------------------------------------------------- findings & validation
@app.get("/api/validation")
def validation():
    with open(ROOT / "analysis" / "results" / "validation.csv", newline="") as f:
        return list(csv.DictReader(f))


app.mount("/figures", StaticFiles(directory=ROOT / "report" / "figures"), name="figures")


@app.get("/report.pdf")
def report():
    return FileResponse(ROOT / "report" / "CS404_Torrent_Analytics_Report.pdf", media_type="application/pdf")


app.mount("/", StaticFiles(directory=STATIC, html=True), name="static")


if __name__ == "__main__":
    port = int(os.environ.get("PORTAL_PORT", "8800"))
    print(f"Dashboard: http://localhost:{port}")
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
