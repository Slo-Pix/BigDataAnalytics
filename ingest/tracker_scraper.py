"""BEP 15 UDP tracker scraper.

Repeatedly scrapes seeders / completed / leechers for a list of infohashes from public
UDP trackers and appends rows to hourly JSONL files (data/stream/jsonl/<date>/<hour>.jsonl).
With --kafka, rows are also published to the `tracker.scrapes` topic.

Only aggregate swarm counts are collected: no peer IPs, no content.

Usage:
    uv run python -m ingest.tracker_scraper --hashes data/raw/infohashes.txt
    uv run python -m ingest.tracker_scraper --hashes ... --kafka localhost:9092
"""
import argparse
import json
import logging
import random
import socket
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from common.spark import STREAM

PROTOCOL_ID = 0x41727101980
ACTION_CONNECT, ACTION_SCRAPE, ACTION_ERROR = 0, 2, 3
MAX_HASHES_PER_SCRAPE = 74  # keeps the packet under typical MTU
CONN_ID_TTL = 50  # seconds; BEP 15 says 60

DEFAULT_TRACKERS = [
    "tracker.opentrackr.org:1337",
    "open.stealth.si:80",
    "tracker.torrent.eu.org:451",
    "exodus.desync.com:6969",
    "open.demonii.com:1337",
]

log = logging.getLogger("scraper")


class TrackerError(Exception):
    pass


class UDPTracker:
    def __init__(self, hostport: str, timeout: float = 5.0, retries: int = 2):
        host, port = hostport.rsplit(":", 1)
        self.name = hostport
        self.addr = (socket.gethostbyname(host), int(port))
        self.timeout, self.retries = timeout, retries
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(timeout)
        self.conn_id, self.conn_time = None, 0.0

    def _request(self, payload_fn, expected_action: int) -> bytes:
        for _ in range(self.retries + 1):
            tid = random.getrandbits(32)
            self.sock.sendto(payload_fn(tid), self.addr)
            try:
                while True:
                    data, _ = self.sock.recvfrom(65535)
                    if len(data) >= 8 and struct.unpack(">I", data[4:8])[0] == tid:
                        break  # ignore stale replies from earlier timed-out requests
            except socket.timeout:
                continue
            action = struct.unpack(">I", data[:4])[0]
            if action == ACTION_ERROR:
                raise TrackerError(data[8:].decode(errors="replace"))
            if action != expected_action:
                raise TrackerError(f"unexpected action {action}")
            return data[8:]
        raise TrackerError("timeout")

    def _connect(self):
        body = self._request(lambda tid: struct.pack(">QII", PROTOCOL_ID, ACTION_CONNECT, tid), ACTION_CONNECT)
        self.conn_id = struct.unpack(">Q", body[:8])[0]
        self.conn_time = time.monotonic()

    def scrape(self, hashes: list[str]) -> list[tuple[str, int, int, int]]:
        if self.conn_id is None or time.monotonic() - self.conn_time > CONN_ID_TTL:
            self._connect()
        raw = b"".join(bytes.fromhex(h) for h in hashes)
        body = self._request(
            lambda tid: struct.pack(">QII", self.conn_id, ACTION_SCRAPE, tid) + raw, ACTION_SCRAPE
        )
        out = []
        for i, h in enumerate(hashes):
            chunk = body[i * 12 : i * 12 + 12]
            if len(chunk) < 12:
                break
            seeders, completed, leechers = struct.unpack(">III", chunk)
            out.append((h, seeders, completed, leechers))
        return out


class Sink:
    """Writes rows to hourly JSONL files and optionally Kafka."""

    def __init__(self, kafka: str | None):
        self.fh, self.fh_key = None, None
        self.lock = threading.Lock()
        self.producer = None
        if kafka:
            from kafka import KafkaProducer

            self.producer = KafkaProducer(
                bootstrap_servers=kafka, value_serializer=lambda v: json.dumps(v).encode(), linger_ms=200
            )

    def _file(self, now: datetime):
        key = now.strftime("%Y-%m-%d/%H")
        if key != self.fh_key:
            if self.fh:
                self.fh.close()
            path = STREAM / "jsonl" / f"{key}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            self.fh, self.fh_key = open(path, "a", buffering=1 << 16), key
        return self.fh

    def write(self, tracker: str, rows):
        now = datetime.now(timezone.utc)
        ts = now.isoformat(timespec="seconds")
        with self.lock:
            fh = self._file(now)
            for h, s, c, l in rows:
                rec = {"infohash": h, "tracker": tracker, "seeders": s, "completed": c, "leechers": l, "scraped_at": ts}
                fh.write(json.dumps(rec) + "\n")
                if self.producer:
                    self.producer.send("tracker.scrapes", rec)

    def flush(self):
        with self.lock:
            if self.fh:
                self.fh.flush()
        if self.producer:
            self.producer.flush()


def load_hashes(path: Path, limit: int | None) -> list[str]:
    hashes = []
    for line in path.read_text().splitlines():
        h = line.strip().lower()
        if len(h) == 40 and all(c in "0123456789abcdef" for c in h):
            hashes.append(h)
    hashes = list(dict.fromkeys(hashes))
    return hashes[:limit] if limit else hashes


def scrape_tracker(hostport: str, batches: list[list[str]], sink: Sink) -> int:
    try:
        tr = UDPTracker(hostport)
    except OSError as e:
        log.warning("%s: resolve failed (%s)", hostport, e)
        return 0
    n, errors = 0, 0
    for batch in batches:
        try:
            rows = tr.scrape(batch)
        except (TrackerError, OSError) as e:
            errors += 1
            tr.conn_id = None
            if errors >= 10:
                log.warning("%s: giving up this round after %d errors (last: %s)", hostport, errors, e)
                break
            continue
        sink.write(hostport, rows)
        n += len(rows)
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hashes", type=Path, required=True, help="text file, one hex infohash per line")
    ap.add_argument("--limit", type=int, default=None, help="only scrape the first N hashes")
    ap.add_argument("--trackers", nargs="*", default=DEFAULT_TRACKERS)
    ap.add_argument("--interval", type=int, default=3600, help="seconds between round starts")
    ap.add_argument("--kafka", default=None, help="bootstrap servers, e.g. localhost:9092")
    ap.add_argument("--once", action="store_true", help="run a single round and exit")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sink = Sink(args.kafka)

    while True:
        start = time.monotonic()
        hashes = load_hashes(args.hashes, args.limit)  # re-read each round so the list can grow
        batches = [hashes[i : i + MAX_HASHES_PER_SCRAPE] for i in range(0, len(hashes), MAX_HASHES_PER_SCRAPE)]
        # trackers in parallel; each tracker's batches sequentially (be polite)
        with ThreadPoolExecutor(len(args.trackers)) as pool:
            counts = dict(zip(args.trackers, pool.map(lambda t: scrape_tracker(t, batches, sink), args.trackers)))
        sink.flush()
        log.info("round done in %.0fs, %d hashes: %s", time.monotonic() - start, len(hashes), counts)
        if args.once:
            break
        time.sleep(max(0, args.interval - (time.monotonic() - start)))


if __name__ == "__main__":
    main()
