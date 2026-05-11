"""
Komponen 1 — Producer RSS
GitTrend | ETS Big Data | Kelompok 8
Dikerjakan: Syifa Nurul Alfiah (5027241019)

Mengirim artikel RSS ke Kafka topic 'github-rss'.
Polling setiap 5 menit, dedup via MD5 hash URL.
"""
import os, json, time, hashlib
from datetime import datetime
from kafka import KafkaProducer
import feedparser

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RSS       = "github-rss"
POLL_INTERVAL   = 300  # 5 menit

RSS_FEEDS = [
    "https://techcrunch.com/feed/",
    "https://tekno.kompas.com/rss/",
]


def buat_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
                key_serializer=lambda k: k.encode(),
                acks="all",
                linger_ms=20,
            )
            print(f"[producer-rss] Terhubung ke Kafka: {KAFKA_BOOTSTRAP}")
            return p
        except Exception as e:
            print(f"[producer-rss] Kafka belum siap: {e} — retry 10s")
            time.sleep(10)


def fetch_rss(producer, seen_rss):
    sent = 0
    for url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
        except Exception as e:
            print(f"[producer-rss] Error parsing {url}: {e}")
            continue
        for entry in feed.entries:
            link = getattr(entry, "link", "")
            if not link:
                continue
            key = hashlib.md5(link.encode()).hexdigest()[:12]
            if key in seen_rss:
                continue
            seen_rss.add(key)
            event = {
                "title"    : getattr(entry, "title", ""),
                "link"     : link,
                "summary"  : getattr(entry, "summary", "")[:500],
                "published": getattr(entry, "published", ""),
                "source"   : feed.feed.get("title", url),
                "timestamp": datetime.now().isoformat(),
            }
            producer.send(TOPIC_RSS, key=key, value=event).get(timeout=10)
            sent += 1
    producer.flush()
    return sent


if __name__ == "__main__":
    print("[producer-rss] Mulai...")
    time.sleep(20)  # tunggu kafka-init

    producer  = buat_producer()
    seen_rss  = set()

    while True:
        n = fetch_rss(producer, seen_rss)
        print(f"[producer-rss] {n} artikel baru | {datetime.now().strftime('%H:%M:%S')}")
        time.sleep(POLL_INTERVAL)
