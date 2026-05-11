"""
Komponen 1 — Producer API
GitTrend | ETS Big Data | Kelompok 8
Dikerjakan: Syifa Nurul Alfiah (5027241019)

Mengirim data repositori GitHub ke Kafka topic 'github-api'.
- Fase 1: seed dari kaggle CSV (jika tersedia)
- Fase 2: polling GitHub Search API setiap 30 menit
"""
import os, json, csv, time, hashlib, requests
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
GITHUB_PAT      = os.environ.get("GITHUB_PAT", "")
TOPIC_API       = "github-api"
KAGGLE_CSV      = "github_trending_repos.csv"
POLL_INTERVAL   = 1800  # 30 menit


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
            print(f"[producer-api] Terhubung ke Kafka: {KAFKA_BOOTSTRAP}")
            return p
        except Exception as e:
            print(f"[producer-api] Kafka belum siap: {e} — retry 10s")
            time.sleep(10)


def seed_kaggle(csv_path, producer):
    if not os.path.exists(csv_path):
        print(f"[producer-api] File CSV tidak ditemukan: {csv_path}, skip seed.")
        return
    seen, sent = set(), 0
    with open(csv_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = row.get("full_name", "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            event = {
                "full_name"        : key,
                "description"      : row.get("description", "") or "",
                "language"         : row.get("language", "") or "Unknown",
                "stargazers_count" : int(row.get("stars", 0) or 0),
                "topics"           : [],
                "html_url"         : row.get("url", ""),
                "forks_count"      : int(row.get("forks", 0) or 0),
                "sumber"           : "kaggle_seed",
                "timestamp"        : datetime.now().isoformat(),
            }
            producer.send(TOPIC_API, key=key, value=event).get(timeout=10)
            sent += 1
            if sent % 200 == 0:
                print(f"[producer-api] Seed: {sent} repo terkirim...")
    producer.flush()
    print(f"[producer-api] Seed selesai: {sent} repo")


def fetch_github():
    kemarin = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_PAT:
        headers["Authorization"] = f"token {GITHUB_PAT}"
    try:
        resp = requests.get(
            "https://api.github.com/search/repositories",
            headers=headers,
            params={"q": f"created:>{kemarin}", "sort": "stars", "order": "desc", "per_page": 30},
            timeout=15,
        )
        remaining = resp.headers.get("X-RateLimit-Remaining", "?")
        print(f"[producer-api] GitHub rate limit remaining: {remaining}")
        resp.raise_for_status()
        return resp.json().get("items", [])
    except Exception as e:
        print(f"[producer-api] GitHub API error: {e}")
        return []


def publish_github(producer, repos):
    sent = 0
    for repo in repos:
        key = repo.get("full_name", "")
        if not key:
            continue
        event = {
            "full_name"        : key,
            "description"      : repo.get("description", "") or "",
            "language"         : repo.get("language", "") or "Unknown",
            "stargazers_count" : repo.get("stargazers_count", 0),
            "topics"           : repo.get("topics", []),
            "html_url"         : repo.get("html_url", ""),
            "forks_count"      : repo.get("forks_count", 0),
            "sumber"           : "github_api_live",
            "timestamp"        : datetime.now().isoformat(),
        }
        producer.send(TOPIC_API, key=key, value=event).get(timeout=10)
        sent += 1
    producer.flush()
    return sent


if __name__ == "__main__":
    print("[producer-api] Mulai...")
    time.sleep(20)  # tunggu kafka-init selesai buat topic

    producer = buat_producer()

    # Fase 1: seed Kaggle
    seed_kaggle(KAGGLE_CSV, producer)

    # Fase 2: polling terus
    while True:
        repos = fetch_github()
        n = publish_github(producer, repos)
        print(f"[producer-api] Live: {n} repo terkirim | {datetime.now().strftime('%H:%M:%S')}")
        time.sleep(POLL_INTERVAL)
