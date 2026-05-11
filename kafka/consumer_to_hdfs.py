"""
Komponen 2 — Consumer to HDFS
GitTrend | ETS Big Data | Kelompok 8
Dikerjakan: Nisrina Bilqis (5027241054)

Membaca dari kedua Kafka topic, menyimpan batch ke HDFS via WebHDFS.
Juga update live_api.json dan live_rss.json untuk dashboard.
"""
import os, json, time, threading
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HDFS_URL        = os.environ.get("HDFS_URL", "http://namenode:9870")
TOPIC_API       = "github-api"
TOPIC_RSS       = "github-rss"
DASHBOARD_DIR   = "/app/data"
FLUSH_COUNT     = 50
FLUSH_SECONDS   = 120

os.makedirs(DASHBOARD_DIR, exist_ok=True)


def get_hdfs_client():
    while True:
        try:
            client = InsecureClient(HDFS_URL, user="root")
            # Test koneksi
            client.status("/")
            print(f"[consumer-hdfs] Terhubung ke HDFS: {HDFS_URL}")
            return client
        except Exception as e:
            print(f"[consumer-hdfs] HDFS belum siap: {e} — retry 15s")
            time.sleep(15)


def simpan_ke_hdfs(client, buffer, hdfs_path, label):
    if not buffer:
        return
    ts      = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    target  = f"{hdfs_path}/{ts}.json"
    try:
        with client.write(target, encoding="utf-8", overwrite=True) as writer:
            json.dump(buffer, writer, ensure_ascii=False)
        print(f"[consumer-hdfs] [{label}] {len(buffer)} event → {target}")
    except Exception as e:
        print(f"[consumer-hdfs] [{label}] Gagal ke HDFS: {e}")


def atomic_write(filepath, data):
    tmp = filepath + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, filepath)


def consumer_loop(topic, hdfs_path, dashboard_file, label):
    hdfs = get_hdfs_client()

    # Pastikan direktori HDFS ada
    try:
        hdfs.makedirs(hdfs_path)
    except Exception:
        pass

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=f"hdfs-writer-{label}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode()),
        consumer_timeout_ms=FLUSH_SECONDS * 1000,
    )

    buffer, live = [], []
    start = time.time()
    print(f"[consumer-hdfs] [{label}] Membaca dari topic '{topic}'...")

    while True:
        try:
            for msg in consumer:
                buffer.append(msg.value)
                live.append(msg.value)

                # Update dashboard file (20 terbaru)
                atomic_write(
                    f"{DASHBOARD_DIR}/{dashboard_file}",
                    live[-20:]
                )

                # Flush ke HDFS
                if len(buffer) >= FLUSH_COUNT or (time.time() - start) >= FLUSH_SECONDS:
                    simpan_ke_hdfs(hdfs, buffer, hdfs_path, label)
                    consumer.commit()
                    buffer = []
                    start  = time.time()

            # Timeout — flush sisa
            if buffer:
                simpan_ke_hdfs(hdfs, buffer, hdfs_path, label)
                consumer.commit()
                buffer = []
            start = time.time()

        except Exception as e:
            print(f"[consumer-hdfs] [{label}] Error: {e} — restart consumer 10s")
            time.sleep(10)


if __name__ == "__main__":
    print("[consumer-hdfs] Mulai, tunggu 40s agar HDFS & Kafka siap...")
    time.sleep(40)

    t1 = threading.Thread(
        target=consumer_loop,
        args=(TOPIC_API, "/data/github/api", "live_api.json", "api"),
        daemon=True
    )
    t2 = threading.Thread(
        target=consumer_loop,
        args=(TOPIC_RSS, "/data/github/rss", "live_rss.json", "rss"),
        daemon=True
    )
    t1.start()
    t2.start()
    print("[consumer-hdfs] 2 thread consumer berjalan.")
    t1.join()
    t2.join()
