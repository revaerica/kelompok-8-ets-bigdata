# Cara Menjalankan Pipeline Lakehouse

## Prasyarat

- Docker Desktop berjalan
- File `docker-compose-kafka.yml` sudah menggunakan versi terbaru
  (sudah include service `lakehouse-pipeline` untuk otomasi Bronze→Silver→Gold)

## Langkah Menjalankan

### 1. Jalankan container Hadoop terlebih dahulu

```bash
docker network create gittrend-net
docker compose -f docker-compose-hadoop.yml up -d
docker network connect gittrend-net namenode
docker network connect gittrend-net datanode
```

Tunggu ~30 detik hingga HDFS siap. Cek di `http://localhost:9870` — pastikan **Live Nodes: 1** dan Safemode off.

### 2. Jalankan semua service (Kafka + Pipeline + Dashboard)

```bash
docker compose -f docker-compose-kafka.yml up --build -d
```

Selesai! Tidak ada langkah manual lagi. Semua service termasuk pipeline lakehouse akan berjalan otomatis.

### 3. Akses dashboard

| Dashboard | URL |
|-----------|-----|
| Dashboard ETS (Spark Streaming) | http://localhost:5000 |
| Dashboard Gold (Lakehouse) | http://localhost:5001 |

---

## Yang Terjadi Secara Otomatis

Setelah `docker compose up`, ini yang berjalan sendiri tanpa intervensi manual:

```
producer-api    → kirim data GitHub API ke Kafka (setiap 30 menit)
producer-rss    → kirim berita RSS ke Kafka (setiap 10 menit)
consumer-hdfs   → simpan data Kafka ke HDFS
spark           → analisis Spark Streaming (setiap 60 detik)
lakehouse-pipeline → Bronze → Silver → Gold (setiap 10 menit)
dashboard       → tampilkan hasil Spark Streaming (port 5000)
lakehouse-dashboard → tampilkan Gold Layer (port 5001)
```

Pipeline lakehouse berjalan otomatis dengan jadwal:
- **Menit 0–1**: tunggu HDFS dan data siap
- **Menit 1**: Bronze → Silver → Gold pertama kali dijalankan
- **Setiap 10 menit**: pipeline dijalankan ulang untuk refresh data terbaru

## Memantau Pipeline

Cek log pipeline berjalan otomatis:

```bash
docker logs -f lakehouse-pipeline
```

Contoh output yang akan muncul:

```
[pipeline] Menunggu 60 detik agar HDFS dan data siap...
[pipeline] Mulai run Bronze...
[bronze] SparkSession aktif
[bronze] API record: 2148
[bronze] API disimpan ke Delta
[pipeline] Mulai run Silver...
[silver] Bronze record: 2148
[silver] Setelah dedup: 6946
[pipeline] Mulai run Gold...
[gold] language_dist disimpan: 32 bahasa
[gold] top_repos disimpan
[gold] star_velocity disimpan: 1159 repo
[gold] emerging_topics disimpan: 1402 kata
[gold] api_rss_join disimpan
[pipeline] Selesai! Tunggu 10 menit...
```

Cek semua container aktif:

```bash
docker ps | grep lakehouse
```

---

## Output yang Dihasilkan

```
Bronze:
  API  : 2148 record → lakehouse_data/bronze/github_api
  RSS  : 24 record   → lakehouse_data/bronze/github_rss

Silver:
  11639 Bronze → 6946 Silver (4693 duplikat dihapus)
  → lakehouse_data/silver/github
  + Demo Time Travel (9 versi)
  + Demo Schema Evolution (kolom repo_tier ditambahkan)

Gold:
  language_dist  : 32 bahasa   → lakehouse_data/gold/language_dist
  top_repos      : 10 repo     → lakehouse_data/gold/top_repos
  star_velocity  : 1159 repo   → lakehouse_data/gold/star_velocity
  emerging_topics: 1402 kata   → lakehouse_data/gold/emerging_topics
  api_rss_join   : 1 pasangan  → lakehouse_data/gold/api_rss_join
```

---

## Struktur Folder yang Dihasilkan

```
lakehouse_data/
├── bronze/
│   ├── github_api/
│   │   ├── _delta_log/      ← Transaction log (ACID)
│   │   └── *.snappy.parquet ← Data files
│   └── github_rss/
│       ├── _delta_log/
│       └── *.snappy.parquet
├── silver/
│   └── github/
│       ├── _delta_log/      ← 9 versi tercatat
│       └── *.snappy.parquet
└── gold/
    ├── language_dist/
    ├── top_repos/
    ├── star_velocity/
    ├── emerging_topics/
    └── api_rss_join/
```

---

## Troubleshooting

### Pipeline tidak jalan / container `lakehouse-pipeline` langsung exit

Cek lognya:
```bash
docker logs lakehouse-pipeline
```

Kemudian restart:
```bash
docker compose -f docker-compose-kafka.yml up -d --force-recreate lakehouse-pipeline
```

### `ModuleNotFoundError: No module named 'importlib_metadata'`

Dependency diinstall otomatis saat container start. Kalau masih error, rebuild containernya:
```bash
docker compose -f docker-compose-kafka.yml build lakehouse-pipeline
docker compose -f docker-compose-kafka.yml up -d lakehouse-pipeline
```

### `/app/lakehouse` tidak ditemukan di container

Pastikan volume sudah benar di `docker-compose-kafka.yml`:
```yaml
volumes:
  - ./lakehouse:/app/lakehouse
```

Kemudian recreate:
```bash
docker compose -f docker-compose-kafka.yml up -d --force-recreate lakehouse-pipeline
```

### `star_velocity` kosong

Normal terjadi saat pertama kali jalan karena `lag()` butuh minimal 2 observasi per repo di waktu berbeda. Tunggu pipeline berjalan minimal 2 kali (sekitar 10 menit), data star_velocity akan terisi otomatis.

### Dashboard Gold kosong / Gold Tables ❌

Tunggu pipeline selesai berjalan minimal sekali (cek log `lakehouse-pipeline`). Dashboard akan otomatis menampilkan data setelah Gold layer selesai ditulis.

---

## Stop dan Cleanup

```bash
# Stop semua container
docker compose -f docker-compose-kafka.yml down
docker compose -f docker-compose-hadoop.yml down

# Stop + hapus volume (data HDFS hilang)
docker compose -f docker-compose-kafka.yml down -v
docker compose -f docker-compose-hadoop.yml down -v
```
