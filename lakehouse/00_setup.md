# Cara Menjalankan Pipeline Lakehouse

## Prasyarat

- Docker Desktop berjalan
- Container ETS sudah aktif (`docker compose up -d`)
- Tambahkan volume berikut pada service `spark` di `docker-compose-kafka.yml`:

```yaml
volumes:
  - ./dashboard/data:/app/data
  - ./lakehouse:/app/lakehouse
```

## Langkah Menjalankan

### 1. Jalankan container ETS

```bash
docker network create gittrend-net
docker compose -f docker-compose-hadoop.yml up -d
docker network connect gittrend-net namenode
docker network connect gittrend-net datanode
docker compose -f docker-compose-kafka.yml up --build -d
```

Tunggu ~2 menit hingga data terkumpul di HDFS.

### 2. Jalankan pipeline lakehouse

```bash
# Masuk ke container Spark
docker exec -it spark bash

# Pindah ke folder lakehouse
cd /app/lakehouse

# Install dependencies
pip install delta-spark==3.1.0 importlib_metadata

# Jalankan Bronze → Silver → Gold secara berurutan
python3 01_bronze.py
python3 02_silver.py
python3 03_gold.py
```

### 3. Akses dashboard Gold

Buka browser: **http://localhost:5001**

> **Catatan**: Data Bronze dan Silver tersimpan di HDFS (`hdfs://namenode:9000/app/lakehouse/lakehouse_data/`).
> Data Gold tersimpan di local filesystem (`file:///app/lakehouse/lakehouse_data/gold/`) agar bisa dibaca langsung oleh dashboard tanpa Spark.

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

## Troubleshooting

### `ModuleNotFoundError: No module named 'importlib_metadata'`
```bash
pip install delta-spark==3.1.0 importlib_metadata
```

### `/app/lakehouse` tidak ditemukan di container
Pastikan volume sudah ditambahkan di `docker-compose-kafka.yml` dan container di-recreate:
```bash
docker compose -f docker-compose-kafka.yml up -d --force-recreate spark
```

### `star_velocity` kosong
Jalankan `01_bronze.py` minimal dua kali dengan jeda beberapa menit, lalu jalankan ulang `03_gold.py`. Window Function `lag()` butuh minimal 2 observasi per repo di waktu berbeda.

### Dashboard Gold kosong / Gold Tables ❌
Data Gold perlu di-copy dari container spark ke host setelah pipeline selesai:
```bash
docker cp spark:/app/lakehouse/lakehouse_data ./lakehouse/
docker compose -f docker-compose-kafka.yml up -d --force-recreate lakehouse-dashboard
```
