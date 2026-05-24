# Cara Menjalankan Pipeline Lakehouse

## Prasyarat

- Docker Desktop berjalan
- Container ETS sudah aktif (`docker compose up -d`)
- Python package `delta-spark` tersedia di container Spark
- Menambahkan ini pada service `spark` di file `docker-compose-kafka.yml`
  ```
       volumes:
      - ./dashboard/data:/app/data
      - ./lakehouse:/app/lakehouse
      - ./lakehouse/lakehouse_data:/app/lakehouse/lakehouse_data
  ```

## Langkah Menjalankan

### 1. Pastikan container ETS aktif

```bash
docker compose -f docker-compose-hadoop.yml up -d
docker compose -f docker-compose-kafka.yml up -d
```

Tunggu ~2 menit hingga data terkumpul di HDFS.

### 2. Jalankan script secara berurutan

```bash
# Masuk ke container Spark
docker exec -it spark bash

# Pindah ke folder lakehouse
cd /app/lakehouse

#install
pip install delta-spark==3.1.0 importlib_metadata

# Jalankan Bronze
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 01_bronze.py

# Jalankan Silver (termasuk demo Time Travel)
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 02_silver.py

# Jalankan Gold
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 03_gold.py
```

### 3. Cek hasil

```bash
# Lihat struktur folder Delta yang dihasilkan
ls -la lakehouse_data/bronze/
ls -la lakehouse_data/silver/
ls -la lakehouse_data/gold/

# Lihat transaction log Bronze
cat lakehouse_data/bronze/github_api/_delta_log/00000000000000000000.json
```

## Alternatif: Jika HDFS tidak aktif

Jika container Hadoop tidak bisa diaktifkan, script Bronze bisa membaca
dari file JSON lokal. Edit baris berikut di `01_bronze.py`:

```python
# Ganti:
HDFS_API_PATH = f"hdfs://{HDFS_NAMENODE}/data/github/api/"
# Menjadi:
HDFS_API_PATH = "./sample_data/api/"
```

Lalu copy beberapa file JSON dari `dashboard/data/` ke `lakehouse/sample_data/api/`
sebagai data sample.

## Struktur Folder yang Dihasilkan

```
lakehouse_data/
├── bronze/
│   ├── github_api/
│   │   ├── _delta_log/      ← Transaction log
│   │   └── *.parquet        ← Data files
│   └── github_rss/
├── silver/
│   └── github/
│       ├── _delta_log/
│       └── *.parquet
└── gold/
    ├── language_dist/
    ├── top_repos/
    ├── star_velocity/
    └── emerging_topics/
```
