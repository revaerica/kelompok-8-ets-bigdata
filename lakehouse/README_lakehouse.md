# README Lakehouse — GitTrend Data Lakehouse
**Kelompok 8 | Tugas Week 12 | Big Data**

---

## Arsitektur: Sebelum vs Sesudah

### Sebelum (ETS)
```
[GitHub API] → Kafka → consumer_to_hdfs → HDFS (JSON mentah)
[RSS Feed]   → Kafka → consumer_to_hdfs → HDFS (JSON mentah)
                                               ↓
                                    Spark Structured Streaming
                                    (baca JSON, analisis, simpan JSON)
                                               ↓
                                    spark_results.json → Dashboard Flask
```

**Masalah arsitektur lama:**
- Data di HDFS adalah JSON mentah tanpa schema enforcement
- Tidak ada ACID — jika proses gagal di tengah jalan, data bisa korup
- Tidak ada versioning — tidak bisa tahu data berubah kapan dan oleh siapa
- Duplikat repo tidak terdeteksi → analisis tidak akurat
- Timestamp masih String → tidak bisa pakai Window Function

### Sesudah (Lakehouse)
```
[GitHub API] → Kafka → consumer_to_hdfs → HDFS (JSON mentah)
[RSS Feed]   → Kafka → consumer_to_hdfs → HDFS (JSON mentah)
                                               ↓
                                    🥉 BRONZE (Delta Lake)
                                    Raw data + metadata _ingested_at, _source
                                               ↓
                                    🥈 SILVER (Delta Lake)
                                    Cleaned: dedup, cast types, filter invalid
                                               ↓
                                    🥇 GOLD (Delta Lake)
                                    language_dist | top_repos
                                    star_velocity | emerging_topics
                                               ↓
                                    Dashboard Flask (baca dari Gold)
```

---

## Penjelasan Transformasi Silver

### Transformasi 1: Hapus Duplikat (`dropDuplicates(["full_name"])`)
**Mengapa:** Producer mengirim data dari dua sumber — seed dataset Kaggle (1049 repo) dan live GitHub API. Repo yang sama bisa muncul di kedua sumber, sehingga tanpa deduplication, satu repo bisa terhitung dua kali dalam analisis distribusi bahasa atau leaderboard.

### Transformasi 2: Filter `full_name` null
**Mengapa:** `full_name` adalah identifier utama setiap repo (format: `owner/repo-name`). Tanpa `full_name`, data tidak bisa diidentifikasi, tidak bisa di-join, dan tidak berguna untuk analisis apapun.

### Transformasi 3: Filter `stargazers_count` negatif
**Mengapa:** Nilai bintang tidak mungkin negatif. Jika ada, itu adalah data korup dari sumber. Membiarkannya akan merusak hasil analisis rata-rata dan ranking.

### Transformasi 4: Cast `timestamp` String → `TimestampType`
**Mengapa:** Di ETS, timestamp disimpan sebagai String sehingga tidak bisa digunakan untuk operasi temporal seperti Window Function (`lag`, `lead`), filter per jam, atau agregasi per periode waktu. Dengan casting ke `TimestampType`, analisis temporal menjadi mungkin.

### Transformasi 5: Fill null `language` → "Unknown"
**Mengapa:** Banyak repo tidak mencantumkan bahasa utama. Jika dibiarkan null, repo-repo ini tidak akan masuk ke analisis distribusi bahasa. Dengan mengisinya sebagai "Unknown", kita bisa tetap melacak proporsi repo yang tidak terklasifikasi.

### Transformasi 6: Ekstrak kolom `jam` dari timestamp
**Mengapa:** Memudahkan analisis per jam tanpa harus parsing ulang timestamp setiap kali query. Digunakan untuk `emerging_topics` — deteksi kata kunci yang baru muncul dalam 3 jam terakhir.

| Transformasi | Baris Hilang | Alasan |
|---|---|---|
| Hapus duplikat | ~X baris | Repo yang dikirim duplikat dari seed + live API |
| Filter full_name null | ~Y baris | Event tidak lengkap dari Kafka |
| Filter stars negatif | ~Z baris | Data korup dari sumber |

---

## Tabel Gold — Perbandingan vs Analisis ETS

### Gold 1: `language_dist` (Repro Analisis 1 ETS)

| Aspek | ETS (Spark Streaming) | Gold Layer |
|---|---|---|
| Sumber | JSON mentah HDFS | Silver Delta (sudah bersih) |
| Duplikat | Ada → jumlah tidak akurat | Sudah dihapus → akurat |
| Null handling | Filter saat query | Sudah dihandle di Silver |
| Hasil | Bisa bias karena duplikat | Lebih akurat |

### Gold 2: `top_repos` (Repro Analisis 2 ETS)

| Aspek | ETS (Spark Streaming) | Gold Layer |
|---|---|---|
| Ranking | Bisa ada repo duplikat di top 10 | Dedup sudah dilakukan → ranking bersih |
| Format | JSON flat | Delta format dengan schema ketat |

### Gold 3: `star_velocity` (🆕 Enhanced — Window Function)

Tidak bisa dibuat di ETS karena:
- Timestamp masih String → tidak bisa `orderBy("timestamp")` dalam Window Function
- Data duplikat → `lag()` akan menghasilkan nilai yang salah

Cara kerja: untuk setiap repo, hitung selisih `stargazers_count` antar observasi menggunakan `lag()`. Total selisih = estimasi berapa bintang yang diterima selama periode pengumpulan data.

**Contoh output:**
```
freeCodeCamp/freeCodeCamp | TypeScript | total_star_gain: 150 | current: 434k
flutter/flutter           | Dart       | total_star_gain: 89  | current: 174k
```

### Gold 4: `emerging_topics` (🆕 Enhanced — Cross-time analysis)

Tidak bisa dibuat di ETS karena kolom `jam` belum diekstrak dari timestamp.

Cara kerja: bandingkan kata kunci deskripsi yang muncul di 3 jam terakhir dengan kata yang ada sebelumnya. Kata yang **hanya ada di 3 jam terakhir** = emerging topic.

---

## Demo Time Travel

Script `02_silver.py` mendemonstrasikan Time Travel:

1. Tulis Silver layer (versi 0)
2. Lakukan UPDATE: language null → "Unknown"
3. Bandingkan distribusi language versi 0 vs versi terbaru

```python
# Baca data SEBELUM update
spark.read.format("delta").option("versionAsOf", 0).load(silver_path)

# Baca data SESUDAH update
spark.read.format("delta").load(silver_path)
```

**Kegunaan nyata:** Jika tim data science sudah melatih model menggunakan Silver versi 0, mereka bisa mereproduksi hasil yang sama 6 bulan kemudian dengan `versionAsOf=0`.

---

## Refleksi: Keuntungan Delta Lake vs HDFS/JSON

| Aspek | HDFS JSON (ETS) | Delta Lake (Tugas) |
|---|---|---|
| **ACID** | ❌ Tidak ada — jika proses gagal, data bisa setengah-setengah | ✅ Atomik — commit berhasil semua atau tidak sama sekali |
| **Versioning** | ❌ Tidak bisa lihat data versi lama | ✅ Time Travel ke versi manapun |
| **Schema** | ❌ Tidak ada enforcement — kolom bisa berbeda tiap file | ✅ Schema konsisten, perubahan terdokumentasi |
| **Update/Delete** | ❌ Harus tulis ulang seluruh file | ✅ MERGE INTO, UPDATE, DELETE efisien |
| **Audit Trail** | ❌ Tidak tahu siapa ubah apa kapan | ✅ `_delta_log` mencatat semua operasi |
| **Query Performance** | ❌ Baca semua file JSON satu per satu | ✅ Predicate pushdown, column pruning via Parquet |
| **ML Reprodusibility** | ❌ Tidak bisa memastikan data yang sama | ✅ `versionAsOf` menjamin data identik |

**Kesimpulan:** Delta Lake memberikan jaminan kualitas data tingkat production di atas infrastruktur storage murah (HDFS/S3). Untuk GitTrend, keuntungan paling nyata adalah kemampuan mendeteksi duplikat secara terstruktur dan Time Travel yang memungkinkan audit data kapanpun dibutuhkan.

---

## Cara Menjalankan

Lihat `00_setup.md` untuk instruksi lengkap.

```bash
docker exec -it spark bash
cd /app/lakehouse
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 01_bronze.py
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 02_silver.py
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 03_gold.py
```
