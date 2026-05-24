"""
Lakehouse — Silver Layer
GitTrend | Tugas Week 12 | Kelompok 8

Membaca Bronze Delta layer dan melakukan cleaning:
1. Hapus duplikat berdasarkan full_name
2. Filter baris yang tidak punya full_name (data invalid)
3. Cast timestamp dari String ke TimestampType
4. Isi null pada kolom language dengan "Unknown"
5. Filter stargazers_count negatif (data korup)
6. Ekstrak kolom jam dari timestamp (untuk analisis temporal)
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, current_timestamp
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "namenode:9000")
BRONZE_API    = "./lakehouse_data/bronze/github_api"
SILVER_GITHUB = "./lakehouse_data/silver/github"


def buat_spark():
    builder = (
        SparkSession.builder
        .appName("GitTrend-Silver")
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}")
    )
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
    ).getOrCreate()


def main():
    spark = buat_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("[silver] SparkSession aktif")

    # ── Baca Bronze ──────────────────────────────────────────────────────────
    bronze_df = spark.read.format("delta").load(BRONZE_API)
    total_bronze = bronze_df.count()
    print(f"[silver] Bronze record: {total_bronze}")

    # ── Transformasi 1: Hapus duplikat berdasarkan full_name ─────────────────
    # Alasan: producer bisa mengirim repo yang sama beberapa kali
    # (seed Kaggle + live API bisa overlap)
    after_dedup = bronze_df.dropDuplicates(["full_name"])
    print(f"[silver] Setelah dedup           : {after_dedup.count()} "
          f"(hilang {total_bronze - after_dedup.count()} duplikat)")

    # ── Transformasi 2: Filter baris tanpa full_name ─────────────────────────
    # Alasan: full_name adalah identifier utama repo, tanpanya data tidak berguna
    after_null_filter = after_dedup.filter(col("full_name").isNotNull())
    print(f"[silver] Setelah filter null name : {after_null_filter.count()}")

    # ── Transformasi 3: Filter stargazers_count negatif ──────────────────────
    # Alasan: nilai negatif adalah data korup dari sumber
    after_stars_filter = after_null_filter.filter(
        col("stargazers_count").isNull() | (col("stargazers_count") >= 0)
    )
    print(f"[silver] Setelah filter stars < 0 : {after_stars_filter.count()}")

    # ── Transformasi 4: Cast timestamp String → TimestampType ────────────────
    # Alasan: agar bisa digunakan untuk analisis temporal (Window Function)
    # ── Transformasi 5: Isi null language dengan "Unknown" ───────────────────
    # Alasan: kolom language sering null untuk repo multi-bahasa
    # ── Transformasi 6: Ekstrak kolom jam dari timestamp ─────────────────────
    # Alasan: memudahkan analisis per jam tanpa harus parse ulang
    silver_df = (
        after_stars_filter
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .fillna({"language": "Unknown"})
        .withColumn("jam", hour(col("timestamp")))
        .withColumn("_processed_at", current_timestamp())
    )

    total_silver = silver_df.count()
    print(f"\n[silver] Total Silver record     : {total_silver}")
    print(f"[silver] Total hilang dari Bronze: {total_bronze - total_silver}")

    # ── Simpan ke Silver Delta ───────────────────────────────────────────────
    silver_df.write.format("delta").mode("overwrite").save(SILVER_GITHUB)
    print(f"[silver] Disimpan ke Delta: {SILVER_GITHUB}")

    print("\nSchema Silver:")
    spark.read.format("delta").load(SILVER_GITHUB).printSchema()

    # ── Demo Time Travel ─────────────────────────────────────────────────────
    print("\n=== DEMO TIME TRAVEL ===")

    deltaTable = DeltaTable.forPath(spark, SILVER_GITHUB)

    # Lihat history
    print("History tabel Silver:")
    deltaTable.history().select("version", "timestamp", "operation").show()

    # Lakukan update: repo yang language-nya masih null → "Unknown"
    print("Melakukan UPDATE: language null → 'Unknown'...")
    deltaTable.update(
        condition="language IS NULL",
        set={"language": "'Unknown'"}
    )

    # Bandingkan versi sekarang vs versi 0
    print("\n=== Distribusi language SEKARANG ===")
    (spark.read.format("delta").load(SILVER_GITHUB)
     .groupBy("language").count()
     .orderBy("count", ascending=False)
     .show(10))

    print("\n=== Distribusi language VERSI 0 (sebelum update) ===")
    (spark.read.format("delta")
     .option("versionAsOf", 0)
     .load(SILVER_GITHUB)
     .groupBy("language").count()
     .orderBy("count", ascending=False)
     .show(10))

    # History terbaru
    print("\nHistory tabel Silver setelah update:")
    deltaTable.history().select("version", "timestamp", "operation").show()


if __name__ == "__main__":
    main()
