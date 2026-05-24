"""
Lakehouse — Bronze Layer
GitTrend | Tugas Week 12 | Kelompok 8

Membaca file JSON mentah dari HDFS (hasil consumer ETS) dan menyimpannya
ke format Delta Lake sebagai Bronze layer.
Ditambahkan metadata: _ingested_at dan _source.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta import configure_spark_with_delta_pip

HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "namenode:9000")
HDFS_API_PATH = f"hdfs://{HDFS_NAMENODE}/data/github/api/"
HDFS_RSS_PATH = f"hdfs://{HDFS_NAMENODE}/data/github/rss/"
BRONZE_API    = "./lakehouse_data/bronze/github_api"
BRONZE_RSS    = "./lakehouse_data/bronze/github_rss"


def buat_spark():
    builder = (
        SparkSession.builder
        .appName("GitTrend-Bronze")
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
    print("[bronze] SparkSession aktif")

    # ── Ingest API ───────────────────────────────────────────────────────────
    print(f"[bronze] Membaca API dari: {HDFS_API_PATH}")
    api_df = (
        spark.read
        .option("multiLine", True)
        .json(HDFS_API_PATH)
    )
    print(f"[bronze] API record: {api_df.count()}")

    bronze_api = (
        api_df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("api"))
    )
    bronze_api.write.format("delta").mode("append").save(BRONZE_API)
    print(f"[bronze] API disimpan ke Delta: {BRONZE_API}")

    # ── Ingest RSS ───────────────────────────────────────────────────────────
    print(f"[bronze] Membaca RSS dari: {HDFS_RSS_PATH}")
    rss_df = (
        spark.read
        .option("multiLine", True)
        .json(HDFS_RSS_PATH)
    )
    print(f"[bronze] RSS record: {rss_df.count()}")

    bronze_rss = (
        rss_df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("rss"))
    )
    bronze_rss.write.format("delta").mode("append").save(BRONZE_RSS)
    print(f"[bronze] RSS disimpan ke Delta: {BRONZE_RSS}")

    # ── Ringkasan ────────────────────────────────────────────────────────────
    print("\n=== BRONZE LAYER — Ringkasan ===")
    print(f"API  : {bronze_api.count()} record")
    print(f"RSS  : {bronze_rss.count()} record")
    print("Kolom metadata ditambahkan: _ingested_at, _source")
    print("\nSchema Bronze API:")
    spark.read.format("delta").load(BRONZE_API).printSchema()


if __name__ == "__main__":
    main()
