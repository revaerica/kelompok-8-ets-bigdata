"""
Komponen 3 — Spark Structured Streaming
GitTrend | ETS Big Data | Kelompok 8
Dikerjakan: Revalina Erica Permatasari (5027241007)

Membaca stream dari Kafka topic 'github-api', menjalankan 3 analisis,
dan menyimpan hasilnya ke HDFS + dashboard/data/spark_results.json.

Micro-batch setiap 60 detik agar dashboard terupdate secara near-realtime.
"""
import os, json, time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HDFS_NAMENODE    = os.environ.get("HDFS_NAMENODE", "namenode:9000")
TOPIC_API        = "github-api"
DASHBOARD_OUT    = "/app/dashboard/data/spark_results.json"
HDFS_HASIL       = f"hdfs://{HDFS_NAMENODE}/data/github/hasil"
TRIGGER_INTERVAL = "60 seconds"

# Schema event dari producer
EVENT_SCHEMA = StructType([
    StructField("full_name",        StringType(), True),
    StructField("description",      StringType(), True),
    StructField("language",         StringType(), True),
    StructField("stargazers_count", LongType(),   True),
    StructField("forks_count",      LongType(),   True),
    StructField("html_url",         StringType(), True),
    StructField("sumber",           StringType(), True),
    StructField("timestamp",        StringType(), True),
    StructField("topics",           ArrayType(StringType()), True),
])


def buat_spark():
    """Inisialisasi SparkSession dengan koneksi ke Kafka dan HDFS."""
    return (
        SparkSession.builder
        .appName("GitTrend-Streaming")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-ckpt-local")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )


def atomic_write(filepath, data):
    tmp = filepath + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, filepath)


def run_batch_analysis(spark, df_batch):
    """
    Jalankan 3 analisis dari batch DataFrame terkini.
    df_batch: DataFrame kolom event (full_name, language, stargazers_count, description, dst.)
    """
    if df_batch.isEmpty():
        return None

    total_repo = df_batch.count()

    # ── Analisis 1: Distribusi Bahasa Pemrograman ──────────────────────────
    # DataFrame API: groupBy language → count + avg stars + total stars
    df_lang = (
        df_batch
        .filter(
            F.col("language").isNotNull()
            & (F.col("language") != "")
            & (F.col("language") != "Unknown")
        )
        .groupBy("language")
        .agg(
            F.count("*").alias("jumlah_repo"),
            F.round(F.avg("stargazers_count"), 0).alias("rata_rata_bintang"),
            F.sum("stargazers_count").alias("total_bintang"),
        )
        .orderBy(F.col("jumlah_repo").desc())
        .limit(15)
    )
    lang_list = [row.asDict() for row in df_lang.collect()]
    # Konversi numerik agar JSON-safe
    for r in lang_list:
        r["jumlah_repo"]       = int(r["jumlah_repo"])
        r["rata_rata_bintang"] = int(r["rata_rata_bintang"] or 0)
        r["total_bintang"]     = int(r["total_bintang"] or 0)

    # ── Analisis 2: Top 10 Repo Berdasarkan Bintang ────────────────────────
    # Spark SQL
    df_batch.createOrReplaceTempView("repos")
    df_top10 = spark.sql("""
        SELECT
            full_name,
            language,
            CAST(stargazers_count AS LONG) AS stargazers_count,
            SUBSTRING(COALESCE(description, '(no description)'), 1, 100) AS description_preview,
            html_url
        FROM repos
        WHERE stargazers_count IS NOT NULL
        ORDER BY CAST(stargazers_count AS LONG) DESC
        LIMIT 10
    """)
    # Dedup by full_name
    seen_repos, top10_list = set(), []
    for row in df_top10.collect():
        d = row.asDict()
        if d["full_name"] not in seen_repos:
            seen_repos.add(d["full_name"])
            d["stargazers_count"] = int(d["stargazers_count"] or 0)
            top10_list.append(d)

    # ── Analisis 3: Frekuensi Kata di Deskripsi ───────────────────────────
    # DataFrame API: explode → lowercase → filter → count
    STOPWORDS = {
        "the","and","for","with","that","this","from","your","you",
        "are","was","has","have","will","can","use","using","used",
        "its","not","but","also","all","one","any","more","into",
        "new","based","build","made","make","like","get","set",
        "tool","simple","easy","fast","full","free","open","source",
    }
    df_words = (
        df_batch
        .filter(F.col("description").isNotNull() & (F.col("description") != ""))
        .select(F.explode(F.split(F.col("description"), r"[\s\W]+")).alias("word"))
        .withColumn("word", F.lower(F.col("word")))
        .filter(F.col("word").rlike("^[a-z]{4,}"))
        .filter(~F.col("word").isin(list(STOPWORDS)))
        .groupBy("word")
        .count()
        .orderBy(F.col("count").desc())
        .limit(50)
    )
    word_list = [{"word": r["word"], "count": int(r["count"])} for r in df_words.collect()]

    return {
        "generated_at"          : datetime.now().isoformat(),
        "sumber_data"           : "kafka_stream",
        "total_repo_dianalisis" : total_repo,
        "language_distribution" : lang_list,
        "top10_repos"           : top10_list,
        "word_frequency"        : word_list,
    }


def process_batch(batch_df, batch_id):
    """Dipanggil setiap micro-batch oleh foreachBatch."""
    print(f"[spark] Batch #{batch_id} — {batch_df.count()} event masuk")

    if batch_df.isEmpty():
        print(f"[spark] Batch #{batch_id} kosong, skip.")
        return

    # Parse JSON dari value kolom Kafka
    df_parsed = (
        batch_df
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("full_name").isNotNull())
    )

    # Simpan raw event ke HDFS (Storage Layer)
    try:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        df_parsed.coalesce(1).write.mode("append").json(
            f"{HDFS_HASIL}/raw/batch_{ts}"
        )
        print(f"[spark] Raw batch disimpan ke HDFS: {HDFS_HASIL}/raw/batch_{ts}")
    except Exception as e:
        print(f"[spark] Gagal simpan raw ke HDFS: {e}")

    # Baca semua data historis dari HDFS untuk analisis kumulatif
    try:
        spark = batch_df.sparkSession
        df_all = spark.read.option("multiLine", True).json(f"{HDFS_HASIL}/raw/")
        print(f"[spark] Total historis dari HDFS: {df_all.count()} record")
        results = run_batch_analysis(spark, df_all)
    except Exception as e:
        print(f"[spark] Fallback ke batch saat ini saja: {e}")
        spark = batch_df.sparkSession
        results = run_batch_analysis(spark, df_parsed)

    if results:
        # Simpan ke dashboard JSON
        atomic_write(DASHBOARD_OUT, results)
        print(f"[spark] spark_results.json diupdate — {results['total_repo_dianalisis']} repo")

        # Simpan ringkasan hasil ke HDFS
        try:
            ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            hasil_json = json.dumps(results, ensure_ascii=False)
            spark = batch_df.sparkSession
            spark.sparkContext.parallelize([hasil_json]).coalesce(1).saveAsTextFile(
                f"{HDFS_HASIL}/summary/summary_{ts}"
            )
        except Exception as e:
            print(f"[spark] Gagal simpan summary ke HDFS: {e}")


def main():
    print("[spark] Menunggu Kafka & HDFS siap...")
    time.sleep(50)

    spark = buat_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[spark] SparkSession aktif — versi: {spark.version}")

    # Baca stream dari Kafka
    df_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_API)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Jalankan foreachBatch setiap TRIGGER_INTERVAL
    query = (
        df_stream.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", "/tmp/spark-ckpt-local/github-api")
        .start()
    )

    print(f"[spark] Streaming dimulai, micro-batch setiap {TRIGGER_INTERVAL}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
