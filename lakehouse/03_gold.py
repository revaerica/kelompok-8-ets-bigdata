"""
Lakehouse — Gold Layer
GitTrend | Tugas Week 12 | Kelompok 8

Membuat 4 tabel Gold dari Silver layer:

Tabel Reproduksi ETS:
1. gold/language_dist  — Distribusi bahasa pemrograman (repro Analisis 1 ETS)
2. gold/top_repos      — Top 10 repo berdasarkan bintang (repro Analisis 2 ETS)

Tabel Enhanced (tidak bisa dibuat di ETS karena butuh timestamp yang sudah di-cast):
3. gold/star_velocity  — Star velocity per repo: repo yang paling cepat viral
4. gold/emerging_topics — Kata kunci deskripsi yang muncul di 3 jam terakhir
                          tapi tidak ada di periode sebelumnya
"""
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, max as spark_max,
    round as spark_round, desc, asc,
    explode, split, lower, regexp_replace,
    lag, hour, lit, current_timestamp
)
from delta import configure_spark_with_delta_pip

HDFS_NAMENODE   = os.environ.get("HDFS_NAMENODE", "namenode:9000")
SILVER_GITHUB   = "./lakehouse_data/silver/github"
GOLD_LANG       = "./lakehouse_data/gold/language_dist"
GOLD_TOP        = "./lakehouse_data/gold/top_repos"
GOLD_VELOCITY   = "./lakehouse_data/gold/star_velocity"
GOLD_EMERGING   = "./lakehouse_data/gold/emerging_topics"

STOPWORDS = {
    "the","and","for","with","that","this","from","your","you",
    "are","was","has","have","will","can","use","using","used",
    "its","not","but","also","all","one","any","more","into",
    "new","based","build","made","make","like","get","set",
    "tool","simple","easy","fast","full","free","open","source",
}


def buat_spark():
    builder = (
        SparkSession.builder
        .appName("GitTrend-Gold")
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
    print("[gold] SparkSession aktif")

    silver = spark.read.format("delta").load(SILVER_GITHUB)
    total = silver.count()
    print(f"[gold] Silver record: {total}")

    # ════════════════════════════════════════════════════════════════════════
    # TABEL 1: language_dist — Repro Analisis 1 ETS
    # Keunggulan vs ETS: timestamp sudah di-cast → hasil lebih akurat,
    # tidak ada duplikat → jumlah repo per bahasa lebih benar
    # ════════════════════════════════════════════════════════════════════════
    print("\n[gold] Membuat language_dist...")
    lang_df = (
        silver
        .filter(
            col("language").isNotNull()
            & (col("language") != "")
            & (col("language") != "Unknown")
        )
        .groupBy("language")
        .agg(
            count("*").alias("jumlah_repo"),
            spark_round(avg("stargazers_count"), 0).alias("rata_rata_bintang"),
            spark_sum("stargazers_count").alias("total_bintang"),
        )
        .orderBy(desc("jumlah_repo"))
    )
    lang_df.write.format("delta").mode("overwrite").save(GOLD_LANG)
    print(f"[gold] language_dist disimpan: {lang_df.count()} bahasa")
    lang_df.show(15)

    # ════════════════════════════════════════════════════════════════════════
    # TABEL 2: top_repos — Repro Analisis 2 ETS (Spark SQL)
    # Keunggulan vs ETS: tidak ada duplikat repo → ranking lebih akurat
    # ════════════════════════════════════════════════════════════════════════
    print("\n[gold] Membuat top_repos...")
    silver.createOrReplaceTempView("silver_repos")
    top_df = spark.sql("""
        SELECT
            full_name,
            language,
            CAST(stargazers_count AS LONG)                          AS stargazers_count,
            forks_count,
            SUBSTRING(COALESCE(description, '(no description)'), 1, 120) AS description_preview,
            html_url
        FROM silver_repos
        WHERE stargazers_count IS NOT NULL
        ORDER BY CAST(stargazers_count AS LONG) DESC
        LIMIT 10
    """)
    top_df.write.format("delta").mode("overwrite").save(GOLD_TOP)
    print(f"[gold] top_repos disimpan")
    top_df.show(truncate=60)

    # ════════════════════════════════════════════════════════════════════════
    # TABEL 3: star_velocity — Enhanced (Window Function)
    # Deteksi repo yang paling cepat mendapat bintang baru.
    # Tidak bisa dibuat di ETS karena timestamp belum di-cast ke TimestampType
    # sehingga tidak bisa pakai Window.partitionBy().orderBy("timestamp")
    # ════════════════════════════════════════════════════════════════════════
    print("\n[gold] Membuat star_velocity...")
    window_spec = Window.partitionBy("full_name").orderBy("timestamp")

    velocity_df = (
        silver
        .filter(col("timestamp").isNotNull())
        .withColumn("prev_stars", lag("stargazers_count", 1).over(window_spec))
        .withColumn(
            "star_gain",
            col("stargazers_count") - col("prev_stars")
        )
        .filter(col("star_gain").isNotNull())
        .groupBy("full_name", "language")
        .agg(
            spark_sum("star_gain").alias("total_star_gain"),
            spark_max("stargazers_count").alias("current_stars"),
            count("*").alias("jumlah_observasi")
        )
        .orderBy(desc("total_star_gain"))
    )
    velocity_df.write.format("delta").mode("overwrite").save(GOLD_VELOCITY)
    print(f"[gold] star_velocity disimpan: {velocity_df.count()} repo")
    print("Top 10 repo yang paling viral (star gain terbesar):")
    velocity_df.show(10, truncate=50)

    # ════════════════════════════════════════════════════════════════════════
    # TABEL 4: emerging_topics — Enhanced (Cross-time analysis)
    # Kata kunci deskripsi yang muncul di 3 jam terakhir data.
    # Tidak bisa dibuat di ETS karena jam belum diekstrak dari timestamp.
    # ════════════════════════════════════════════════════════════════════════
    print("\n[gold] Membuat emerging_topics...")

    # Ambil jam max sebagai "jam terkini"
    from pyspark.sql.functions import max as spark_max_fn
    max_jam = silver.filter(col("jam").isNotNull()).agg(
        spark_max_fn("jam")
    ).collect()[0][0]

    if max_jam is not None:
        jam_recent  = max_jam
        jam_cutoff  = (max_jam - 3) % 24  # 3 jam sebelumnya

        words_recent = (
            silver
            .filter(
                col("description").isNotNull()
                & col("jam").isNotNull()
                & (col("jam") >= jam_cutoff)
            )
            .select(
                explode(
                    split(
                        lower(regexp_replace(col("description"), r"[^a-zA-Z\s]", "")),
                        r"\s+"
                    )
                ).alias("word")
            )
            .filter(col("word").rlike("^[a-z]{4,}"))
            .filter(~col("word").isin(list(STOPWORDS)))
            .groupBy("word")
            .agg(count("*").alias("count_recent"))
        )

        words_old = (
            silver
            .filter(
                col("description").isNotNull()
                & col("jam").isNotNull()
                & (col("jam") < jam_cutoff)
            )
            .select(
                explode(
                    split(
                        lower(regexp_replace(col("description"), r"[^a-zA-Z\s]", "")),
                        r"\s+"
                    )
                ).alias("word")
            )
            .filter(col("word").rlike("^[a-z]{4,}"))
            .filter(~col("word").isin(list(STOPWORDS)))
            .groupBy("word")
            .agg(count("*").alias("count_old"))
        )

        # Kata yang ada di recent tapi tidak ada di old = emerging
        emerging_df = (
            words_recent
            .join(words_old, "word", "left_anti")
            .orderBy(desc("count_recent"))
            .withColumn("_computed_at", current_timestamp())
        )

        emerging_df.write.format("delta").mode("overwrite").save(GOLD_EMERGING)
        print(f"[gold] emerging_topics disimpan: {emerging_df.count()} kata baru")
        print(f"Kata kunci emerging (jam {jam_cutoff}–{jam_recent}):")
        emerging_df.show(20, truncate=False)
    else:
        print("[gold] Tidak cukup data temporal untuk emerging_topics, skip.")

    # ════════════════════════════════════════════════════════════════════════
    # RINGKASAN
    # ════════════════════════════════════════════════════════════════════════
    print("\n=== GOLD LAYER — Ringkasan ===")
    print(f"language_dist  : {spark.read.format('delta').load(GOLD_LANG).count()} bahasa")
    print(f"top_repos      : {spark.read.format('delta').load(GOLD_TOP).count()} repo")
    print(f"star_velocity  : {spark.read.format('delta').load(GOLD_VELOCITY).count()} repo")
    try:
        print(f"emerging_topics: {spark.read.format('delta').load(GOLD_EMERGING).count()} kata")
    except Exception:
        print("emerging_topics: (tidak dibuat, data tidak cukup)")
    print("\nSemua tabel Gold tersimpan di format Delta Lake ✅")


if __name__ == "__main__":
    main()
