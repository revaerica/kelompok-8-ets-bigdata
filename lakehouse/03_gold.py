"""
Lakehouse — Gold Layer
GitTrend | Tugas Week 12 | Kelompok 8

Membuat 5 tabel Gold dari Silver layer:

Tabel Reproduksi ETS:
1. gold/language_dist   — Distribusi bahasa pemrograman (repro Analisis 1 ETS)
2. gold/top_repos       — Top 10 repo berdasarkan bintang (repro Analisis 2 ETS)

Tabel Enhanced:
3. gold/star_velocity   — Star velocity per repo (Window Function)
4. gold/emerging_topics — Kata kunci deskripsi 3 jam terakhir (Cross-time analysis)

Bonus Cross-source Join:
5. gold/api_rss_join    — Join Silver API + Silver RSS: repo yang muncul
                          bersamaan di GitHub trending DAN di berita teknologi
"""
import os
import shutil
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, max as spark_max,
    round as spark_round, desc,
    explode, split, lower, regexp_replace,
    lag, lit, current_timestamp, trim, row_number, concat, expr
)
from delta import configure_spark_with_delta_pip

HDFS_NAMENODE   = os.environ.get("HDFS_NAMENODE", "namenode:9000")

# ── Read paths (dari HDFS, karena Spark Bronze/Silver menulis ke HDFS) ────
SILVER_GITHUB   = "/app/lakehouse/lakehouse_data/silver/github"
BRONZE_RSS      = "/app/lakehouse/lakehouse_data/bronze/github_rss"

# ── Write paths (ke LOCAL filesystem agar dashboard bisa baca via volume) ──
# Dashboard container mount: ./lakehouse/lakehouse_data → /app/lakehouse/lakehouse_data
# Prefix file:/// memastikan Spark menulis ke local, bukan ke HDFS
GOLD_BASE       = "file:///app/lakehouse/lakehouse_data/gold"
GOLD_LANG       = f"{GOLD_BASE}/language_dist"
GOLD_TOP        = f"{GOLD_BASE}/top_repos"
GOLD_VELOCITY   = f"{GOLD_BASE}/star_velocity"
GOLD_EMERGING   = f"{GOLD_BASE}/emerging_topics"
GOLD_JOIN       = f"{GOLD_BASE}/api_rss_join"

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


def hapus_dir_lokal(path_uri):
    """
    Menghapus direktori lokal sebelum menulis Delta Table.
    Ini penting karena dashboard Flask membaca data via glob (*.parquet),
    sehingga kita harus membersihkan parquet versi historis agar tidak terjadi
    schema mismatch dengan file-file lama.
    """
    local_path = path_uri.replace("file://", "")
    if os.path.exists(local_path):
        try:
            shutil.rmtree(local_path)
            print(f"[gold] Membersihkan direktori lama: {local_path}")
        except Exception as e:
            print(f"[gold] Gagal membersihkan {local_path}: {e}")


def main():
    spark = buat_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("[gold] SparkSession aktif")

    silver = spark.read.format("delta").load(SILVER_GITHUB)
    total = silver.count()
    print(f"[gold] Silver record (total riwayat): {total}")

    # ── Deduplikasi data Silver untuk non-temporal analysis ──────────────────
    # Karena Silver menyimpan riwayat (dedup berdasarkan full_name + _ingested_at),
    # kita perlu mengambil record terbaru per repo untuk language_dist, top_repos,
    # emerging_topics, dan api_rss_join agar tidak terjadi duplikasi data.
    window_latest = Window.partitionBy("full_name").orderBy(desc("_ingested_at"), desc("timestamp"))
    silver_latest = (
        silver
        .withColumn("row_num", row_number().over(window_latest))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    print(f"[gold] Silver record (unik/terbaru) : {silver_latest.count()}")

    # ── Bersihkan direktori Gold lama ────────────────────────────────────────
    # Karena dashboard Flask membaca raw parquet via glob (*.parquet), kita harus
    # menghapus folder lama secara fisik sebelum Spark menulis ulang agar tidak ada
    # file parquet lama dengan skema/data usang yang tertinggal dan memicu error.
    hapus_dir_lokal(GOLD_LANG)
    hapus_dir_lokal(GOLD_TOP)
    hapus_dir_lokal(GOLD_VELOCITY)
    hapus_dir_lokal(GOLD_EMERGING)
    hapus_dir_lokal(GOLD_JOIN)

    # ════════════════════════════════════════════════════════════════════════
    # TABEL 1: language_dist — Repro Analisis 1 ETS
    # ════════════════════════════════════════════════════════════════════════
    print("\n[gold] Membuat language_dist...")
    lang_df = (
        silver_latest
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
    # ════════════════════════════════════════════════════════════════════════
    print("\n[gold] Membuat top_repos...")
    silver_latest.createOrReplaceTempView("silver_repos")
    top_df = spark.sql("""
        SELECT
            full_name,
            language,
            CAST(stargazers_count AS LONG)                               AS stargazers_count,
            forks_count,
            SUBSTRING(COALESCE(description, '(no description)'), 1, 120) AS description_preview,
            html_url
        FROM silver_repos
        WHERE stargazers_count IS NOT NULL
        ORDER BY CAST(stargazers_count AS LONG) DESC
        LIMIT 10
    """)
    top_df.write.format("delta").mode("overwrite").save(GOLD_TOP)
    print("[gold] top_repos disimpan")
    top_df.show(truncate=60)

    # ════════════════════════════════════════════════════════════════════════
    # TABEL 3: star_velocity — Enhanced (Window Function)
    # ════════════════════════════════════════════════════════════════════════
    print("\n[gold] Membuat star_velocity...")
    window_spec = Window.partitionBy("full_name").orderBy("timestamp")

    velocity_df = (
        silver
        .filter(col("timestamp").isNotNull())
        .withColumn("prev_stars", lag("stargazers_count", 1).over(window_spec))
        .withColumn("star_gain", col("stargazers_count") - col("prev_stars"))
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
    velocity_df.show(10, truncate=50)

    # ════════════════════════════════════════════════════════════════════════
    # TABEL 4: emerging_topics — Enhanced (Cross-time analysis)
    # ════════════════════════════════════════════════════════════════════════
    print("\n[gold] Membuat emerging_topics...")
    from pyspark.sql.functions import max as spark_max_fn
    max_jam = silver_latest.filter(col("jam").isNotNull()).agg(
        spark_max_fn("jam")
    ).collect()[0][0]

    if max_jam is not None:
        jam_recent = max_jam
        jam_cutoff = (max_jam - 3) % 24

        words_recent = (
            silver_latest
            .filter(col("description").isNotNull() & col("jam").isNotNull()
                    & (col("jam") >= jam_cutoff))
            .select(explode(split(
                lower(regexp_replace(col("description"), r"[^a-zA-Z\s]", "")),
                r"\s+")).alias("word"))
            .filter(col("word").rlike("^[a-z]{4,}"))
            .filter(~col("word").isin(list(STOPWORDS)))
            .groupBy("word").agg(count("*").alias("count_recent"))
        )

        words_old = (
            silver_latest
            .filter(col("description").isNotNull() & col("jam").isNotNull()
                    & (col("jam") < jam_cutoff))
            .select(explode(split(
                lower(regexp_replace(col("description"), r"[^a-zA-Z\s]", "")),
                r"\s+")).alias("word"))
            .filter(col("word").rlike("^[a-z]{4,}"))
            .filter(~col("word").isin(list(STOPWORDS)))
            .groupBy("word").agg(count("*").alias("count_old"))
        )

        emerging_df = (
            words_recent.join(words_old, "word", "left_anti")
            .orderBy(desc("count_recent"))
            .withColumn("_computed_at", current_timestamp())
        )
        emerging_df.write.format("delta").mode("overwrite").save(GOLD_EMERGING)
        print(f"[gold] emerging_topics disimpan: {emerging_df.count()} kata baru")
        emerging_df.show(20, truncate=False)
    else:
        print("[gold] Tidak cukup data temporal untuk emerging_topics, skip.")

    # ════════════════════════════════════════════════════════════════════════
    # TABEL 5: api_rss_join — BONUS Cross-source Join
    # ════════════════════════════════════════════════════════════════════════
    print("\n[gold] Membuat api_rss_join (cross-source join bonus)...")
    try:
        rss_df = spark.read.format("delta").load(BRONZE_RSS)
        rss_count = rss_df.count()
        print(f"[gold] RSS record tersedia: {rss_count}")

        # Ekstrak kata kunci dari nama repo (ambil bagian setelah '/')
        silver_keywords = silver_latest.withColumn(
            "repo_name",
            lower(trim(split(col("full_name"), "/").getItem(1)))
        ).select("full_name", "repo_name", "language", "stargazers_count")

        # Deduplikasi artikel RSS berdasarkan judul agar tidak terjadi perulangan join
        rss_titles = (
            rss_df
            .withColumn("title_lower", lower(col("title")))
            .select("title", "title_lower", "link", "published", "source")
            .dropDuplicates(["title"])
        )

        # Cross join + filter kata utuh menggunakan locate (dynamic column matching)
        join_df = (
            silver_keywords
            .crossJoin(rss_titles)
            # Buat kolom temporary agar bisa dicari secara kata utuh
            .withColumn("padded_title", concat(lit(" "), regexp_replace(regexp_replace(col("title_lower"), r"[^\w\s-]", " "), r"-", " "), lit(" ")))
            .withColumn("padded_repo", concat(lit(" "), col("repo_name"), lit(" ")))
            # Gunakan locate via SQL expression (menghindari error iteration Column)
            .filter(expr("locate(padded_repo, padded_title) > 0"))
            .select(
                col("full_name").alias("repo"),
                col("language"),
                col("stargazers_count"),
                col("title").alias("berita_judul"),
                col("source").alias("berita_sumber"),
                col("published").alias("berita_tanggal"),
                col("link").alias("berita_url"),
            )
            .orderBy(desc("stargazers_count"))
            .withColumn("_computed_at", current_timestamp())
        )

        join_count = join_df.count()
        join_df.write.format("delta").mode("overwrite").save(GOLD_JOIN)
        print(f"[gold] api_rss_join disimpan: {join_count} pasangan repo-berita")

        if join_count > 0:
            print("Repo GitHub yang muncul di berita teknologi:")
            join_df.show(10, truncate=70)
        else:
            print("[gold] Belum ada kecocokan repo-berita (data RSS masih sedikit)")
            print("       Tabel tetap disimpan sebagai Delta Lake ✅")

    except Exception as e:
        print(f"[gold] RSS belum tersedia untuk join: {e}")
        print("[gold] Membuat tabel kosong sebagai placeholder...")
        # Buat tabel placeholder agar struktur Gold tetap lengkap
        empty_df = spark.createDataFrame([], schema="repo STRING, language STRING, "
                                         "stargazers_count LONG, berita_judul STRING, "
                                         "berita_sumber STRING, berita_tanggal STRING, "
                                         "berita_url STRING")
        empty_df.write.format("delta").mode("overwrite").save(GOLD_JOIN)
        print("[gold] api_rss_join placeholder disimpan ✅")

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
    print(f"api_rss_join   : {spark.read.format('delta').load(GOLD_JOIN).count()} pasangan")
    print("\nSemua tabel Gold tersimpan di format Delta Lake ✅")


if __name__ == "__main__":
    main()