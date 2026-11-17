
"""Prefect flow: Bluesky Parquet -> DuckDB -> analysis tables -> charts.

Run on full data:
    python flow.py

Run on committed sample:
    python flow.py data/sample
"""
import os
import sys
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from prefect import flow, get_run_logger, task


@task(retries=2, retry_delay_seconds=5)
def load_raw(source_dir, db_path):
    files = sorted(Path(source_dir).glob("*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No Parquet files found in {source_dir}"
        )

    with duckdb.connect(db_path) as con:
        con.execute(
            """
            CREATE OR REPLACE TABLE raw_bluesky AS
            SELECT * FROM read_parquet(?)
            """,
            [str(Path(source_dir) / "*.parquet")],
        )
        return con.execute(
            "SELECT COUNT(*) FROM raw_bluesky"
        ).fetchone()[0]


@task
def stage(db_path):
    """Remove duplicate Kafka records but retain malformed raw rows."""
    with duckdb.connect(db_path) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY kafka_partition, kafka_offset
                        ORDER BY landed_at DESC
                    ) AS rn
                FROM raw_bluesky
            )
            WHERE rn = 1
        """)

        malformed = con.execute("""
            SELECT COUNT(*)
            FROM raw_bluesky
            WHERE action = 'malformed'
        """).fetchone()[0]

        unique_kafka = con.execute("""
            SELECT COUNT(*) FROM stg_records
        """).fetchone()[0]

    return {
        "malformed": malformed,
        "unique_kafka": unique_kafka,
    }


@task
def entities(db_path):
    """Keep the latest event for each Bluesky post."""
    with duckdb.connect(db_path) as con:
        con.execute("""
            CREATE OR REPLACE TABLE bluesky_posts AS
            WITH post_events AS (
                SELECT
                    did,
                    rkey,
                    operation,
                    action,
                    TRY_CAST(event_time AS TIMESTAMPTZ)
                        AS event_time,
                    TRY_CAST(langs AS JSON) AS langs_json,
                    is_reply,
                    kafka_partition,
                    kafka_offset
                FROM stg_records
                WHERE action <> 'malformed'
                  AND did IS NOT NULL
                  AND rkey IS NOT NULL
                  AND collection = 'app.bsky.feed.post'
            ),
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY did, rkey
                        ORDER BY kafka_offset DESC
                    ) AS rn
                FROM post_events
            )
            SELECT
                did,
                rkey,
                operation,
                action,
                event_time,
                COALESCE(
                    JSON_EXTRACT_STRING(langs_json, '$[0]'),
                    'unknown'
                ) AS language,
                is_reply,
                kafka_partition,
                kafka_offset
            FROM ranked
            WHERE rn = 1
        """)

        return con.execute("""
            SELECT COUNT(*) FROM bluesky_posts
        """).fetchone()[0]


@task
def marts(db_path):
    """Create hourly and language summary tables."""
    with duckdb.connect(db_path) as con:
        con.execute("""
            CREATE OR REPLACE TABLE mart_posts_hourly AS
            SELECT
                TIME_BUCKET(
                    INTERVAL '1 hour', event_time
                ) AS hour,
                COUNT(*) AS post_count
            FROM bluesky_posts
            WHERE operation = 'create'
              AND event_time IS NOT NULL
            GROUP BY hour
            ORDER BY hour
        """)

        con.execute("""
            CREATE OR REPLACE TABLE mart_posts_language AS
            SELECT
                language,
                COUNT(*) AS post_count
            FROM bluesky_posts
            WHERE operation = 'create'
            GROUP BY language
            ORDER BY post_count DESC
        """)

        return {
            "hours": con.execute(
                "SELECT COUNT(*) FROM mart_posts_hourly"
            ).fetchone()[0],
            "languages": con.execute(
                "SELECT COUNT(*) FROM mart_posts_language"
            ).fetchone()[0],
        }


@task
def charts(db_path, chart_dir):
    """Generate charts with labelled axes."""
    os.makedirs(chart_dir, exist_ok=True)

    with duckdb.connect(db_path, read_only=True) as con:
        hourly = con.execute("""
            SELECT hour, post_count
            FROM mart_posts_hourly
            ORDER BY hour
        """).fetchall()

        languages = con.execute("""
            SELECT language, post_count
            FROM mart_posts_language
            ORDER BY post_count DESC
            LIMIT 10
        """).fetchall()

    if not hourly:
        raise ValueError("No hourly posts available to chart")

    hours = [r[0] for r in hourly]
    counts = [r[1] for r in hourly]

    plt.figure(figsize=(11, 5))
    plt.plot(hours, counts, marker="o", color="#277DA1")
    plt.title("Bluesky Posts by Hour")
    plt.xlabel("Hour (UTC)")
    plt.ylabel("Number of posts")
    plt.xticks(rotation=45)
    plt.grid(axis="y", alpha=0.25)
    plt.tight_layout()

    hourly_path = os.path.join(
        chart_dir, "bluesky_posts_per_hour.png"
    )
    plt.savefig(hourly_path, dpi=150)
    plt.close()

    if languages:
        names = [r[0] for r in languages][::-1]
        values = [r[1] for r in languages][::-1]

        plt.figure(figsize=(9, 5))
        plt.barh(names, values, color="#277DA1")
        plt.title("Top 10 Languages in Bluesky Posts")
        plt.xlabel("Number of posts")
        plt.ylabel("Language code")
        plt.tight_layout()

        language_path = os.path.join(
            chart_dir, "bluesky_languages.png"
        )
        plt.savefig(language_path, dpi=150)
        plt.close()
    else:
        language_path = None

    return {
        "hourly": hourly_path,
        "languages": language_path,
    }


@flow(name="bluesky-pipeline")
def bluesky_pipeline(source_dir="data/raw"):
    log = get_run_logger()

    is_sample = Path(source_dir).name == "sample"

    db_path = (
        "bluesky_sample.duckdb"
        if is_sample
        else "bluesky.duckdb"
    )

    chart_dir = (
        "charts/sample"
        if is_sample
        else "charts"
    )

    raw_count = load_raw(source_dir, db_path)
    stage_counts = stage(db_path)
    unique_posts = entities(db_path)
    mart_counts = marts(db_path)
    chart_paths = charts(db_path, chart_dir)

    log.info("Raw records landed:        %d", raw_count)
    log.info(
        "Malformed (kept in raw):   %d",
        stage_counts["malformed"],
    )
    log.info(
        "Unique Kafka records:      %d",
        stage_counts["unique_kafka"],
    )
    log.info("Unique Bluesky posts:      %d", unique_posts)

    log.info("Hourly windows: %d", mart_counts["hours"])
    log.info("Languages: %d", mart_counts["languages"])
    log.info("Hourly chart: %s", chart_paths["hourly"])
    log.info("Language chart: %s", chart_paths["languages"])


if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
    bluesky_pipeline(source)
