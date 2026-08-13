"""Shared SparkSession builder.

Every exercise goes through this so runs are comparable. If each script built
its own session with slightly different config, the timings would not mean
anything next to each other.
"""

from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
YELLOW = str(ROOT / "data" / "yellow" / "*.parquet")
ZONES = str(ROOT / "data" / "reference" / "taxi_zone_lookup.csv")


def get_spark(
    app_name: str = "spark-lab",
    *,
    aqe: bool = False,
    shuffle_partitions: int | None = None,
    driver_memory: str = "4g",
) -> SparkSession:
    """Build a local session with event logging on.

    aqe defaults to **off**, on purpose. Adaptive Query Execution coalesces
    shuffle partitions and splits skewed ones automatically, which means it
    silently fixes the exact problems days 2 and 3 are about diagnosing. Turn
    it on deliberately in day 4 to measure what it buys.
    """
    events = ROOT / "logs" / "spark-events"
    events.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", str(aqe).lower())
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", events.as_uri())
        .config("spark.sql.warehouse.dir", (ROOT / "warehouse").as_uri())
    )
    if shuffle_partitions is not None:
        builder = builder.config("spark.sql.shuffle.partitions", shuffle_partitions)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark {spark.version} | AQE={aqe} | UI {spark.sparkContext.uiWebUrl}")
    return spark


def hold(spark: SparkSession) -> None:
    """Block so the live UI on :4040 stays reachable. Ctrl-C to release."""
    input("\nUI is live. Press Enter to shut down the session... ")
    spark.stop()
