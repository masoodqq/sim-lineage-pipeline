import os
import sys

os.environ.pop("SPARK_HOME", None)
os.environ.pop("PYTHONPATH", None)
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

VENV_PYSPARK = os.path.join(
    os.path.dirname(sys.executable), "..", "lib",
    "python{}.{}".format(*sys.version_info[:2]),
    "site-packages"
)
sys.path.insert(0, VENV_PYSPARK)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from metadata_store import init_db, insert_lineage

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "sim-outputs"
DELTA_PATH = "./delta/sim_outputs"
METADATA_PATH = "./delta/metadata"
CHECKPOINT_PATH = "./delta/checkpoints/sim_outputs"

os.makedirs(DELTA_PATH, exist_ok=True)
os.makedirs(METADATA_PATH, exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

spark = SparkSession.builder \
    .appName("SimLineagePipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
init_db()

schema = StructType([
    StructField("cell_name",    StringType(), True),
    StructField("tool",         StringType(), True),
    StructField("tool_version", StringType(), True),
    StructField("metric",       StringType(), True),
    StructField("value",        DoubleType(), True),
    StructField("timestamp",    StringType(), True),
    StructField("run_id",       StringType(), True),
    StructField("git_sha",      StringType(), True),
    StructField("ingested_at",  StringType(), True),
])

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("offset"),
    col("partition")
).select("data.*", "offset", "partition")

def process_batch(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return

    print(f"\n--- Batch {batch_id} | {count} records ---")

    batch_df.write \
        .format("delta") \
        .mode("append") \
        .save(DELTA_PATH)

    from delta import DeltaTable
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    delta_version = delta_table.history(1).collect()[0]["version"]

    run_ids = [r.run_id for r in batch_df.select("run_id").distinct().collect()]
    git_shas = [r.git_sha for r in batch_df.select("git_sha").distinct().collect()]
    processed_at = str(batch_df.select(current_timestamp()).first()[0])
    git_sha = git_shas[0] if git_shas else "unknown"

    for run_id in run_ids:
        metadata = spark.createDataFrame([{
            "batch_id":      str(batch_id),
            "run_id":        run_id,
            "git_sha":       git_sha,
            "row_count":     count,
            "delta_version": delta_version,
            "processed_at":  processed_at
        }])

        metadata.write \
            .format("delta") \
            .mode("append") \
            .save(METADATA_PATH)

        insert_lineage(
            batch_id=str(batch_id),
            run_id=run_id,
            git_sha=git_sha,
            row_count=count,
            delta_version=delta_version,
            processed_at=processed_at
        )
        print(f"  run_id={run_id} → delta_version={delta_version} git_sha={git_sha}")

query = parsed.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="15 seconds") \
    .start()

print("Consumer started. Listening for messages every 15 seconds. Ctrl+C to stop.")
query.awaitTermination()