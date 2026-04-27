import os
import sys
import json

os.environ.pop("SPARK_HOME", None)
os.environ.pop("PYTHONPATH", None)

# load config first so JAVA_HOME is set before Spark starts
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    KAFKA_BROKER, KAFKA_TOPIC, KAFKA_DLQ_TOPIC,
    DELTA_PATH, METADATA_PATH, CHECKPOINT_PATH,
    SPARK_MASTER, SPARK_PACKAGES, JAVA_HOME
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from metadata_store import init_db, insert_lineage
from kafka import KafkaProducer as KafkaProducerClient


os.environ["JAVA_HOME"]             = JAVA_HOME
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

VENV_PYSPARK = os.path.join(
    os.path.dirname(sys.executable), "..", "lib",
    "python{}.{}".format(*sys.version_info[:2]),
    "site-packages"
)
sys.path.insert(0, VENV_PYSPARK)



os.makedirs(DELTA_PATH, exist_ok=True)
os.makedirs(METADATA_PATH, exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

spark = SparkSession.builder \
    .appName("SimLineagePipeline") \
    .config("spark.jars.packages", SPARK_PACKAGES) \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master(SPARK_MASTER) \
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
    .option("failOnDataLoss", "false") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("offset"),
    col("partition")
).select("data.*", "offset", "partition")


dlq_producer = KafkaProducerClient(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_to_dlq(record, error_msg, batch_id):
    dlq_record = {
        "original_record": record,
        "error":           error_msg,
        "batch_id":        str(batch_id),
        "failed_at":       str(__import__('datetime').datetime.utcnow())
    }
    dlq_producer.send(KAFKA_DLQ_TOPIC, value=dlq_record)
    dlq_producer.flush()
    print(f"  [DLQ] Sent bad record to {KAFKA_DLQ_TOPIC}: {error_msg}")

def validate_record(row):
    errors = []
    if not row.cell_name:
        errors.append("missing cell_name")
    if not row.run_id:
        errors.append("missing run_id")
    if row.value is None:
        errors.append("missing value")
    if row.value is not None and not (-100 < row.value < 100):
        errors.append(f"value {row.value} out of expected range")
    return errors

def process_batch(batch_df, batch_id):
    try:
        count = batch_df.count()
        if count == 0:
            return

        print(f"\n--- Batch {batch_id} | {count} records ---")

        # ── Validate records ──────────────────────────────────
        good_records = []
        bad_records  = []

        for row in batch_df.collect():
            errors = validate_record(row)
            if errors:
                bad_records.append((row.asDict(), errors))
            else:
                good_records.append(row)

        # ── Route bad records to DLQ ──────────────────────────
        if bad_records:
            print(f"  [DLQ] {len(bad_records)} bad records found")
            for record, errors in bad_records:
                send_to_dlq(record, ", ".join(errors), batch_id)

        # ── Process good records ──────────────────────────────
        if not good_records:
            print("  No valid records in this batch")
            return

        good_df = batch_df.sparkSession.createDataFrame(
            good_records, schema=batch_df.schema
        )

        good_df.write \
            .format("delta") \
            .mode("append") \
            .save(DELTA_PATH)

        from delta import DeltaTable
        delta_table   = DeltaTable.forPath(spark, DELTA_PATH)
        delta_version = delta_table.history(1).collect()[0]["version"]

        run_ids  = [r.run_id  for r in good_df.select("run_id").distinct().collect()]
        git_shas = [r.git_sha for r in good_df.select("git_sha").distinct().collect()]
        git_sha  = git_shas[0] if git_shas else "unknown"
        processed_at = str(good_df.select(current_timestamp()).first()[0])

        for run_id in run_ids:
            metadata = spark.createDataFrame([{
                "batch_id":      str(batch_id),
                "run_id":        run_id,
                "git_sha":       git_sha,
                "row_count":     len(good_records),
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
                row_count=len(good_records),
                delta_version=delta_version,
                processed_at=processed_at
            )
            print(f"  run_id={run_id} → delta_version={delta_version} "
                  f"git_sha={git_sha} | good={len(good_records)} bad={len(bad_records)}")

    except Exception as e:
        print(f"  [ERROR] Batch {batch_id} failed: {e}")
        raise

query = parsed.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="15 seconds") \
    .start()

print("Consumer started. Listening for messages every 15 seconds. Ctrl+C to stop.")
query.awaitTermination()