import os
from dotenv import load_dotenv

load_dotenv()

# Kafka
KAFKA_BROKER     = os.getenv("KAFKA_BROKER",      "localhost:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC",        "sim-outputs")
KAFKA_DLQ_TOPIC  = os.getenv("KAFKA_DLQ_TOPIC",   "sim-outputs-failed")

# Delta Lake
DELTA_PATH       = os.getenv("DELTA_PATH",         "./delta/sim_outputs")
METADATA_PATH    = os.getenv("METADATA_PATH",      "./delta/metadata")
CHECKPOINT_PATH  = os.getenv("CHECKPOINT_PATH",    "./delta/checkpoints/sim_outputs")

# Spark
SPARK_MASTER     = os.getenv("SPARK_MASTER",       "local[*]")
SPARK_PACKAGES   = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "io.delta:delta-spark_2.12:3.1.0"
)

# Java
JAVA_HOME        = os.getenv("JAVA_HOME",          "/opt/homebrew/opt/openjdk@17")

# Metadata
DB_PATH          = os.getenv("METADATA_DB_PATH",   "./metadata.db")