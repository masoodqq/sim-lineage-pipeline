# save this as check_delta.py in your project root
import os, sys
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckDelta") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("./delta/sim_outputs")
print(f"\nTotal records: {df.count()}")
print("\nSample records:")
df.select("cell_name", "tool", "metric", "value", "run_id", "git_sha").show(5)

from delta import DeltaTable
dt = DeltaTable.forPath(spark, "./delta/sim_outputs")
print("\nDelta history:")
dt.history().select("version", "timestamp", "operation").show()