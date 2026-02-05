from datetime import date
from pyspark.sql import SparkSession, functions as F
import time

spark = (
    SparkSession.builder
    .appName("feeder")
    .getOrCreate()
)

datasets = [
    {
        "name": "orders_line",
        "input": "data/orders_line.csv",
        "output": "hdfs://namenode:9000/data/raw/orders_line_partitioned"
    },
    {
        "name": "orders",
        "input": "data/orders.csv",
        "output": "hdfs://namenode:9000/data/raw/orders_partitioned"
    },
    {
        "name": "products",
        "input": "data/products.csv",
        "output": "hdfs://namenode:9000/data/raw/products_partitioned"
    }
]

today = date.today()

# Process each dataset
for dataset in datasets:
    ##print(f"Reading {dataset['name']} from {dataset['input']}...")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(dataset['input'])
    )

    # Add partition columns
    df2 = (
        df.withColumn("year", F.lit(today.year))
          .withColumn("month", F.lit(today.month))
          .withColumn("day", F.lit(today.day))
    )

    df2.cache()
    
    ##print(f"Waiting 60 seconds before writing {dataset['name']}...")
    time.sleep(60)

    ##print(f"Writing {dataset['name']} to {dataset['output']}...")
    (
        df2.repartition(8)
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(dataset['output'])
    )
    ##print(f"Completed {dataset['name']}.\n")

spark.stop()
