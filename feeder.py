from datetime import date
from pyspark.sql import SparkSession, functions as F
import time

spark = SparkSession.builder.appName("feeder").getOrCreate()

datasets = [
    {
        "name": "orders_line",
        "input": "/opt/pipeline/data/orders_line.csv",
        "output": "hdfs://namenode:9000/data/raw/orders_line",
        "partition": False,
    },
    {
        "name": "orders",
        "input": "/opt/pipeline/data/orders.csv",
        "output": "hdfs://namenode:9000/data/raw/orders_partitioned",
        "partition": True,
    },
    {
        "name": "products",
        "input": "/opt/pipeline/data/products.csv",
        "output": "hdfs://namenode:9000/data/raw/products_partitioned",
        "partition": False,
    },
]

for dataset in datasets:

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(dataset["input"])
    )

    # Partitionnement par date uniquement pour celui qui en a une
    if dataset["partition"]:

        df2 = (
            df.withColumn(
                "invoice_timestamp",
                F.to_timestamp(F.col("InvoiceDate"), "M/d/yyyy H:mm"),
            )
            .withColumn("year", F.year(F.col("invoice_timestamp")))
            .withColumn("month", F.month(F.col("invoice_timestamp")))
            .withColumn("day", F.dayofmonth(F.col("invoice_timestamp")))
            .drop("invoice_timestamp")
        )

        df2.cache()
        # time.sleep(60)

        (
            df2.repartition(8)
            .write.mode("overwrite")
            .partitionBy("year", "month", "day")
            .parquet(dataset["output"])
        )
    else:
        df.cache()
        # time.sleep(60)

        (df.repartition(8).write.mode("overwrite").parquet(dataset["output"]))

spark.stop()
