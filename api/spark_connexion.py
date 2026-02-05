from pyspark.sql import SparkSession


def create_spark_session():
    spark = (
        SparkSession.builder.appName("DataMart API").master("local[*]").getOrCreate()
    )
    return spark
