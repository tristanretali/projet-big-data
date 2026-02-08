# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("datamart")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
    .config("spark.sql.hive.metastore.version", "2.3.7")
    .config("spark.sql.hive.metastore.jars", "builtin")
    # Empêche Spark de charger la table en mémoire pour les jointures (évite les problème d'exécution de jobs)
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .enableHiveSupport()
    .getOrCreate()
)

# Configuration PostgreSQL
postgres_url = "jdbc:postgresql://hive-metastore-postgresql:5432/metastore"
postgres_properties = {
    "user": "hive",
    "password": "hive",
    "driver": "org.postgresql.Driver",
}

# Lecture des données issues du silver
orders = spark.table("default.orders_partitioned")
orders_line = spark.table("default.orders_line_partitioned")
products = spark.table("default.products_partitioned")

# Jointure entre toute les tables pour récupérer toutes les infos
sales_data = (
    orders_line.join(orders, "InvoiceNo")
    .join(products, "StockCode")
    .withColumn("Amount", F.col("Quantity") * F.col("UnitPrice"))
)

### 1. Ventes par pays ###
sales_per_country = (
    # On prend uniquement les ventes non annulées
    sales_data.filter(~F.col("IsCancel"))
    .groupBy("Country")
    .agg(
        F.sum("Amount").alias("total_sales"),
        F.count("InvoiceNo").alias("number_of_orders"),
        F.sum("Quantity").alias("total_quantity"),
    )
    .orderBy(F.desc("total_sales"))
)

sales_per_country.write.jdbc(
    url=postgres_url,
    table="sales_per_country",
    mode="overwrite",
    properties=postgres_properties,
)

### 2. Top produits ###
top_products = (
    sales_data.filter(~F.col("IsCancel"))
    .groupBy("StockCode", "Description")
    .agg(
        F.sum("Amount").alias("total_revenue"),
        F.sum("Quantity").alias("total_quantity_sold"),
        F.count("InvoiceNo").alias("number_of_sales"),
    )
    .orderBy(F.desc("total_revenue"))
)

top_products.write.jdbc(
    url=postgres_url,
    table="top_products",
    mode="overwrite",
    properties=postgres_properties,
)

### 3. Produits problématiques ###
return_products = (
    # On récupère les commandes annulées ou avec une quantité négative (ça correspond à des retours)
    sales_data.filter((F.col("Quantity") < 0) | F.col("IsCancel"))
    .groupBy("StockCode", "Description")
    .agg(
        F.sum("Quantity").alias("total_returned"),
        F.count("InvoiceNo").alias("number_of_returns"),
        F.sum("Amount").alias("total_loss"),
    )
    .orderBy(F.desc("total_returned"))
)

return_products.write.jdbc(
    url=postgres_url,
    table="return_products",
    mode="overwrite",
    properties=postgres_properties,
)

### 4. Ventes par période ###
sales_by_period = (
    sales_data.filter(~F.col("IsCancel"))
    .groupBy("year", "month", "day")
    .agg(
        F.sum("Amount").alias("total_sales"),
        F.count("InvoiceNo").alias("number_of_orders"),
        F.sum("Quantity").alias("total_quantity"),
    )
    .orderBy("year", "month", "day")
)

sales_by_period.write.jdbc(
    url=postgres_url,
    table="sales_by_period",
    mode="overwrite",
    properties=postgres_properties,
)
