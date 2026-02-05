# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("preprocessor").enableHiveSupport().getOrCreate()


# Validator
def validate_and_split(df, dataset_name, validations):
    valid_condition = None
    for condition in validations:
        if valid_condition is None:
            valid_condition = condition
        else:
            valid_condition = valid_condition & condition

    # Séparation des données valides et invalides
    valid_df = df.filter(valid_condition)
    invalid_df = df.filter(~valid_condition)

    return valid_df, invalid_df


##### Traitement des données concernant les produits #####
products_df = spark.read.parquet("hdfs://namenode:9000/data/raw/products_partitioned")

# Suppression des espaces inutile pour les colonnes "Description" et "StockCode"
products_cleaned = products_df.withColumn(
    "Description", F.trim(F.col("Description"))
).withColumn("StockCode", F.trim(F.col("StockCode")))


# Règles de validation pour les produits. On s'assure qu'ils ont un code de stock valide et une description non vide
products_validations = [
    F.col("StockCode").isNotNull(),
    F.col("StockCode") != "",
    ~F.upper(F.col("StockCode")).isin(["POST", "POSTAGE", "M", "D", "BANK CHARGES"]),
    F.col("Description").isNotNull(),
    F.col("Description") != "",
    ~F.lower(F.col("Description")).contains("bad debt"),
    ~F.lower(F.col("Description")).contains("adjust"),
    ~F.lower(F.col("Description")).contains("dotcom postage"),
    ~F.lower(F.col("Description")).contains("amazon fee"),
    F.col("UnitPrice").isNotNull(),
    F.col("UnitPrice") > 0,
]

products_valid, products_invalid = validate_and_split(
    products_cleaned, "products", products_validations
)

products_processed = (
    products_valid.groupBy("StockCode")
    .agg(
        F.avg("UnitPrice").alias("UnitPrice"),
        F.first("Description").alias("Description"),
    )
    .select("StockCode", "Description", "UnitPrice")
)

# Sauvegarde dans Hive
products_processed.write.mode("overwrite").format("parquet").saveAsTable(
    "default.products_partitioned"
)
products_invalid.write.mode("overwrite").format("parquet").saveAsTable(
    "default.products_invalid"
)

##### Traitement des données concernant les commandes #####
orders_df = spark.read.parquet("hdfs://namenode:9000/data/raw/orders_partitioned")

# Conversion de la date en type TIMESTAMP
orders_with_date = orders_df.withColumn(
    "InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), "M/d/yyyy H:mm")
)

# Les règles de validation pour orders
orders_validations = [
    F.col("InvoiceNo").isNotNull(),
    F.col("InvoiceNo") != "",
    F.col("InvoiceDate").isNotNull(),
    F.col("Country").isNotNull(),
    F.col("Country") != "",
]


orders_valid, orders_invalid = validate_and_split(
    orders_with_date, "orders", orders_validations
)

# Nettoyage et ajout de colonnes
orders_processed = (
    orders_valid
    # Quand il n'y a pas de customerID on met -1
    .withColumn(
        "CustomerID",
        F.when(F.col("CustomerID").isNull(), -1).otherwise(
            F.col("CustomerID").cast("int")
        ),
    )
    # Quand le pays est "Unspecified" on met "Unknown"
    .withColumn(
        "Country",
        F.when(
            (F.col("Country") == "Unspecified") | (F.col("Country").isNull()), "Unknown"
        ).otherwise(F.col("Country")),
    )
    # Ajouter le flag IsCancel pour les commandes annulées (InvoiceNo commençant par "C")
    .withColumn(
        "IsCancel", F.when(F.col("InvoiceNo").startswith("C"), True).otherwise(False)
    ).select(
        "InvoiceNo",
        "InvoiceDate",
        "CustomerID",
        "Country",
        "IsCancel",
        "year",
        "month",
        "day",
    )
)

orders_processed.write.mode("overwrite").format("parquet").partitionBy(
    "year", "month", "day"
).saveAsTable("default.orders_partitioned")

orders_invalid.write.mode("overwrite").format("parquet").saveAsTable(
    "default.orders_invalid"
)


##### Traitement des données concernant les lignes de commandes #####
orders_line_df = spark.read.parquet("hdfs://namenode:9000/data/raw/orders_line")

orders_line_validations = [
    F.col("InvoiceNo").isNotNull(),
    F.col("InvoiceNo") != "",
    F.col("StockCode").isNotNull(),
    F.col("StockCode") != "",
    F.col("Quantity").isNotNull(),
    F.col("Quantity") != 0,
]

orders_line_valid, orders_line_invalid = validate_and_split(
    orders_line_df, "orders_line", orders_line_validations
)

orders_line_valid.write.mode("overwrite").format("parquet").saveAsTable(
    "default.orders_line_partitioned"
)
orders_line_invalid.write.mode("overwrite").format("parquet").saveAsTable(
    "default.orders_line_invalid"
)

spark.stop()
