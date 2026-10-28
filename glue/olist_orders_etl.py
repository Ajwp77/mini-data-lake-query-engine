from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import (col,to_timestamp,to_date,lit,year,month)
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# READ RAW DATA

customers = spark.read.csv(
    "s3://mini-data-lake-query-engine/raw/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)

orders = spark.read.csv(
    "s3://mini-data-lake-query-engine/raw/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

order_items = spark.read.csv(
    "s3://mini-data-lake-query-engine/raw/olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)

products = spark.read.csv(
    "s3://mini-data-lake-query-engine/raw/olist_products_dataset.csv",
    header=True,
    inferSchema=True
)

logger.info("Starting Olist ETL pipeline")

logger.info("Customers: %s", customers.count())
logger.info("Orders: %s", orders.count())
logger.info("Order Items: %s", order_items.count())
logger.info("Products: %s", products.count())


#DATA QUALITY CHECKS 

print("===== DATA QUALITY CHECKS =====")

# Missing order IDs
missing_order_id = orders.filter(
    col("order_id").isNull()
).count()

print("Missing order_id:", missing_order_id)


# Missing customer IDs
missing_customer_id = orders.filter(
    col("customer_id").isNull()
).count()

print("Missing customer_id:", missing_customer_id)


# Duplicate orders
duplicate_orders = orders.groupBy("order_id") \
    .count() \
    .filter(col("count") > 1) \
    .count()

print("Duplicate order_id values:", duplicate_orders)


# Invalid prices
invalid_prices = order_items.filter(
    col("price") < 0
).count()

print("Invalid prices:", invalid_prices)


# Invalid freight values
invalid_freight = order_items.filter(
    col("freight_value") < 0
).count()

print("Invalid freight values:", invalid_freight)

logger.info("Data quality checks completed")

#CLEAN ORDERS

orders_clean = orders \
    .dropDuplicates(["order_id"]) \
    .withColumn(
        "order_purchase_timestamp",
        to_timestamp(col("order_purchase_timestamp"))
    ) \
    .withColumn(
        "order_date",
        to_date(col("order_purchase_timestamp"))
    )


#CLEAN ORDER ITEMS

order_items_clean = order_items \
    .filter(col("price") >= 0) \
    .filter(col("freight_value") >= 0)


# CLEAN PRODUCTS

products_clean = products.dropDuplicates(["product_id"])

# JOIN ORDERS + ORDER ITEMS

orders_items = orders_clean.join(
    order_items_clean,
    on="order_id",
    how="inner"
)


# JOIN PRODUCTS

orders_items_products = orders_items.join(
    products_clean,
    on="product_id",
    how="left"
)


# JOIN CUSTOMERS

final_data = orders_items_products.join(
    customers,
    on="customer_id",
    how="left"
)


# CREATE BUSINESS COLUMNS

final_data = final_data \
    .withColumn("quantity", lit(1)) \
    .withColumn(
        "revenue",
        col("price") * col("quantity")
    ) \
    .withColumn(
        "order_year",
        year(col("order_date"))
    ) \
    .withColumn(
        "order_month",
        month(col("order_date"))
    )


# SELECT REQUIRED COLUMNS

final_data = final_data.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_date",
    "order_year",
    "order_month",
    "product_id",
    "product_category_name",
    "quantity",
    "price",
    "freight_value",
    "revenue",
    "customer_city",
    "customer_state"
)

# CHECK RESULT

print("===== FINAL DATA =====")
final_data.show(10, truncate=False)

print("===== FINAL SCHEMA =====")
final_data.printSchema()

print("===== FINAL ROW COUNT =====")
print(final_data.count())


# WRITE PROCESSED DATA

logger.info("Writing partitioned Parquet dataset")

final_data.write \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .parquet(
        "s3://mini-data-lake-query-engine/processed/orders_partitioned/"
    )

logger.info("Olist ETL pipeline completed successfully")
print("ETL COMPLETED SUCCESSFULLY.....")
