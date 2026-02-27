from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, filter, when, count,sum, min, max
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder\
    .appName("Aggregations Demo")\
    .getOrCreate()

# Sample sales data
sales_data = [
    ("2023-01", "Electronics", "Laptop", 1200, "NY"),
    ("2023-01", "Electronics", "Phone", 800, "NY"),
    ("2023-01", "Electronics", "Tablet", 500, "CA"),
    ("2023-01", "Clothing", "Jacket", 150, "NY"),
    ("2023-01", "Clothing", "Shoes", 100, "CA"),
    ("2023-02", "Electronics", "Laptop", 1300, "TX"),
    ("2023-02", "Electronics", "Phone", 850, "NY"),
    ("2023-02", "Clothing", "Jacket", 175, "CA"),
    ("2023-02", "Clothing", "Pants", 80, "TX")
]

df = spark.createDataFrame(
    sales_data,["month", "category", "product", "amount", "state"]
)

df.show()

# count operation
print(f"Total Records: {df.count()}")

# use bais agg() without groupBy
df.agg(
    count("*").alias("Total_Sales"),
    sum("amount").alias("Total_revenue")
).show()

# groupBy
print("Group by Category")
df.groupBy("category").count().show()
print("Group by Produt")
df.groupBy("product").sum("amount").show()

# Grou by using agg() for complex aggregation. agg is used for several aggregations
df.groupBy("category").agg(
    count("*").alias("Total Sales"),
    sum("amount").alias("Total Revenue"),
    min("amount").alias("Min Sales"),
    max("amount").alias("Max Sales"),
).show()