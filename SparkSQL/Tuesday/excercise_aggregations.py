"""
Exercise: Aggregations
======================
Week 2, Tuesday

Practice groupBy and aggregate functions on sales data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, min, max, countDistinct

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Aggregations").master("local[*]").getOrCreate()

# Sample sales data
sales = [
    ("2023-01", "Electronics", "Laptop", 1200, "Alice"),
    ("2023-01", "Electronics", "Phone", 800, "Bob"),
    ("2023-01", "Electronics", "Tablet", 500, "Alice"),
    ("2023-01", "Clothing", "Jacket", 150, "Charlie"),
    ("2023-01", "Clothing", "Shoes", 100, "Diana"),
    ("2023-02", "Electronics", "Laptop", 1300, "Eve"),
    ("2023-02", "Electronics", "Phone", 850, "Alice"),
    ("2023-02", "Clothing", "Jacket", 175, "Bob"),
    ("2023-02", "Clothing", "Pants", 80, "Charlie"),
    ("2023-03", "Electronics", "Laptop", 1100, "Frank"),
    ("2023-03", "Electronics", "Phone", 750, "Grace"),
    ("2023-03", "Clothing", "Shoes", 120, "Alice")
]

df = spark.createDataFrame(sales, ["month", "category", "product", "amount", "salesperson"])

print("=== Exercise: Aggregations ===")
print("\nSales Data:")
df.show()

# =============================================================================
# TASK 1: Simple Aggregations (15 mins)
# =============================================================================

print("\n--- Task 1: Simple Aggregations ---")

# 1a: Calculate total, average, min, and max amount across ALL sales
# Use agg() without groupBy
df.agg(
    spark_sum("amount").alias('Total Amount'),
    avg("amount").alias("Total Average"),
    min("amount").alias("Total Min"),
    max("amount").alias("Total Max")
).show()


# 1b: Count the total number of sales transactions
df.agg(
    count("*").alias("Total number of sales")
).show()


# 1c: Count distinct categories
df.agg(
    countDistinct("category")
).show()


# =============================================================================
# TASK 2: GroupBy with Single Aggregation (15 mins)
# =============================================================================

print("\n--- Task 2: GroupBy Single Aggregation ---")

#  2a: Total sales amount by category
df.groupBy("category").sum("amount").show()


#  2b: Average sale amount by month
df.groupBy("month").avg("amount").show()

#  2c: Count of transactions by salesperson
df.groupBy("salesperson").agg(
    count("*").alias('Transactions')
).show()

# =============================================================================
# TASK 3: GroupBy with Multiple Aggregations (20 mins)
# =============================================================================

print("\n--- Task 3: GroupBy Multiple Aggregations ---")

#  3a: For each category, calculate:
# - Number of transactions
# - Total revenue
# - Average sale amount
# - Highest single sale
# Use meaningful aliases!
df.groupBy("category").agg(
    count("*").alias("Total Transactions"),
    spark_sum("amount").alias("Total Revenue"),
    avg("amount").alias("Average Sales amount"),
    max("amount").alias("Highest Sale"),
).show()


# 3b: For each salesperson, calculate:
# - Number of sales
# - Total revenue
# - Distinct products sold (countDistinct)
df.groupBy("salesperson").agg(
    count("*").alias("Number of Sales"),
    spark_sum("amount").alias("Total Revenue"),
    countDistinct("product").alias("Products Sold")
).show()

# =============================================================================
# TASK 4: Multi-Column GroupBy (15 mins)
# =============================================================================

print("\n--- Task 4: Multi-Column GroupBy ---")

# 4a: Calculate total sales by month AND category
df.groupBy("month", 'category').sum('amount').show()

#  4b: Find the top salesperson by month (hint: use multi-column groupBy)
df.groupBy('salesperson','month').max('amount').show()

# =============================================================================
# TASK 5: Filtering After Aggregation (15 mins)
# =============================================================================

print("\n--- Task 5: Filtering After Aggregation ---")

# 5a: Find categories with total revenue > 2000
df.groupBy('category').agg(
    spark_sum('amount').alias('total_revenue')
).filter(col('total_revenue') > 2000).show()

#  5b: Find salespeople who made more than 2 transactions
df.groupBy('salesperson').agg(
    count('*').alias('Transactions')
).filter(col('Transactions') > 2).show()

# 5c: Find month-category combinations with average sale > 500
df.groupBy('month', 'category').agg(
    avg('amount').alias('avg_per_month')
).filter(col('avg_per_month') > 500).show()

# =============================================================================
# CHALLENGE: Business Questions (20 mins)
# =============================================================================

print("\n--- Challenge: Business Questions ---")

#  6a: Which category had the highest average transaction value?
df.groupBy('category').agg(
    avg('amount').alias('avg_amount')
).orderBy(col('avg_amount').desc()).limit(1).show()

# 6b: Who is the top salesperson by total revenue?
df.groupBy('salesperson').agg(
    spark_sum('amount').alias('Top Revenue')
).orderBy(col('Top Revenue').desc()).limit(1).show()


#  6c: Which month had the most diverse products sold?
# HINT: Use countDistinct on product column
df.groupBy('month').agg(
    countDistinct('product').alias('unique_products')
).orderBy(col('unique_products').desc()).limit(1).show()


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()