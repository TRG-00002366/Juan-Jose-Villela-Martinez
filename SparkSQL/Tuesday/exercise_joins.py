"""
Exercise: Joins
===============
Week 2, Tuesday

Practice all join types with customer and order data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, count, sum, when

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Joins").master("local[*]").getOrCreate()

# Customers
customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com", "NY"),
    (2, "Bob", "bob@email.com", "CA"),
    (3, "Charlie", "charlie@email.com", "TX"),
    (4, "Diana", "diana@email.com", "FL"),
    (5, "Eve", "eve@email.com", "WA")
], ["customer_id", "name", "email", "state"])

# Orders
orders = spark.createDataFrame([
    (101, 1, "2023-01-15", 150.00),
    (102, 2, "2023-01-16", 200.00),
    (103, 1, "2023-01-17", 75.00),
    (104, 3, "2023-01-18", 300.00),
    (105, 6, "2023-01-19", 125.00),  # customer_id 6 does not exist!
    (106, 2, "2023-01-20", 180.00)
], ["order_id", "customer_id", "order_date", "amount"])

# Products (for multi-table join)
products = spark.createDataFrame([
    (101, "Laptop"),
    (102, "Phone"),
    (103, "Mouse"),
    (104, "Keyboard"),
    (107, "Monitor")  # Not in any order!
], ["order_id", "product_name"])

print("=== Exercise: Joins ===")
print("\nCustomers:")
customers.show()
print("Orders:")
orders.show()
print("Products:")
products.show()

# =============================================================================
# TASK 1: Inner Join (15 mins)
# =============================================================================

print("\n--- Task 1: Inner Join ---")

# TODO 1a: Join customers and orders (only matching records)
# Show customer name, order_id, order_date, amount
customers.join(
    orders,
    'customer_id',
    'inner'
).select(
    customers.name,
    orders.order_id,
    orders.order_date,
    orders.amount
).show()

# TODO 1b: How many orders have matching customers?
# HINT: Compare this count to total orders
customers.join(
    orders,
    'customer_id',
    'inner'
).select(
    count("*").alias("Orders matching a customer")
).show()

# =============================================================================
# TASK 2: Left and Right Joins (20 mins)
# =============================================================================

print("\n--- Task 2: Left and Right Joins ---")

# TODO 2a: LEFT JOIN - All customers, with order info where available
# Who has NOT placed any orders?
customers.join(
    orders,
    'customer_id',
    'left'
).select(
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    orders.amount
).show()

# print("Customers that have not placed an order:")
customers.join(
    orders,
    'customer_id',
    'left_anti'
).show()

# TODO 2b: RIGHT JOIN - All orders, with customer info where available
# Which order has no matching customer?
orders.join(
    customers,
    'customer_id',
    'right'
).select(
    *customers.columns
).show()

# print("Orders with no matching customer")
orders.join(
    customers,
    'customer_id',
    'left_anti'
).select(
    *orders.columns
).show()

# TODO 2c: What is the difference between the two results?
# Answer in a comment:
#The left Join finds all matching rows on orders based on customer_id,
# whereas the Right join finds al matching orders on customers df, based on customer_id

# =============================================================================
# TASK 3: Full Outer Join (10 mins)
# =============================================================================

print("\n--- Task 3: Full Outer Join ---")

# TODO 3a: Perform a FULL OUTER join between customers and orders
# All customers AND all orders should appear
customers.join(
    orders,
    'customer_id',
    'outer'
).show()

# # TODO 3b: Filter to show only rows where there is a mismatch
# # (customer without order OR order without customer)
print('Customers/Orders with no match')
customers.join(
    orders,
    'customer_id',
    'outer'
).filter(
    col('order_id').isNull() | col('name').isNull()
).show()

# =============================================================================
# TASK 4: Semi and Anti Joins (15 mins)
# =============================================================================

print("\n--- Task 4: Semi and Anti Joins ---")

# TODO 4a: LEFT SEMI JOIN - Customers who HAVE placed orders
# Only customer columns should appear
customers.join(
    orders,
    'customer_id',
    'left_semi'
).show()


# TODO 4b: LEFT ANTI JOIN - Customers who have NOT placed orders
customers.join(
    orders,
    'customer_id',
    'left_anti'
).show()

# TODO 4c: When would you use anti join in real data work?
# Answer in a comment:
#IN a real data work one would use left_semi to query a DF to filter our null columns, 
# thereby getting only rows that HAVE placed orders, whereas a left_anti would be use
# for querying data and filtering out rows we know have data, but we want the rows with null
# values to see how they do not match. 

# =============================================================================
# TASK 5: Handling Duplicate Columns (15 mins)
# =============================================================================

print("\n--- Task 5: Handling Duplicate Columns ---")

# After joining customers and orders, both have customer_id

# TODO 5a: Join and then DROP the duplicate customer_id column
joined = customers.join(
    orders,
    'customer_id',
    'inner'
)
cleaned = joined.drop(orders.customer_id).show()

# TODO 5b: Alternative: Use aliases to reference specific columns
# HINT: customers.alias("c"), orders.alias("o")
joined = customers.alias('c').join(
    orders.alias('o'),
    'customer_id',
    'inner'
)

cleaned = joined.drop('o.customer_id').show()

# =============================================================================
# TASK 6: Multi-Table Join (15 mins)
# =============================================================================

print("\n--- Task 6: Multi-Table Join ---")

# TODO 6a: Join customers -> orders -> products
# Show: customer name, order_id, amount, product_name
customers.alias('c').join(
    orders.alias('o'),
    'customer_id',
    'inner'
).join(
    products.alias('p'),
    'order_id',
    'inner'
).select(
    ['c.name', 'o.order_id', 'o.amount', 'p.product_name']
).show()

# TODO 6b: What kind of join should you use when some orders might not have products?
# Depends on what we're trying to query. If we only want to query orders that HAVE product
# then, we would use a left_semi join or a simple inner join to match rows based on order_id

# =============================================================================
# CHALLENGE: Real-World Scenarios (20 mins)
# =============================================================================

print("\n--- Challenge: Real-World Scenarios ---")

# TODO 7a: Find the total spending per customer (only customers with orders)
# Use join + groupBy + sum
customers.join(
    orders,
    'customer_id',
    'inner'
).groupBy(
    'name'
).agg(
    sum('amount').alias('Total Spent')
).show()


# TODO 7b: Find customers from CA who placed orders > $150
customers.join(
    orders,
    'customer_id',
    'inner'
).filter(
    (col('state') == 'CA') & (col('amount') > 150)
).show()

# TODO 7c: Find orders without valid product information
# (anti join pattern)
orders.join(
    products,
    'order_id',
    'left_anti'
).show()

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()