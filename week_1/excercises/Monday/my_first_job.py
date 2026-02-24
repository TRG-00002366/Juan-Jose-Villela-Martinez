from pyspark.sql import  SparkSession
from pyspark.sql.functions import col, sum as _sum

def main():
    # Step 1: Create SparkSession
    spark = SparkSession.builder \
        .appName("MyFirstJob") \
        .master("local[*]") \
        .getOrCreate()
    
    # Step 2: YOUR CODE HERE - Create some data
    # Sample data: (product, category, price, quantity)
    sales_data = [
        ("Laptop", "Electronics", 999.99, 5),
        ("Mouse", "Electronics", 29.99, 50),
        ("Desk", "Furniture", 199.99, 10),
        ("Chair", "Furniture", 149.99, 20),
        ("Monitor", "Electronics", 299.99, 15),
    ]

    # Create DataFrame with column names
    df = spark.createDataFrame(sales_data, ["product", "category", "price", "quantity"])
    
    # Step 3: YOUR CODE HERE - Perform transformations
        # 2. Total number of products
    total_products = df.count()

        # 3. calc total revenue per product:
    df_revenue = df.withColumn("revenue", col("price") * col("quantity") )

        # 4. Filter by category: show only Electornic products
    electronics_df = df_revenue.filter(col("category") == "Electronics")
    

        # 5. Aggregate by category: Calculate total revenue per category
    revenue_by_category = df_revenue.groupBy("category").agg(
        _sum("revenue").alias("total_revenue")
    )

    # Step 4: YOUR CODE HERE - Show results
    print(f"Ttoal number of products: {total_products}")
    df.show()
    print("Revenue per product:\n")
    df_revenue.show()
    print("Electronics only:\n")
    electronics_df.show()
    print("Revenue by category:\n")
    revenue_by_category.show()
    
    # Step 5: Clean up
    spark.stop()

if __name__ == "__main__":
    main()