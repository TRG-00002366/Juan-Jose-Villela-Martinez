from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, when, desc, asc, isnull
)
from pyspark.sql.window import Window

# file paths
rides_path = "rides.csv"
drivers_path = "drivers.csv"
output_dir = "Ride_Analytics_Results"

# SparkSession, entry point
spark = SparkSession.builder \
    .appName("City Ride Analytics") \
    .master("local[*]") \
    .getOrCreate()


# T1: CREATE DATAFRAME FROM CSV
# Load rides data and drivers data with header and schema inference
rides_df = spark.read.csv(
    rides_path,
    header=True,
    inferSchema=True
)
drivers_df = spark.read.csv(
    drivers_path,
    header=True,
    inferSchema=True
)

# T2: DISPLAY SCHEMA & PREVIEW
print("\nRIDES SCHEMA")
rides_df.printSchema()

print("\nRIDES First 5 rows")
rides_df.show(5, truncate=False)

print("\nDRIVERS SCHEMA")
drivers_df.printSchema()

print("\nDRIVERS First 5 rows")
drivers_df.show(5, truncate=False)


# T3: COLUMN SELECTION
selected_rides_df = rides_df.select(
    "ride_id",
    "pickup_location",
    "dropoff_location",
    "fare_amount"
)

print("Selected columns: ride_id, pickup_location, dropoff_location, fare_amount")
selected_rides_df.show(5, truncate=False)


# T4: FILTERING RIDES
filtered_rides_df = rides_df.filter(
    (col("distance_miles") > 5.0) & (col("ride_type") == "premium")
)

print(f"Found {filtered_rides_df.count()} premium rides with distance > 5 miles")
filtered_rides_df.show(5, truncate=False)


# T5: ADDING A DERIVED COLUMN — FARE PER MILE
rides_with_fare_per_mile = rides_df.withColumn(
    "fare_per_mile",
    col("fare_amount") / col("distance_miles")
)

print("Added 'fare_per_mile' column")
rides_with_fare_per_mile.select(
    "ride_id", "distance_miles", "fare_amount", "fare_per_mile"
).show(5, truncate=False)


# T6: REMOVING COLUMNS
# Drop ride_type column
rides_without_type = rides_df.drop("ride_type")

print("Dropped 'ride_type' column:")
rides_without_type.show(5, truncate=False)


# T7: RENAMING COLUMNS
rides_renamed = rides_df.withColumnRenamed("pickup_location", "start_area") \
                        .withColumnRenamed("dropoff_location", "end_area")

print("Renamed: pickup_location to start_area, dropoff_location to end_area")
rides_renamed.select("ride_id", "start_area", "end_area").show(5, truncate=False)


# T8: AGGREGATION: TOTAL REVENUE BY RIDE TYPE
revenue_by_type = rides_df.groupBy("ride_type") \
    .agg(spark_sum("fare_amount").alias("total_revenue"))

print("Total revenue per ride type:")
revenue_by_type.show(truncate=False)


# T9: AGGREGATION: AVERAGE RATING PER DRIVER
avg_rating_per_driver = rides_df.groupBy("driver_id") \
    .agg(avg("rating").alias("avg_rating"))

print("Average rating per driver:")
avg_rating_per_driver.orderBy("driver_id").show(truncate=False)


# T10: JOIN: RIDES WITH DRIVER INFO
enriched_rides_df = rides_df.join(
    drivers_df,
    on="driver_id",
    how="inner"
)

print(f"Joined rides with drivers: {enriched_rides_df.count()} records")
enriched_rides_df.select(
    "ride_id", "driver_id", "driver_name", "vehicle_type",
    "pickup_location", "dropoff_location", "fare_amount"
).show(5, truncate=False)


# T11: SET OPERATIONS: COMBINE PEAK & OFF-PEAK RIDES
# peak rides
peak_rides_df = rides_df.filter(
    (col("ride_date") >= "2025-01-01") & (col("ride_date") < "2025-02-01")
)

# off-peak
off_peak_rides_df = rides_df.filter(
    (col("ride_date") >= "2025-02-01") & (col("ride_date") < "2025-03-01")
)

# Union the two DataFrames
combined_rides_df = peak_rides_df.union(off_peak_rides_df)
combined_rides_df.show(5, truncate=False)


# T12: SPARK SQL QUERIES
# Register rides DataFrame as a temporary SQL view
rides_df.createOrReplaceTempView("rides")

# Execute SQL query to find top 3 highest-fare rides
top_fares_sql = """
    SELECT ride_id, pickup_location, dropoff_location, fare_amount
    FROM rides
    ORDER BY fare_amount DESC
    LIMIT 3
"""

top_fares_df = spark.sql(top_fares_sql)

print("Top 3 highest-fare rides:")
top_fares_df.show(truncate=False)


# O1: MULTI-COLUMN SORTING
# Sort by fare_amount (descending), then by distance_miles (ascending)
sorted_rides_df = rides_df.orderBy(
    desc("fare_amount"),
    asc("distance_miles")
)

print("Rides sorted by fare_amount, then distance_miles:")
sorted_rides_df.select(
    "ride_id", "fare_amount", "distance_miles"
).show(5, truncate=False)


# O2: HANDLING NULLS
null_rating_count = rides_df.filter(isnull("rating")).count()
print(f"Rides with null rating: {null_rating_count}")

# Fill null ratings with 0.0
rides_filled_df = rides_df.fillna({"rating": 0.0})

print("Null ratings filled with 0.0:")
rides_filled_df.select("ride_id", "rating").show(10, truncate=False)


# O3: CONDITIONAL COLUMN — RIDE CATEGORY
# Add ride_category based on distance_miles
# short: < 3 miles, medium: 3-8 miles, long: > 8 miles
rides_with_category = rides_df.withColumn(
    "ride_category",
    when(col("distance_miles") < 3, "short")
    .when((col("distance_miles") >= 3) & (col("distance_miles") <= 8), "medium")
    .otherwise("long")
)

print("\nAdded 'ride_category' column:")
rides_with_category.select(
    "ride_id", "distance_miles", "ride_category"
).show(15, truncate=False)


# O4: WINDOW FUNCTION
window_spec = Window.partitionBy("driver_id").orderBy("ride_date")

# Calculate running total of fare_amount
rides_with_running_total = rides_df.withColumn(
    "running_revenue",
    spark_sum("fare_amount").over(window_spec)
)

print("Running revenue total for each driver:")
rides_with_running_total.select(
    "ride_id", "driver_id", "ride_date", "fare_amount", "running_revenue"
).orderBy("driver_id", "ride_date").show(10, truncate=False)


# O5: SAVING RESULTS
# Save enriched DataFrame as Parquet
parquet_path = f"{output_dir}/enriched_rides.parquet"
enriched_rides_df.write.mode("overwrite").parquet(parquet_path)

# Save aggregation results (revenue by type) as CSV
csv_path = f"{output_dir}/revenue_by_type.csv"
revenue_by_type.write.mode("overwrite") \
    .option("header", "true") \
    .csv(csv_path)

# Save average rating per driver as CSV
avg_rating_csv = f"{output_dir}/avg_rating_per_driver.csv"
avg_rating_per_driver.write.mode("overwrite") \
    .option("header", "true") \
    .csv(avg_rating_csv)

# Save rides with category as CSV
category_csv = f"{output_dir}/rides_with_category.csv"
rides_with_category.write.mode("overwrite") \
    .option("header", "true") \
    .csv(category_csv)

# Stop Spark session
spark.stop()
