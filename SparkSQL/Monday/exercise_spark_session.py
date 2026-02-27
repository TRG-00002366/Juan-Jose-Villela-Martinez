"""
Exercise: SparkSession Setup and Configuration
===============================================
Week 2, Monday

Complete the TODOs below to practice creating and configuring SparkSession objects.
"""

from pyspark.sql import SparkSession

# =============================================================================
# TASK 1: Basic SparkSession Creation
# =============================================================================

#  1a: Create a SparkSession with:
#   - App name: "MyFirstSparkSQLApp"
#   - Master: "local[*]"
# HINT: Use SparkSession.builder.appName(...).master(...).getOrCreate()

spark = SparkSession.builder\
    .appName("MyFirstSparkSQLApp")\
    .master("local[*]")\
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

#  1b: Print the following information:
#   - Spark version
#   - Application ID
#   - Default parallelism
# HINT: Access these via spark.version, spark.sparkContext.applicationId, etc.
print(f"Spark Version: {spark.version}")
print(f"App Name: {spark.sparkContext.appName}")

print("=== Task 1: Basic SparkSession ===")
# Your print statements here


#  1c: Create a simple DataFrame with 3 columns and 5 rows
# to verify your session works

data = [
    (1, "Alice", 34, "Engineering", 75000.0),
    (2, "Bob", 45, "Marketing", 65000.0),
    (3, "Charlie", 29, "Engineering", 80000.0),
    (4, "Diana", 31, "Sales", 55000.0),
    (5, "Eve", 38, "Marketing", 70000.0)
    ]  # Replace with sample data

columns = ["id", "name", "age", "department", "salary"]  # Replace with column names
# df = spark.createDataFrame(data, columns)  # Create DataFrame

# Show the DataFrame
# df.show()


# =============================================================================
# TASK 2: Configuration Exploration
# =============================================================================

print("\n=== Task 2: Configuration ===")

# 2a: Print the value of spark.sql.shuffle.partitions
# HINT: Use spark.conf.get("spark.sql.shuffle.partitions")

print(f"Shuffle partitions: {spark.conf.get("spark.sql.shuffle.partitions")}")  # Complete this


# 2b: Print at least 3 other configuration values
# Some options: spark.driver.memory, spark.executor.memory, spark.sql.adaptive.enabled
print(f"Spark App Name:{spark.sparkContext.appName}")
print(f"Spark Master:{spark.sparkContext.master}")

# 2c: Try changing spark.sql.shuffle.partitions at runtime
# Does it work? Add a comment explaining what happens.

# Your code here
current = spark.conf.get("spark.sql.shuffle.partitions")
#changing
spark.conf.set("spark.sql.shuffle.partitions", "20")
after = spark.conf.get("spark.sql.shuffle.partitions")

print(f"Shuffle partitions before: {current}")
print(f"Shuffle partitions after: {after}")
# After checking out the print statements, it looks like shuffle partitions can
# be changed during runtime

# =============================================================================
# TASK 3: getOrCreate() Behavior
# =============================================================================

print("\n=== Task 3: getOrCreate() Behavior ===")

# 3a: Create another reference using getOrCreate with a DIFFERENT app name
  # Replace with SparkSession.builder.appName("DifferentName").getOrCreate()
spark2 = SparkSession.builder\
    .appName("DifferentName")\
    .getOrCreate()


# 3b: Check which app name is actually used
print(f"spark app name: {spark.sparkContext.appName}")
print(f"spark2 app name: {spark2.sparkContext.appName}")


# 3c: Are spark and spark2 the same object? Check with 'is' operator
print(f"spark the same object as spark2: {spark is spark2}")


# 3d: EXPLAIN IN A COMMENT: Why does getOrCreate() behave this way?
# Your explanation:
# They are the same object because, creating a spark object is memory intensive/heavy
# therefore, to mitigate this, spark reuses the already created object, and treats it
# as a single instance.


# =============================================================================
# TASK 4: Session Cleanup
# =============================================================================

print("\n=== Task 4: Cleanup ===")

# 4a: Stop the SparkSession properly
# HINT: Use spark.stop()
# 4b: Verify the session has stopped
# HINT: Check spark.sparkContext._jsc.sc().isStopped() before stopping
print("Sparked stopped?: ", spark.sparkContext._jsc.sc().isStopped())
spark.stop()

try:
    spark.sparkContext._jsc.sc().isStopped()
    print("Spark stopped")
except AttributeError:
    print("Spark is already stopped")

# =============================================================================
# STRETCH GOALS (Optional)
# =============================================================================

# Stretch 1: Create a helper function that builds a SparkSession with your
# preferred default configurations

def create_my_spark_session(app_name, shuffle_partitions=100):
    """
    Creates a SparkSession with custom defaults.
    
    Args:
        app_name: Name of the Spark application
        shuffle_partitions: Number of shuffle partitions (default: 100)
    
    Returns:
        SparkSession object
    """
    # : Implement this function
    return SparkSession.builder\
        .appName(app_name)\
        .master("local[*]")\
        .config("spark.sql.shuffle.partitions", shuffle_partitions)\
        .getOrCreate()


# Stretch 2: Enable Hive support
# HINT: Use .enableHiveSupport() in the builder chain
# Note: This may fail if Hive is not configured - that's okay!
try:
    spark_with_hive = SparkSession.builder\
        .appName("SparkWithHive")\
        .master("local[*]")\
        .enalbeHiveSupport()\
        .getOrCreate()
    
    print("Hive support enabled successfully!")
    print(f"Hive metastore: {spark_with_hive.conf.get('spark.sql.warehouse.dir', 'default')}")
except:
    print("Hive support failed, expected if Hive is not configured")


# Stretch 3: List all configuration options
# HINT: spark.sparkContext.getConf().getAll() returns all settings
print(spark.sparkContext.getConf().getAll())