# from pyspark.sql import SparkSession

# # Creating a Spark Session
# spark = SparkSession.builder\
# .appName("First Demo")\
# .master("local[*]")\
# .getOrCreate()

# print("-"*50)
# print(f"App Name: {spark.sparkContext.appName}")
# print(f"Spark Version: {spark.version}")
# print(f"Master: {spark.sparkContext.master}")
# print("-"*50)





# print("-"*50)
# data = [("Seth", 25), ("Justin", 26), ("Juan", 34), ("Anas", 23)]
# df = spark.createDataFrame(data, ["name", "age"])
# df.show()
# print("Dataframe Schema")
# df.printSchema()
# print("-"*50)

# # Run SQL on the DataFrame
# df.createOrReplaceTempView("people")
# result = spark.sql("SELECT name, age FROM people WHERE age > 30")

# print("result", result.show())


# print("-"*50)
# print("Configuration Settings")
# # Access configuration
# print("Shuffle partitions: ", spark.conf.get("spark.sql.shuffle.partitions"))
# spark.conf.set("spark.sql.shuffle.partitions", 50)
# print("Shuffle NEW partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
# print("-"*50)



print("-"*50)
from pyspark.sql import SparkSession
# create sparksession
spark = SparkSession.builder \
    .appName("HybridApp") \
    .getOrCreate()


print("convert from RDD to DataFrame")
df.spark.read.csv("data.csv", header=True)

# For  RDD operations, access SparkContext
sc = spark.sparkContext
rdd = sc.parallelize([1,2,3])

# You can convert between RDD and DataFrame
df_from_rdd = rdd.map(lambda x: (x,)).toDF(["value"])
rdd_from_df = df.rdd



spark.stop()