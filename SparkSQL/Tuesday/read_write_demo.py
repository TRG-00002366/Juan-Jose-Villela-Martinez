from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col

# Create Spark Session
spark=SparkSession.builder.appName("file Read Write Demo").getOrCreate()

employee_schema=StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
])

# create DF and read CSV
df = spark.read.csv(
    "./PySparkSQL/Tuesday/employees.csv",
    header=True,
    schema=employee_schema
)

# show data
df.show()

# Apply a Transform
df_sal_gt_65=df.filter(col("salary")>65000)
df_sal_gt_65.show()

#write to an output file
# df_sal_get_65.write.mode("overwrite").json("output/sal_gt_65")