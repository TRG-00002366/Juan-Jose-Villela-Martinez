import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,col,when, split, trim, regexp_replace, coalesce
)
os.system('clear')
# set up
spark = SparkSession.builder\
    .appName("Demo Column Management")\
    .master('local[*]')\
    .getOrCreate()

data = [
    (1, "  Alice Smith  ", "alice@company.com", 75000, None),
    (2, "  Bob Johnson  ", "bob.j@company.com", 65000, "NY"),
    (3, "  Charlie Brown  ", "charlie@company.com", 80000, "CA"),
    (4, "  Diana Prince  ", None, 70000, "TX")
 
]
# create dataframe from the data na use column names
df = spark.createDataFrame(data,['id', 'name', 'email', 'salary', 'state'])
# show data
df.show()

# # add a column withColumn
df.withColumn('country', lit('USA')).show()

# # add a new column as bonus which calculates and display 10 10% of Salary
df.withColumn('Bonus', col('salary') * 0.10).show()

# # Add a column with a value based on a value of another column
# # like if salary > 70,000 column value = high Earner
df.withColumn('High Earner', col('salary') > 70000).show()

# # Add a column based on value of another column -- conditional column
df.withColumn('Salary Tier',
    when(col('salary') < 65000, 'Entry')
    .when(col('salary') < 75000, 'Mid')
    .otherwise('Senior')
).show()

# STRING OPS
# clean the data to remove trailing spaces 
clean_df = df.withColumn('name', trim(col('name')))
# now apply split func
clean_df.withColumn('First_Name', split(col('name'), ' ')[0]).show()

# create a new column based on email, the new col replaces
# @company.com with @mycompany.com
df.withColumn('email',regexp_replace(col('email'), '@company\\.com$', '@mycompany.com')).show()

# Handle nulls -- coalesce -- used to handle null values
# replace NULL in email colum with vaule N/A
df.withColumn('email', coalesce(col('email'), lit('N/A'))).show()

# REANAME COLUMN
df.withColumnRenamed('name', 'Full Name').show()

# RENAME ALL COLUMN AT ONCE -- use toDF()
toDF() overwrites existing one
df.toDF('emp_id', 'emp_name', 'emp_email', 'salary', 'state').show()

# DROP / REMOVING COLUMNS
df.drop('name', 'email').show()
df.show()

# YOU CAN CHAIN YOUR OPERATIONS
df.withColumn().withColumn().withColumn()

# clean up
spark.stop()