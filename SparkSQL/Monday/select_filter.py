import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,col,when, split, trim, regexp_replace, coalesce, 
)
os.system('clear')

spark = SparkSession.builder\
    .appName("Demo Column Management")\
    .master('local[*]')\
    .getOrCreate()

# Sample data
data = [
    (1, "Alice", 34, "Engineering", 75000, "NY"),
    (2, "Bob", 45, "Marketing", 65000, "CA"),
    (3, "Charlie", 29, "Engineering", 80000, "NY"),
    (4, "Diana", 31, None, 55000, "TX"),
    (5, "Eve", 38, "Marketing", None, "CA")
]

df = spark.createDataFrame(data, ['id', 'name', 'age', 'department', 'salary', 'state'])
# df.show()

# VIEW DATA BASED ON column(s)
# df.select('name', 'salary').show()

# VIEW DATA BASED ON column(s) using col method
# df.select(col('name'), col('salary')).show()

# VIEW DATA BASED ON column(s) using col , df attribute df.age, [] notation -> df[id]
# df.select(df['id'], df.name, col('age'), 'salary').show()

# SQL EXPRESSIONs selectExpr (A SELECT expression)
# df.selectExpr(
#     'name', 'age',
#     'salary * 0.10 AS Bonus'
# ).show()

# FILTERING -- filter the rows
# by default all rows are select, so need to specify the WHERE condition
# WHERE == filter
# df.filter(col('age') > 31).show()

# COMPLEX CONDITIONS
# Filter based on 2 columns AND - OR
# df.filter((col('age') > 30) & (col('salary') >=60000)).show()

# df.filter( col('state').isin(['NY', 'CA']) ).show()

# df.filter( col('age').between(30,40) ).show()

# df.filter( col('name').contains('ar') ).show()

# df.filter(col('name').like('A%')).show()

# df.filter(col('department').isNotNull()).show()
