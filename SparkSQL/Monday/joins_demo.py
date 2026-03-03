import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,col,when, split, trim, regexp_replace, coalesce, 
)

spark = SparkSession.builder\
    .appName("Demo Column Management")\
    .master('local[*]')\
    .getOrCreate()

os.system('clear')
# Create sample DataFrames
employees = spark.createDataFrame([
    (1, "Alice", 101),
    (2, "Bob", 102),
    (3, "Charlie", 103),
    (4, "Diana", None)  # No department
], ["emp_id", "emp_name", "dept_id"])
 
departments = spark.createDataFrame([
    (101, "Engineering", "Building A"),
    (102, "Marketing", "Building B"),
    (104, "Sales", "Building C")  # No employees
], ["dept_id", "dept_name", "location"])

# employees.show()
# departments.show()

#
# df1.join(df2, 'condition', type of join)

# INNER JOIN -- matching rows, most common DEFAULT
# inner_result = employees.join(
#     departments,
#     employees.dept_id == departments.dept_id,
#     'inner'
# )
# inner_result.show()


# OUTER JOIN -- Left -- Keeps all from left / full outer join
# inner_result = employees.join(
#     departments,
#     employees.dept_id == departments.dept_id,
#     'outer'
# )
# inner_result.show()

# RIGHT -- Keeps all from Right
# inner_result = employees.join(
#     departments,
#     employees.dept_id == departments.dept_id,
#     'right'
# )
# inner_result.show()

# ALL from both

# Cross - Cartesian product 
inner_result = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    'cross'
)
inner_result.show()

# can also do
cross_result = employees.crossJoin(departments)

# another example
hours = spark.createDataFrame(((i,) for i in range(12)), ['Hour'])
mins = spark.createDataFrame(((i,) for i in range(60)), ["Min"])
hours.crossJoin(mins).show(20)

# SEMI join
semi_result = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    'semi join'
)


# ANTI join












# Clean up
spark.stop()
