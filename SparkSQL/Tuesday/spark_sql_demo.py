from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder.appName("SQL Demo").master('local[*]').getOrCreate()

# Create sample data
employees = spark.createDataFrame([
    (1, "Alice", 1, 75000), (2, "Bob", 2, 65000), (3, "Charlie", 1, 80000),
    (4, "Diana", 3, 55000), (5, "Eve", 2, 70000)
], ["emp_id", "name", "dept_id", "salary"])
 
departments = spark.createDataFrame([
    (1, "Engineering"), (2, "Marketing"), (3, "Sales")
], ["dept_id", "dept_name"])

#Register as Temp View
employees.createOrReplaceTempView("employees")
departments.createOrReplaceTempView("departments")

# 1. SIMPLE QUERY
# spark.sql("""
#     SELECT * 
#     FROM employees 
#     WHERE salary > 60000""").show()

# 2. Group by and Aggregate
# spark.sql("""
#     SELECT dept_id, COUNT(*) AS count, AVG(salary) AS avg_sal
#     FROM employees
#     GROUP BY dept_id
# """).show()

# 3. Jooin -- emp_name, dept_name, salary
# spark.sql("""
#     SELECT name, d.dept_name, salary
#     FROM employees
#     JOIN departments d
#     WHERE d.dept_id == employees.dept_id
# """).show()

# 4. combine both python and SQL
sql_dept = spark.sql("""
    SELECT name, salary
    FROM employees
    WHERE dept_id=1
""")
sql_dept.withColumn("Bonus", col("salary")*0.15).show()