from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDActions")

numbers = sc.parallelize([10, 5, 8, 3, 15, 12, 7, 20, 1, 9])

# TASK 1: BASIC RETRIEVAL ACTIONS

# Task A: collect() - Get all elements
all_nums = numbers.collect()
print(f"All numbers: {all_nums}")

# Task B: count() - Count elements
count = numbers.count()
print(f"Count: {count}")

# Task C: first() - Get first element
first = numbers.first()
print(f"First: {first}")

# Task D: take(n) - Get first n elements
first_three = numbers.take(3)
print(f"First 3: {first_three}")

# Task E: top(n) - Get largest n elements
top_three = numbers.top(3)
print(f"Top 3: {top_three}")

# Task F: takeOrdered(n) - Get smallest n elements
smallest_three = numbers.takeOrdered(3)
print(f"Smallest 3: {smallest_three}")


# TASK 2: AGGREAGATION ACTIONS

# Task A: reduce() - Sum all numbers
total = numbers.reduce(lambda a,b: a + b)
print(f"Sum: {total}")

# Task B: reduce() - Find maximum
maximum = numbers.reduce(lambda a,b: a if a > b else b )
print(f"Max: {maximum}")

# Task C: reduce() - Find minimum
minimum = numbers.reduce(lambda a, b: a if a < b else b)
print(f"Min: {minimum}")

# Task D: fold() - Sum with zero value
folded_sum = numbers.fold(0, lambda a,b: a + b)
print(f"Folded sum: {folded_sum}")

# TASK 3: countByValue()

# Given: colors with duplicates
colors = sc.parallelize(["red", "blue", "red", "green", "blue", "red", "yellow"])

# Count occurrences of each color
color_counts = colors.countByValue()
print(f"Color counts: {dict(color_counts)}")

# Expected: {'red': 3, 'blue': 2, 'green': 1, 'yellow': 1}




sc.stop()