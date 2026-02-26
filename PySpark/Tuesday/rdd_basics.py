from pyspark import SparkContext
# TASK 1: CREATE RDDS FROM DIFFERENT SOURCES

sc = SparkContext("local[*]", "RDDBBasics")
# # 1. Create RDD from a python list
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
print(f"Numbers: {numbers.collect()}")
print(f"Partitions: {numbers.getNumPartitions()}")

# # # 2. Create RDD with explicit partitions
partitioned_numbers = sc.parallelize([1,2,3,4,5,6], numSlices=3)
print(f"\nPartitioned Numbers: {partitioned_numbers.collect()}")
print(f"Partitions: {partitioned_numbers.getNumPartitions()}")

# # 3. Create RDD from a range
ranged_numbers = sc.parallelize(range(1,11))
print(f"\nRange of Numbers: {ranged_numbers.collect()}")

# # TASK 2: APPLY MAP() TRANSFORMATIONS
# # Given: numbers RDD [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# # Task A: Square each number
# # Expected: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
squared = numbers.map(lambda n: n**2)
print(f"\nSquared Numbers: {squared.collect()}")

# # # Task B: Convert to strings with prefix
# # # Expected: ["num_1", "num_2", "num_3", ...]
prefixed = numbers.map(lambda n: f"num_{n}")
print(f"\nPrefixed Numbers: {prefixed.collect()}")


# # TASK 3: APPLY FILTER() TRANSFORMATIONS

# # Task A: Keep only even numbers
# # Expected: [2, 4, 6, 8, 10]
evens = numbers.filter(lambda n: n%2 == 0)
print(f"\nEven Numbers: {evens.collect()}")

# # Task B: Keep numbers greater than 5
# # Expected: [6, 7, 8, 9, 10]
greater_than_5 = numbers.filter(lambda n: n > 5)
print(f"\nGreater than 5 nums: {greater_than_5.collect()}")

# # Task C: Combine - even AND greater than 5
# # Expected: [6, 8, 10]
combined = numbers.filter(lambda n: n%2 == 0 and n > 5)
print(f"\nCombined evens and > 5: {combined.collect()}")

# TASK 4: APPLY flatMap() TRANSFORMATION:

# Given sentences
sentences = sc.parallelize([
    "Hello World",
    "Apache Spark is Fast",
    "PySpark is Python plus Spark"
])

# # Task A: Split into words (use flatMap)
# # Expected: ["Hello", "World", "Apache", "Spark", ...]
words = sentences.flatMap(lambda s: s.split())
print(f"\nWords flatten: {words.collect()}")

# # Task B: Create pairs of (word, length)
# # Expected: [("Hello", 5), ("World", 5), ...]
word_lengths = sentences.flatMap(lambda s: s.split()).map(lambda s: (s, len(s)))
print(f"\nWord lengths: {word_lengths.collect()}")

# TASK 5: CHAIN TRANSFORMATIONS:
# Given: log entries
logs = sc.parallelize([
    "INFO: User logged in",
    "ERROR: Connection failed",
    "INFO: Data processed",
    "ERROR: Timeout occurred",
    "DEBUG: Cache hit"
])

# Pipeline: Extract only ERROR messages, convert to uppercase words
# 1. Filter to keep only ERROR lines
# 2. Split each line into words
# 3. Convert each word to uppercase
# Expected: ["ERROR:", "CONNECTION", "FAILED", "ERROR:", "TIMEOUT", "OCCURRED"]
error_words = logs\
    .filter(lambda line: "ERROR" in line)\
    .flatMap(lambda line: line.split())\
    .map(lambda word: word.upper())
print(f"\nError Words: {error_words.collect()}")

sc.stop()