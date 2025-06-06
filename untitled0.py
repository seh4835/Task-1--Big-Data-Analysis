from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, isnan, isnull, when

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Big Data Analysis with PySpark") \
    .getOrCreate()

# Step 2: Load the Dataset
# Note: Make sure the CSV file is in the working directory or provide the full path.
print("\n Loading dataset...")
df = spark.read.csv("Data_set 2.csv", header=True, inferSchema=True)

# Step 3: Preview the Data
print("\n Schema of the dataset:")
df.printSchema()

print("\n Preview of data:")
df.show(5)

# Step 4: Basic Information
total_rows = df.count()
print(f"\n Total records in the dataset: {total_rows}")

# Step 5: Check for Missing Values
print("\n Checking for missing or null values in each column...")
nulls = df.select([
    count(when(isnull(c) | isnan(c), c)).alias(c)
    for c in df.columns
])
nulls.show()

# Step 6: Descriptive Statistics
print("\n Summary statistics for numeric columns:")
df.describe().show()

# Step 7: Aggregation & Grouping Analysis (Edit as per your columns)
# Example: Finding top categories and their average values
if 'Category' in df.columns:
    print("\n Top Categories by Frequency:")
    df.groupBy("Category") \
        .agg(count("*").alias("Count")) \
        .orderBy(desc("Count")) \
        .show()

if 'Value' in df.columns and 'Category' in df.columns:
    print("\n Average Value per Category:")
    df.groupBy("Category") \
        .agg(avg("Value").alias("Average_Value")) \
        .orderBy(desc("Average_Value")) \
        .show()

# Step 8: Optional - Correlation Analysis
numeric_cols = [f.name for f in df.schema.fields if str(f.dataType) in ['DoubleType', 'IntegerType']]
if len(numeric_cols) >= 2:
    print("\n Correlation Analysis Between Numeric Columns:")
    for i in range(len(numeric_cols)):
        for j in range(i + 1, len(numeric_cols)):
            col1, col2 = numeric_cols[i], numeric_cols[j]
            corr_val = df.stat.corr(col1, col2)
            print(f"Correlation between {col1} and {col2}: {round(corr_val, 2)}")

# Step 9: Wrap Up
spark.stop()
print("\n Analysis complete. Spark session stopped.")
