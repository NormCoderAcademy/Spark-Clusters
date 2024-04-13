from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ExcelAnalysis").getOrCreate()

# Read Excel file into a DataFrame
excel_file_path = "path/to/your/excel/file.xlsx"
df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .load(excel_file_path)

# Show the first few rows of the DataFrame
df.show()

# Perform basic data analysis (you can customize this part)
print(f"Number of rows: {df.count()}")
print(f"Column names: {', '.join(df.columns)}")
print("Summary statistics:")
df.describe().show()

# You can add more analysis steps here (e.g., aggregations, filtering, etc.)

# Stop the Spark session
spark.stop()
