from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, udf
from datetime import datetime

# Vietnamese text to non special characters
vietnamese_char_map = {
    'à': 'a', 'ả': 'a', 'ã': 'a', 'á': 'a', 'ạ': 'a', 'â': 'a', 'ấ': 'a', 'ầ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
    'ă': 'a', 'ắ': 'a', 'ằ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
    'è': 'e', 'ẻ': 'e', 'ẽ': 'e', 'é': 'e', 'ẹ': 'e', 'ê': 'e', 'ế': 'e', 'ề': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
    'ì': 'i', 'ỉ': 'i', 'ĩ': 'i', 'í': 'i', 'ị': 'i',
    'ò': 'o', 'ỏ': 'o', 'õ': 'o', 'ó': 'o', 'ọ': 'o', 'ô': 'o', 'ố': 'o', 'ồ': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
    'ơ': 'o', 'ớ': 'o', 'ờ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
    'ù': 'u', 'ủ': 'u', 'ũ': 'u', 'ú': 'u', 'ụ': 'u', 'ư': 'u', 'ứ': 'u', 'ừ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
    'ỳ': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ý': 'y', 'ỵ': 'y',
    'đ': 'd',
    # Uppercase mappings
    'À': 'A', 'Ả': 'A', 'Ã': 'A', 'Á': 'A', 'Ạ': 'A', 'Â': 'A', 'Ấ': 'A', 'Ầ': 'A', 'Ẩ': 'A', 'Ẫ': 'A', 'Ậ': 'A',
    'Ă': 'A', 'Ắ': 'A', 'Ằ': 'A', 'Ẳ': 'A', 'Ẵ': 'A', 'Ặ': 'A',
    'È': 'E', 'Ẻ': 'E', 'Ẽ': 'E', 'É': 'E', 'Ẹ': 'E', 'Ê': 'E', 'Ế': 'E', 'Ề': 'E', 'Ể': 'E', 'Ễ': 'E', 'Ệ': 'E',
    'Ì': 'I', 'Ỉ': 'I', 'Ĩ': 'I', 'Í': 'I', 'Ị': 'I',
    'Ò': 'O', 'Ỏ': 'O', 'Õ': 'O', 'Ó': 'O', 'Ọ': 'O', 'Ô': 'O', 'Ố': 'O', 'Ồ': 'O', 'Ổ': 'O', 'Ỗ': 'O', 'Ộ': 'O',
    'Ơ': 'O', 'Ớ': 'O', 'Ờ': 'O', 'Ở': 'O', 'Ỡ': 'O', 'Ợ': 'O',
    'Ù': 'U', 'Ủ': 'U', 'Ũ': 'U', 'Ú': 'U', 'Ụ': 'U', 'Ư': 'U', 'Ứ': 'U', 'Ừ': 'U', 'Ử': 'U', 'Ữ': 'U', 'Ự': 'U',
    'Ỳ': 'Y', 'Ỷ': 'Y', 'Ỹ': 'Y', 'Ý': 'Y', 'Ỵ': 'Y',
    'Đ': 'D',
}

# String Transformation Functions 
def to_new_date(date):
    original_date = datetime.strptime(date, "%m/%d/%Y")
    return original_date.strftime("%Y%m%d")

def to_clean_name(name):
    for accented, non_accented in vietnamese_char_map.items():
        name = name.replace(accented, non_accented)
    name = name.replace(" ", "_")

    return name

to_new_date_udf = udf(to_new_date, StringType())

# Define the schema
schema = StructType([
    StructField("student_code", IntegerType(), True),
    StructField("activity", StringType(), True),
    StructField("numberOfFile", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

dssv_schema = StructType([
    StructField("student_code", IntegerType(), True),
    StructField("student_name", StringType(), True),
])

student_schema = StructType([
    StructField("date", StringType(), True),
    StructField("student_code", IntegerType(), True),
    StructField("student_name", StringType(), True),
    StructField("activity", StringType(), True),
    StructField("totalFile", IntegerType(), True),
])

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Read Data From HDFS
activity_df = spark.read.schema(schema).parquet("hdfs://namenode/raw_zone/fact/activity")
    
student_list = spark.read.schema(dssv_schema).csv("hdfs://namenode/raw_zone/fact/student_list")

# Process Data
activity_df = activity_df.withColumn("date", to_new_date_udf(col("timestamp"))) \
    .join(student_list, "student_code") \
    .select("date", "student_code", "student_name", "activity", "numberOfFile")

activity_df = activity_df.groupBy("date", "student_code", "student_name", "activity") \
    .agg({"numberOfFile": "sum"}) \
    .withColumnRenamed("sum(numberOfFile)", "totalFile") \
    .orderBy("date")

activity_df = activity_df.select("date", "student_code", "student_name", "activity", "totalFile")
# Write Processed Data
student_names = student_list.select("student_name").rdd.flatMap(lambda x: x).collect()

for student_name in student_names:
    partition_df = activity_df.filter(activity_df.student_name == student_name)
    file_name = to_clean_name(student_name)
    partition_df.coalesce(1).write.mode("overwrite").option("header", "false").csv(f"hdfs://namenode/output_zone/{file_name}.csv")

# More efficient but wrong naming format
# activity_df.write.partitionBy("student_name").mode("overwrite").option("header", "false").format("csv").save("hdfs://namenode/output_zone/student")

