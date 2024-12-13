from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("output-based Comment Filtering") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020") \
    .getOrCreate()
    output_txt_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/mapreduce/output.txt"
    input_csv_path = "../preprocessing/data_complete.csv"
    output_path = "hdfs:///user/maria_dev/mapreduce/"


