import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StringType, ArrayType

# Spark Session 생성
spark = SparkSession.builder \
    .appName("Process and Parse Comments Data") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/usr/bin/python3") \
    .config("spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON", "/usr/bin/python3") \
    .getOrCreate()

# HDFS 경로
input_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/processed_data.csv"
output_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/parsed_comments_output.csv"

# CSV 파일 읽기
df = spark.read.option("header", True).csv(input_path)

# JSON 데이터 스키마 정의
json_schema = ArrayType(StringType())

# 댓글 데이터 JSON 변환
parsed_df = df.withColumn("comments", from_json(col("Comments"), json_schema))

# 댓글 데이터 explode (개별 댓글로 분리)
exploded_df = parsed_df.select(
    col("Title"),  # Title을 포함한 데이터로 가정
    explode(col("comments")).alias("comment")
)

# 데이터 저장
exploded_df.write.option("header", True).mode("overwrite").csv(output_path)

print(f"Parsed comments saved to {output_path}")

# Spark 세션 종료
spark.stop()
