# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Keyword-based Comment Filtering") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020") \
    .getOrCreate()

# HDFS 경로 정의
keywords_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/output/part-00000"
input_csv_path = "hdfs:///user/maria_dev/BDP_final/data_corlling_2.csv"
output_path = "hdfs:///user/maria_dev/BDP_final/comment_output/"

# 1. 키워드 데이터 로드
keywords_df = spark.read.text(keywords_path)
keywords = [row['value'].split("\t")[0].strip().strip("'") for row in keywords_df.collect()]

# 2. CSV 파일 로드
data = spark.read.csv(input_csv_path, header=True, inferSchema=True)

# 3. 키워드 포함 여부를 확인하는 함수 정의
def contains_keywords(title):
    for keyword in keywords:
        if keyword in title:
            return keyword
    return None

# UDF 등록
contains_keywords_udf = udf(contains_keywords, StringType())

# 4. 키워드가 포함된 댓글 필터링 및 분류
filtered_data = data.withColumn("matched_keyword", contains_keywords_udf(col("title"))) \
                    .filter(col("matched_keyword").isNotNull())

# 5. 키워드별로 댓글 분류 후 저장
for keyword in keywords:
    keyword_filtered = filtered_data.filter(col("matched_keyword") == keyword)
    output_file_path = f"{output_path}{keyword}_comments.csv"
    keyword_filtered.write.csv(output_file_path, header=True, mode="overwrite")

print(f"Filtered comments saved to {output_path}")

# Spark 세션 종료
spark.stop()
