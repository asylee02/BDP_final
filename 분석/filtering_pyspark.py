from pyspark.sql import SparkSession
from pyspark.sql.functions import lit  # 키워드 값을 열로 추가하기 위해 필요
import os

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Keyword Filtering with PySpark") \
    .getOrCreate()

# 파일 경로 설정
csv_file_path = 'hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/parsed_comments_output.csv'
keywords_file_path = 'hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/output/keywords_only.txt'
output_dir = 'hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/keyword_comments'

# HDFS에서 CSV 파일 읽기
df = spark.read.csv(csv_file_path, header=True)

# 스키마 확인
df.printSchema()

# HDFS에서 키워드 파일 읽기
keywords = spark.read.text(keywords_file_path).rdd.map(lambda row: row[0].strip("'")).collect()

# 각 키워드별로 처리
for keyword in keywords:
    # 제목에 키워드가 포함된 행 필터링
    filtered_df = df.filter(df['Title'].contains(keyword))

    # Keyword 열 추가 및 댓글과 값만 추출
    comments_df = filtered_df \
        .select("Comment_Text", "Comment_Value") \
        .withColumn("Keyword", lit(keyword))  # 새로운 열 추가

    # 열 순서 재배치 (Keyword, Comment_Text, Comment_Value)
    comments_df = comments_df.select("Keyword", "Comment_Text", "Comment_Value")

    # 결과를 단일 CSV 파일로 저장
    keyword_output_path = os.path.join(output_dir, f'{keyword}_comments.csv')
    comments_df.coalesce(1).write.csv(keyword_output_path, header=True, mode='overwrite')

print(f"Keyword-specific comment CSV files have been saved in {output_dir}")