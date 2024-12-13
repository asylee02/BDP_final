from pyspark.sql import SparkSession

# SparkSession 생성
spark = SparkSession.builder \
    .appName("Filter Comments and Sort by Likes") \
    .getOrCreate()

# HDFS 경로 정의
input_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/parsed_comments_output.csv/merged_output.csv"
output_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/pig_result/top_filtered_comments"


# 데이터 로드 (CSV)
df = spark.read.csv(input_path, header=True, inferSchema=True)

# 데이터 필터링: Total Comments >= 300
filtered_df = df.filter(df["Total Comments"] >= 300)

# 좋아요 수 기준으로 정렬
sorted_df = filtered_df.orderBy(df["Comment_Value"].desc())

# 상위 30개 추출
top_30_comments = sorted_df.limit(100)

# 결과 저장
top_30_comments.write.csv(output_path, header=True, mode="overwrite")

print(f"Top 30 comments saved to {output_path}")
