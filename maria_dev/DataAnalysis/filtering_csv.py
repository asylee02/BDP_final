from pyspark.sql import SparkSession

# SparkSession 생성
spark = SparkSession.builder \
    .appName("Filter Title and Comment Columns") \
    .getOrCreate()

# HDFS 경로 정의 (호스트 포함)
input_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/data_corlling_2.csv"
output_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/processed_data.csv"

try:
    # 데이터 읽기
    df = spark.read.csv(input_path, header=True)

    # 필요한 컬럼만 선택
    filtered_df = df.select("Index","Title", "Total Comments","Comments")

    # 결과를 저장
    filtered_df.write.csv(output_path, header=True, mode="overwrite")

    print(f"Processed data saved to {output_path}")
except Exception as e:
    print(f"An error occurred: {e}")
