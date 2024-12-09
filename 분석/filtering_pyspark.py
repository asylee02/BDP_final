from pyspark.sql import SparkSession
from pyspark.sql.functions import lit  # 키워드 값을 열로 추가하기 위해 필요
import os

# 서버에서 사용을 위해 정확한 Python 경로 설정
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.6'

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Keyword Filtering with PySpark") \
    .getOrCreate()

# 로컬 파일 경로 설정
# csv_file_path = 'hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/parsed_comments_output.csv'
# keywords_file_path = 'hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/output/keywords_only.txt'
# output_dir = 'hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/keyword_comments'

# 서버 파일 경로 설정
csv_file_path = 'hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/parsed_comments_output.csv/merged_output.csv'
keywords_file_path = 'hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/BDP_final/output/keywords_only.txt'
output_dir = 'hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/Result'

# HDFS에서 CSV 파일 읽기
df = spark.read.csv(csv_file_path, header=True)

# 스키마 확인
df.printSchema()

# HDFS에서 키워드 파일 읽기
keywords = spark.read.text(keywords_file_path).rdd.map(lambda row: row[0].strip("'")).collect()

# 서버용 Hadoop FileSystem 설정
from py4j.java_gateway import java_import
java_import(spark._jvm, 'org.apache.hadoop.fs.*')

hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jvm.java.net.URI.create(output_dir),
    hadoop_conf
)

# 순차적으로 파일 이름을 지정하기 위한 인덱스 초기화
file_index = 1

# 각 키워드별로 처리
for keyword in keywords:
    # 제목에 키워드가 포함된 행 필터링
    filtered_df = df.filter(df['Title'].contains(keyword))

    # Keyword 열 추가 및 필요한 열만 선택, Title 추가
    comments_df = filtered_df \
        .select("Title", "Comment_Text", "Comment_Value") \
        .withColumn("Keyword", lit(keyword)) \
        .select("Keyword", "Title", "Comment_Text", "Comment_Value")

    # 임시 디렉토리 경로 설정
    temp_dir = f'{output_dir}/temp_comments{file_index}'
    comments_df.coalesce(1).write.csv(temp_dir, header=True, mode='overwrite')
    
    # 임시 파일 이름 변경
    temp_path = spark._jvm.org.apache.hadoop.fs.Path(temp_dir)
    target_file_path = spark._jvm.org.apache.hadoop.fs.Path(f'{output_dir}/comments{file_index}.csv')

    # 디렉토리 내 파일 검색 및 이름 변경
    for file_status in fs.listStatus(temp_path):
        file_name = file_status.getPath().getName()
        if file_name.startswith("part"):
            fs.rename(file_status.getPath(), target_file_path)
            
    # 임시 디렉토리 삭제
    fs.delete(temp_path, True)

    # 파일 인덱스 증가
    file_index += 1

print(f"Keyword-specific comment CSV files have been saved in {output_dir}")