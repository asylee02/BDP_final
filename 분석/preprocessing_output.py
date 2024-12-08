import re
from hdfs import InsecureClient

# HDFS 클라이언트 설정
hdfs_client = InsecureClient('http://sandbox-hdp.hortonworks.com:50070', user='maria_dev')

# HDFS 파일 경로 설정
input_file_path = '/user/maria_dev/BDP_final/output/part-00000'
output_file_path = '/user/maria_dev/BDP_final/output/keywords_only.txt'

# 파일에서 숫자를 제거하고 키워드만 추출하는 함수
def extract_keywords(lines):
    keywords = []
    for line in lines:
        # 숫자를 제거하고 공백을 트림
        cleaned_line = re.sub(r'\d+', '', line).strip()
        if cleaned_line:  # 빈 줄 제외
            keywords.append(cleaned_line)
    return keywords

# HDFS 파일 읽기
with hdfs_client.read(input_file_path, encoding='utf-8') as reader:
    lines = reader.readlines()

# 숫자를 제거하고 키워드 추출
keywords = extract_keywords(lines)

# HDFS에 처리된 파일 저장
with hdfs_client.write(output_file_path, encoding='utf-8') as writer:
    writer.write('\n'.join(keywords))

print(f"키워드가에 저장되었습니다.")