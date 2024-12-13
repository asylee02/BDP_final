import pandas as pd
import subprocess

# 로컬 파일 경로
input_excel_path = "data_corlling_2.xlsx"
output_csv_path = "data_corlling_2.csv"

# HDFS 저장 경로
hdfs_csv_path = "hdfs:///user/maria_dev/BDP_final/data_corlling_2.csv"

# Excel 파일을 읽어와서 CSV로 저장
df = pd.read_excel(input_excel_path, engine="openpyxl")
df.to_csv(output_csv_path, index=False, encoding="utf-8")
print(f"{input_excel_path} 파일이 {output_csv_path}로 저장되었습니다.")

# HDFS에 CSV 파일 업로드
try:
    subprocess.run(["hdfs", "dfs", "-put", "-f", output_csv_path, hdfs_csv_path], check=True)
    print(f"{output_csv_path} 파일이 HDFS 경로 {hdfs_csv_path}에 업로드되었습니다.")
except subprocess.CalledProcessError as e:
    print(f"HDFS 업로드 중 오류 발생: {e}")
