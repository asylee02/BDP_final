import pandas as pd

# 엑셀 파일 경로
input_excel_path = "data_corlling_2.xlsx"
output_csv_path = "data_corlling_2.csv"

# Excel -> CSV 변환
df = pd.read_excel(input_excel_path, engine="openpyxl")
df.to_csv(output_csv_path, index=False, encoding="utf-8")

