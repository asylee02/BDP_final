import pandas as pd
input_excel_path ="crolling_data_20.xlsx"
output_csv_path = "data_corlling_2.csv"
df = pd.read_excel(input_excel_path, engine="openpyxl")
df.to_csv(output_csv_path, index=False, encoding="utf-8")
