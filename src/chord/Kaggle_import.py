import pandas as pd

input_file_path = r'C:\Users\batma\DHT\data_movies_clean.xlsx'

df = pd.read_excel(input_file_path)

print(df.head())