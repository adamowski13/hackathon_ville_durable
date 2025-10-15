import pandas as pd

# Replace 'your_file.csv' with the path to your CSV file
df = pd.read_csv('data/consommation-quotidienne-brute-regionale.csv',sep=';')

print("Columns in the CSV file:")
print(df.columns.tolist())

print("\nFirst few rows of the DataFrame:")
print(df.head().to_string())

print("\nUnique regions in the dataset:")
print(df['Région'].unique())

df_idf = df[df['Région'] == 'Île-de-France']
df_idf['Date'] = pd.to_datetime(df_idf['Date'], errors='coerce')
df_idf = df_idf[df_idf['Date'].dt.year > 2018]

df_idf.to_csv('data/consommation-idf.csv', index=False, sep=';', encoding='utf-8',mode='w')
print(len(df_idf))