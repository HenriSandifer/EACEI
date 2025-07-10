import os
import pymysql
import pandas as pd

# MySQL connection details
conn = pymysql.connect(
    host='localhost',
    user='testuser',
    password='testpass',
    database='my_project'
)
cursor = conn.cursor()

# Folder with CSVs
folder = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\Enquête annuelle sur les consommations d'énergie dans l'industrie\metadata_tables"

for filename in os.listdir(folder):
    if filename.endswith(".csv"):
        table_name = filename.replace(".csv", "")
        df = pd.read_csv(os.path.join(folder, filename))

        # Create table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                code VARCHAR(50),
                label TEXT
            );
        """)

        # Insert rows
        for _, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO {table_name} (code, label) VALUES (%s, %s)
            """, (str(row['code']), str(row['label'])))

        print(f"Imported {filename} into {table_name}")

conn.commit()
cursor.close()
conn.close()
