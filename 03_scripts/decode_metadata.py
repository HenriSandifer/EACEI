import pandas as pd
import os

# --- Set these paths yourself ---
input_file = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\Enquête annuelle sur les consommations d'énergie dans l'industrie\DS_EACEI_2023_CSV_FR\DS_EACEI_2023_metadata.csv"
output_dir = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\Enquête annuelle sur les consommations d'énergie dans l'industrie\metadata_tables"

# Create output folder if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Read the metadata file
df = pd.read_csv(input_file, sep=';', header=None, quotechar='"', engine='python')
df.columns = ['category_code', 'category_label', 'code', 'label']

# Save one CSV per metadata category
for category in df['category_code'].unique():
    subset = df[df['category_code'] == category][['code', 'label']]
    filename = f"{category.lower().replace(' ', '_')}_labels.csv"
    output_path = os.path.join(output_dir, filename)
    subset.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Saved: {output_path}")
