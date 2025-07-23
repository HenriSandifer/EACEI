import os
import json
import pandas as pd

# Paths
INPUT_MAPPING = r"C:\Users\Henri\Documents\Data_Science\Portfolio\SQL\Datasets\EACEI\04_dictionaries\id_mapping.json"
INPUT_CLEAN_DIR = r"C:\Users\Henri\Documents\Data_Science\Portfolio\SQL\Datasets\EACEI\02_data_clean"
OUTPUT_DIR = r"C:\Users\Henri\Documents\Data_Science\Portfolio\SQL\Datasets\EACEI\05_database_final"

os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"\nOutput directory: {OUTPUT_DIR}")
print(f"Output directory exists: {os.path.exists(OUTPUT_DIR)}")

# Load mapping JSON
def load_mapping(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

mapping = load_mapping(INPUT_MAPPING)

# Build lookups
# Indicator label -> ind_id
label_to_ind = {}
for set_name in ("T1", "T2", "T3", "T4"):
    for rec in mapping.get(set_name, []):
        label = rec[f"{set_name}_label"]
        label_to_ind[label] = rec["ind_id"]
        print(f"Mapped {label} -> {rec['ind_id']}")

# Category code -> category id
naf_lookup = {rec["naf_code"]: rec["naf_id"] for rec in mapping.get("NAF", [])}
reg_lookup = {rec["reg_code"]: rec["reg_id"] for rec in mapping.get("REG", [])}
teff_lookup = {rec["teff_code"]: rec["teff_id"] for rec in mapping.get("TEFF", [])}

print("\nLookup tables:")
print(f"NAF codes: {list(naf_lookup.keys())}")
print(f"REG codes: {list(reg_lookup.keys())}")
print(f"TEFF codes: {list(teff_lookup.keys())}")
print(f"Number of indicators: {len(label_to_ind)}")

# Year lookup (year -> year_id)
year_lookup = {year: year for year in range(2010, 2024)}

# Accumulators for fact rows
facts_naf = []
facts_reg = []
facts_teff = []

# Process each cleaned CSV
for year in range(2010, 2024):
    year_dir = os.path.join(INPUT_CLEAN_DIR, str(year))
    if not os.path.isdir(year_dir):
        continue

    for fname in os.listdir(year_dir):
        if not fname.endswith('.csv'):
            continue
        parts = fname[:-4].split('_')  # remove .csv
        # Expect: ['2010', 'NAF', 'T2']
        _, category, indicator_set = parts
        filepath = os.path.join(year_dir, fname)
        print(f"\nReading file: {fname}")
        df = pd.read_csv(filepath)
        print(f"Columns found: {df.columns.tolist()}")
        print(f"Number of rows: {len(df)}")
        
        # Debug column names
        if 'électricité' in ''.join(df.columns):
            print("Column names containing 'électricité':")
            for col in df.columns:
                if 'électricité' in col:
                    print(f"'{col}' (length: {len(col)})")
                    print(f"Hex representation: {' '.join(hex(ord(c)) for c in col)}")
        
        year_id = year_lookup[year]
        
        # Select appropriate lookup and fact accumulator
        if category == 'NAF':
            cat_lookup = naf_lookup
            fact_rows = facts_naf
            cat_key = 'naf_code'
            fact_columns = ['naf_id', 'ind_id', 'year_id', 'value']
        elif category == 'REG':
            cat_lookup = reg_lookup
            fact_rows = facts_reg
            cat_key = 'reg_code'
            fact_columns = ['reg_id', 'ind_id', 'year_id', 'value']
        elif category == 'TEFF':
            cat_lookup = teff_lookup
            fact_rows = facts_teff
            cat_key = 'teff_code'
            fact_columns = ['teff_id', 'ind_id', 'year_id', 'value']
        else:
            continue

        # Iterate over each row
        print(f"\nProcessing rows for {fname}")
        for idx, row in df.iterrows():
            print(f"Processing row {idx}, {cat_key}: {row[cat_key]}")
            cat_code = row[cat_key]
            cat_id = cat_lookup.get(cat_code)
            if cat_id is None:
                print(f"Warning: Unknown {cat_key}: {cat_code}")
                continue  # unknown code

            # For each indicator column (skip first 2 columns)
            print(f"\nProcessing indicators for row with {cat_key}={cat_code}")
            for label, value in row.iloc[2:].items():
                print(f"Checking indicator: '{label}'")
                ind_id = label_to_ind.get(label)
                if ind_id is None:
                    print(f"WARNING: Unknown indicator label: '{label}'")
                    print("Available labels:", list(label_to_ind.keys())[:5], "...")  # Show first 5 labels
                    continue  # unknown indicator

                # Print when we find electricity consumption indicators
                if "électricité" in label:
                    print(f"Found electricity indicator: {label} -> {ind_id}")

                # Append fact: [cat_id, ind_id, year_id, value]
                fact_rows.append([cat_id, ind_id, year_id, value])

# Save fact tables
naf_df = pd.DataFrame(facts_naf, columns=['naf_id','ind_id','year_id','value'])
print(f"\nNAF facts count: {len(naf_df)}")
print("Sample of NAF facts:")
print(naf_df.head())
output_path = os.path.join(OUTPUT_DIR, 'faits_naf.csv')
naf_df.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f'Written faits_naf.csv to {output_path}')
print(f'File exists: {os.path.exists(output_path)}')
print(f'File size: {os.path.getsize(output_path) if os.path.exists(output_path) else "file not found"} bytes')

reg_df = pd.DataFrame(facts_reg, columns=['reg_id','ind_id','year_id','value'])
output_path = os.path.join(OUTPUT_DIR, 'faits_reg.csv')
reg_df.to_csv(output_path, index=False, encoding='utf-8-sig')
print('Written faits_reg.csv')

pd.DataFrame(facts_teff, columns=['teff_id','ind_id','year_id','value']) \
    .to_csv(os.path.join(OUTPUT_DIR, 'faits_teff.csv'), index=False, encoding='utf-8-sig')
print('Written faits_teff.csv')

if __name__ == '__main__':
    print('Finished building all fact tables.')
