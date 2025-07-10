import pandas as pd
import json
import os
import numpy as np
from collections import defaultdict

def get_id_vars(df):
    """
    Dynamically identifies the identifier columns (e.g., NAF, REG code/libelle).
    Assumes the first two columns are the identifiers.
    """
    return df.columns[:2].tolist()

def process_pre_2020_file(df, year, mapping_config):
    """
    Processes files with multi-level headers (2010-2019).
    It aggregates data based on the mapping, then melts the DataFrame.
    """
    # --- Step 1: Clean up multi-level headers ---
    # The top level (products) has NaNs that need to be filled
    df.columns = pd.MultiIndex.from_tuples(df.columns)
    df.columns = df.columns.to_frame()
    df.columns.iloc[:, 0] = df.columns.iloc[:, 0].str.replace(r'Unnamed:.*', '', regex=True).str.strip()
    df.columns.iloc[:, 0] = df.columns.iloc[:, 0].fillna(method='ffill')
    df.columns = pd.MultiIndex.from_frame(df.columns)

    # --- Step 2: Pre-aggregate columns based on the mapping ---
    id_vars = get_id_vars(df)
    
    # Create a reverse mapping from target header to source (product, indicator) tuples
    aggregation_map = defaultdict(list)
    for product, indicators in mapping_config.items():
        for indicator, target_header in indicators.items():
            # Find the actual column in the DataFrame that matches the product/indicator
            # This handles cases where product names have units, etc.
            for col_product, col_indicator in df.columns:
                if product in col_product and indicator == col_indicator:
                    aggregation_map[target_header].append((col_product, col_indicator))

    # Perform the aggregation
    aggregated_df = df[id_vars].copy()
    for target_header, source_cols in aggregation_map.items():
        # Ensure source columns exist before trying to sum them
        existing_source_cols = [col for col in source_cols if col in df.columns]
        if existing_source_cols:
            # Convert to numeric, coercing errors, then sum
            aggregated_df[target_header] = df[existing_source_cols].apply(pd.to_numeric, errors='coerce').sum(axis=1)

    # --- Step 3: Melt the aggregated DataFrame ---
    melted_df = aggregated_df.melt(
        id_vars=id_vars,
        var_name='indicateur_produit_unite',
        value_name='valeur'
    )
    
    return melted_df


def process_post_2020_file(df, year, mapping_config):
    """
    Processes files with single-level headers (2020 and later).
    It renames columns to the 2023 standard, then melts the DataFrame.
    """
    id_vars = get_id_vars(df)
    
    # --- Step 1: Rename columns to the 2023 standard ---
    # Clean up newline characters from headers
    df = df.rename(columns=lambda c: c.replace('\n', ' ').strip())
    df = df.rename(columns=mapping_config)

    # --- Step 2: Melt the DataFrame ---
    melted_df = df.melt(
        id_vars=id_vars,
        var_name='indicateur_produit_unite',
        value_name='valeur'
    )
    
    return melted_df


def main():
    """
    Main function to orchestrate the T2 file discovery and normalization process.
    """
    # --- Configuration ---
    root_data_dir = 'data'
    config_path = os.path.join('dictionaries', 'T2_header_mapping.json')
    output_filename = 'naf_t2_master_long_format.csv'

    # --- 1. Load the JSON mapping configuration ---
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        mappings = config['mappings']
        target_headers = config['target_2023_headers']
        print("Successfully loaded T2 header mapping from JSON.")
    except FileNotFoundError:
        print(f"Error: Mapping file not found at '{config_path}'")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Could not parse JSON file at '{config_path}'. {e}")
        return

    # --- 2. Discover and process T2 files ---
    all_normalized_data = []
    print(f"\nStarting search in root directory: '{root_data_dir}'...")

    for dirpath, dirnames, filenames in os.walk(root_data_dir):
        if os.path.basename(dirpath) == 'of_interest':
            print(f"\nFound 'of_interest' folder: {dirpath}")
            
            try:
                year = int(dirpath.split(os.sep)[1])
            except (IndexError, ValueError):
                print(f"  - Warning: Could not determine year from path '{dirpath}'. Skipping.")
                continue

            for filename in filenames:
                if filename.endswith('.csv') and 'T2' in filename:
                    file_path = os.path.join(dirpath, filename)
                    print(f"  - Processing: {filename} for year {year}")

                    try:
                        # Determine which processing logic to use based on the year
                        if 2010 <= year < 2017:
                            # Years 2010-2011 and 2012-2016 have slightly different header styles
                            # but can be handled by the same logic if the mapping is correct.
                            # We need to find the correct header row first.
                            header_rows = [0,1] if year > 2011 else [0,1] # Adjust if needed
                            raw_df = pd.read_csv(file_path, header=header_rows)
                            mapping_key = '2012_2016_multi_level' if year >= 2012 else '2010_2011_multi_level'
                            processed_df = process_pre_2020_file(raw_df, year, mappings[mapping_key])
                        
                        elif 2017 <= year < 2020:
                            raw_df = pd.read_csv(file_path, header=[0,1])
                            processed_df = process_pre_2020_file(raw_df, year, mappings['2017_2019_multi_level'])

                        else: # 2020 and later
                            raw_df = pd.read_csv(file_path, header=0)
                            mapping_key = f"{year}_single_level"
                            processed_df = process_post_2020_file(raw_df, year, mappings[mapping_key])
                        
                        # Add the year column
                        processed_df.insert(0, 'year', year)
                        all_normalized_data.append(processed_df)
                        print(f"    ... Done.")

                    except Exception as e:
                        print(f"    ... Error processing {filename}: {e}")

    # --- 3. Combine, clean, and save the final master DataFrame ---
    if not all_normalized_data:
        print("\nNo eligible 'T2' files were found or processed. Exiting.")
        return

    print("\nConcatenating all processed files...")
    master_df = pd.concat(all_normalized_data, ignore_index=True)

    # Clean up and standardize final DataFrame
    master_df = master_df.dropna(subset=['valeur'])
    master_df = master_df[master_df['indicateur_produit_unite'].isin(target_headers)]
    
    # Convert 'valeur' to numeric, coercing errors
    master_df['valeur'] = pd.to_numeric(master_df['valeur'], errors='coerce')

    try:
        master_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f"\nSuccess! Master file saved as '{output_filename}'")
        print(f"Final DataFrame shape: {master_df.shape}")
        print("Sample of the final data:")
        print(master_df.head())
    except Exception as e:
        print(f"\nError saving the final file: {e}")

if __name__ == '__main__':
    main()
