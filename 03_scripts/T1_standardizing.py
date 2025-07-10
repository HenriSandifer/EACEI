import pandas as pd
import json
import os
import numpy as np

def get_mapping_for_year(year, mappings_config):
    """
    Selects the correct renaming dictionary from the config for a given year.
    This handles the cases where one mapping applies to a range of years.
    """
    year_str = str(year)
    
    # Check for year ranges first
    if 2013 <= year <= 2016:
        return mappings_config.get("2013_to_2016", {})
    if 2017 <= year <= 2018:
        return mappings_config.get("2017_to_2018", {})
    
    # Fallback to specific year
    return mappings_config.get(year_str, {})

def preprocess_and_normalize(file_path, year, mappings_config):
    """
    Reads a CSV, adds a year column, handles special cases (like grouping),
    and renames columns based on the JSON mapping.
    
    Args:
        file_path (str): The full path to the CSV file.
        year (int): The year of the data, extracted from the folder structure.
        mappings_config (dict): The 'yearly_mappings' part of the JSON config.

    Returns:
        pandas.DataFrame: A processed and normalized DataFrame.
    """
    try:
        # Some CSVs might have metadata in the first row, so we try to skip it.
        # The header is determined by finding the row with the most matching old column names.
        # This is a robust way to handle files where the header isn't on row 0.
        df = pd.read_csv(file_path, header=None)
        
        # Find the actual header row
        header_row_index = 0
        max_matches = 0
        
        # Get all possible old column names from the mappings for that year
        rename_map = get_mapping_for_year(year, mappings_config)
        all_old_names = list(rename_map.keys())

        for i, row in df.head().iterrows():
            matches = row.astype(str).isin(all_old_names).sum()
            if matches > max_matches:
                max_matches = matches
                header_row_index = i
        
        # Reread the CSV with the correct header row
        df = pd.read_csv(file_path, header=header_row_index)
        
    except Exception as e:
        print(f"  - Error reading {os.path.basename(file_path)}: {e}")
        return None

    # Add the year column at the beginning
    df.insert(0, 'year', year)

    # --- Step 1: Handle Column Grouping (Aggregation) ---
    # This is the most important custom step. It must happen BEFORE renaming.

    # For years before 2017, sum solid fuels
    if year < 2017:
        solid_fuels = ['Houille', 'Lignite - charbon pauvre', 'Coke de houille']
        existing_fuels = [col for col in solid_fuels if col in df.columns]
        if existing_fuels:
            df['Consommation de combustibles minéraux solides'] = df[existing_fuels].sum(axis=1, numeric_only=True)
            df = df.drop(columns=existing_fuels)

    # For 2010-2011, sum gas columns
    if year <= 2011:
        gas_fuels = ['Gaz naturel de réseau', 'Autres gaz de réseau']
        existing_gas = [col for col in gas_fuels if col in df.columns]
        if existing_gas:
            df['Consommation de gaz naturel et autres gaz'] = df[existing_gas].sum(axis=1, numeric_only=True)
            df = df.drop(columns=existing_gas)

    # --- Step 2: Apply the renaming based on the year ---
    rename_map = get_mapping_for_year(year, mappings_config)
    df = df.rename(columns=rename_map)
    
    # --- Step 3: Clean up columns that are all empty or just placeholders ---
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.dropna(axis=1, how='all')

    return df


def main():
    """
    Main function to orchestrate the file discovery and normalization process.
    """
    # --- Configuration ---
    # Define root directory for data and where to find the mapping config
    root_data_dir = r"..\data"
    config_path = r"..\dictionaries\T1_naming_convention.json"
    output_filename = 'eacei_T1_normalized_master.csv'

    # --- 1. Load the JSON mapping configuration ---
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        mappings = config['yearly_mappings']
        target_columns = config['2023_columns']
        print("Successfully loaded naming convention from JSON.")
    except FileNotFoundError:
        print(f"Error: The mapping file was not found at '{config_path}'")
        print("Please ensure 'T1_naming_convention.json' is in a 'dictionaries' folder.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not parse the JSON file at '{config_path}'. Please check its format.")
        return

    # --- 2. Discover and process files ---
    all_normalized_data = []
    print(f"\nStarting search in root directory: '{root_data_dir}'...")

    # os.walk will traverse the directory tree top-down
    for dirpath, dirnames, filenames in os.walk(root_data_dir):
        # We only care about directories named 'of_interest'
        if os.path.basename(dirpath) == 'of_interest':
            print(f"\nFound 'of_interest' folder: {dirpath}")
            
            # Extract the year from the parent directory path
            try:
                # e.g., 'data/2010/some/folder' -> '2010'
                year_str = dirpath.split(os.sep)[2] 
                year = int(year_str)
            except (IndexError, ValueError):
                print(f"  - Warning: Could not determine year from path '{dirpath}'. Skipping this folder.")
                continue

            # Process eligible files within this folder
            for filename in filenames:
                # Rule 1: Must be a CSV and contain 'T1'
                if filename.endswith('.csv') and 'T1' in filename:
                    file_path = os.path.join(dirpath, filename)
                    print(f"  - Processing: {filename} for year {year}")
                    
                    normalized_df = preprocess_and_normalize(file_path, year, mappings)
                    
                    if normalized_df is not None:
                        all_normalized_data.append(normalized_df)

    # --- 3. Combine all data into a single master DataFrame ---
    if not all_normalized_data:
        print("\nNo eligible 'T1' files were found and processed. The script will now exit.")
        return

    print("\nConcatenating all processed files into a master DataFrame...")
    # Use concat, which is more robust for this task
    master_df = pd.concat(all_normalized_data, ignore_index=True, sort=False)

    # --- 4. Final Cleanup and Standardization ---
    # Ensure all target columns exist, filling missing ones with NaN or 0
    for col in target_columns:
        if col not in master_df.columns:
            master_df[col] = np.nan # Use NaN for missing data to distinguish from actual zeros

    # Reorder columns to match the 2023 standard, keeping any extra columns at the end
    existing_target_cols = [col for col in target_columns if col in master_df.columns]
    other_cols = [col for col in master_df.columns if col not in existing_target_cols]
    master_df = master_df[existing_target_cols + other_cols]

    # --- 5. Save the final result ---
    try:
        master_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f"\nSuccess! Master file saved as '{output_filename}'")
        print(f"Final DataFrame shape: {master_df.shape}")
        print("Final columns:", master_df.columns.tolist())
    except Exception as e:
        print(f"\nError saving the final file: {e}")

if __name__ == '__main__':
    main()