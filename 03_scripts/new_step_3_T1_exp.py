import pandas as pd
import os
import re
import logging
from datetime import datetime

# --- Configuration Dictionaries ---

# For renaming the first two columns based on file type
ID_HEADER_MAP = {
    "NAF": {"ID": "naf_code", "NAF": "naf_label"},
    "REG": {"ID": "reg_code", "REG": "reg_label"},
    "TEFF": {"ID": "teff_code", "TEFF": "teff_label"},
}

# For standardizing T1 indicator column headers
T1_INDICATOR_MAP = {
    "Consommation de combustibles minéraux solides": [
        "Combustibles minéraux solides (houille, lignite, charbon pauvre, coke de houille)",
        "Combustibles minéraux solides (houille, lignite - charbon pauvre, coke de houille)",
        "Houille", "Lignite - charbon pauvre", "Coke de houille"
    ],
    "Consommation de gaz naturel et autres gaz": [
        "Quantités de gaz naturel et autres gaz consommées", "Gaz naturel et autres gaz",
        "Gaz de réseau", "Gaz naturel de réseau", "Autres gaz de réseau"
    ],
    "Consommation de biogaz et biométhane": ["Biogaz et biométhane"],
    "Consommation de coke de pétrole": ["Coke de pétrole"],
    "Consommation de butane-propane": ["Butane propane"],
    "Consommation de fioul lourd": ["Fioul lourd"],
    "Consommation de fioul domestique": ["Quantités de fioul domestique consommées", "Fioul domestique"],
    "Consommation de gazole non routier": ["Quantités de gazole non routier consommées", "Gazole non routier"],
    "Consommation de combustibles usuels": ["Total combustibles usuels"],
    "Consommation d’autres produits pétroliers": ["Autres produits pétroliers"],
    "Consommation de liqueur noire": ["Liqueurs noires"],
    "Consommation de bois et sous-produit du bois": ["Bois et sous-produit du bois"],
    "Consommation d’hydrogène": ["Quantités d’hydrogène consommées", "Hydrogene"],
    "Consommation de combustibles spéciaux renouvelables": ["Combustibles spéciaux renouvelables"],
    "Consommation de combustibles spéciaux non renouvelables": ["Combustibles spéciaux non renouvelables"],
    "Consommation autres combustibles": ["Total autres combustibles"],
    "Vapeur consommée": ["Achats de vapeur"],
    "Consommation d’électricité": ["Électricité", "Autoconsommation d’électricité", "Achats d’électricité"],
    "Total brut de la consommation": ["Total brut de la consommation (en milliers de TEP)", "Total brut"],
    "Vapeur vendue": ["Ventes de vapeur", "Vapeur vendue"],
    "Consommation de combustibles pour la production d’électricité": ["Consommation pour production d’électricité"],
    "Total net de la consommation": ["Total net de la consommation (en milliers de TEP)", "Total net"]
}

# --- Logging Setup ---

def setup_logger():
    """Sets up a logger to append warnings to a file in the root 07_logs directory."""
    # Define the logs directory relative to the project root
    log_dir = r"..\07_logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file_path = os.path.join(log_dir, 'data_cleaning_T1.log')
    
    logger = logging.getLogger('DataCleaningLogger')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        # Use mode 'a' to append to the log file instead of 'w' to overwrite
        handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger

LOGGER = setup_logger()

# --- Helper Function for Aggregation with Logging ---

def sum_with_logging(series, file_name, row_identifier, column_name, df):
    """
    Custom aggregation function that sums a pandas Series,
    but logs a warning if 's', 'so', 'ns', or null values are present.
    """
    suppressed_values = ['s', 'so', 'ns']
    series_cleaned = series.replace(suppressed_values, pd.NA)
    target_cell = df.loc[row_identifier, df.columns[0]]
    if series_cleaned.isnull().any():
        log_message = (
            f"Script: step_3_T1.py | File: {file_name} | "
            f"ColumnAggregationWarning: column-wise sum (across rows) for column '{column_name}' for category '{target_cell}' "
            "included a suppressed or null value (s, so, ns, or null)."
        )
        LOGGER.info(log_message)
        
    numeric_series = pd.to_numeric(series_cleaned, errors='coerce').fillna(0)
    return numeric_series.sum()

# --- Main Processing Steps ---

def step1_rename_id_headers(df, file_path):
    """Identifies file type and renames the first two columns."""
    print("Step 1: Renaming identifier headers...")
    file_name = os.path.basename(file_path)
    file_type = None
    if "NAF" in file_name:
        file_type = "NAF"
    elif "REG" in file_name:
        file_type = "REG"
    elif "TEFF" in file_name:
        file_type = "TEFF"

    if file_type:
        rename_dict = ID_HEADER_MAP[file_type]
        df = df.rename(columns=rename_dict)
        print(f"  - Detected file type: {file_type}. Renamed ID columns.")
    else:
        print("  - Warning: Could not determine file type. ID columns not renamed.")
    
    return df

def step2_rename_and_add_indicators(df): 
    """Renames existing indicator columns and adds missing ones."""
    print("Step 2: Renaming and adding indicator columns...")
    df.columns = [col.replace("'", "’") if isinstance(col, str) else col for col in df.columns]
    reverse_map = {old_name: new_name for new_name, old_names in T1_INDICATOR_MAP.items() for old_name in old_names}
    df = df.rename(columns=reverse_map)

    all_target_headers = list(T1_INDICATOR_MAP.keys()) + ["Nombre d’établissements"]
    for header in all_target_headers:
        if header not in df.columns:
            print(f"  - Adding missing column: '{header}'")
            df[header] = pd.NA

    return df

def step3_aggregate_columns(df, file_path):
    """Aggregates columns with the same name, summing their values, then reorders columns."""
    print("Step 3: Aggregating duplicate columns...")
    cell1, cell2 = df.columns[0], df.columns[1]
    print(f"df head is : {df.head()}")

    df_agg = df.groupby(by=df.columns, axis=1).apply(
        lambda g: g.apply(
            lambda row: sum_with_logging(row, os.path.basename(file_path), row.name, g.columns[0], df),
            axis=1
        ) if g.shape[1] > 1 else g.iloc[:, 0]
    )
    
    desired_order = [cell1, cell2, "Nombre d’établissements"] + list(T1_INDICATOR_MAP.keys())
    ordered_columns = [col for col in desired_order if col in df_agg.columns]
    return df_agg.loc[:, ordered_columns]

def step4_aggregate_rows(df, file_path): 
    """Aggregates rows based on the primary identifier code, with logging."""
    print("Step 4: Aggregating duplicate rows...")
    id_column_name = df.columns[0]
    label_column_name = df.columns[1]
    data_cols = df.columns[2:]
    suppressed_values = ['s', 'so', 'ns']

    # --- Pre-aggregation logging for rows ---
    groups = df.groupby(id_column_name)
    for group_name, group_df in groups:
        if len(group_df) > 1: # Only log if there's an actual aggregation happening
            for col in data_cols:
                if col in group_df:
                    series_cleaned = group_df[col].replace(suppressed_values, pd.NA)
                    if series_cleaned.isnull().any():
                        log_message = (
                            f"Script: step_3_T1.py | File: {os.path.basename(file_path)} | "
                            f"RowAggregationWarning: rows-wise sum (across rows) for new column '{col}' in category '{group_name}' "
                            "included a suppressed or null value (s, so, ns, or null)."
                        )
                        LOGGER.info(log_message)

    # --- Perform the aggregation ---
    # First, clean all data columns by replacing suppressed values and converting to numeric
    for col in data_cols:
        if col in df:
            df[col] = pd.to_numeric(df[col].replace(suppressed_values, pd.NA), errors='coerce').fillna(0)

    agg_dict = {col: 'sum' for col in data_cols if col in df.columns}
    agg_dict[label_column_name] = 'first'
    
    df_agg = df.groupby(id_column_name, as_index=False).agg(agg_dict)
    
    # Reorder columns to match the original dataframe's order
    ordered_cols = [col for col in df.columns if col in df_agg.columns]
    return df_agg[ordered_cols]

# --- Main Orchestrator ---

def process_t1_file(file_path):
    """Runs the complete cleaning and transformation pipeline for a single T1 file."""
    if not os.path.exists(file_path):
        print(f"Error: File not found at '{file_path}'")
        return

    LOGGER.info(f"--- Processing file: {os.path.basename(file_path)} ---")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    df_step1 = step1_rename_id_headers(df, file_path)
    df_step2 = step2_rename_and_add_indicators(df_step1)
    df_step3 = step3_aggregate_columns(df_step2, file_path)
    df_step4 = step4_aggregate_rows(df_step3, file_path)
    
    output_dir = os.path.dirname(file_path)
    base_name = os.path.basename(file_path).replace('.csv', '')
    output_path = os.path.join(output_dir, f"{base_name}_columns.csv")

    try:
        df_step4.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\nSuccess! Column-wise cleaned file saved to:\n{output_path}")
    except Exception as e:
        print(f"\nError saving the new file: {e}")

    return df_step4

if __name__ == '__main__':
    example_file = r'C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw\2012\dd_irecoeacei12\of_interest\step_2\2012_TEFF_T1.csv'
    final_dataframe = process_t1_file(example_file)

    if final_dataframe is not None:
        print("\n--- Displaying a sample of the final processed DataFrame ---")
        print(final_dataframe.head())
