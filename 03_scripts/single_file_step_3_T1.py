import pandas as pd
import os
import logging
import json

# --- Configuration Dictionaries ---

# For renaming the first two columns based on file type
ID_HEADER_MAP = {
    "NAF": {"ID": "naf_code", "NAF": "naf_label"},
    "REG": {"ID": "reg_code", "REG": "reg_label"},
    "TEFF": {"ID": "teff_code", "TEFF": "teff_label"},
}

# For standardizing T1 indicator column headers

try:
    with open(r'C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\04_dictionaries\T1_naming_convention.json', 'r', encoding='utf-8') as file:
        header_map = json.load(file)['header_map']
except FileNotFoundError:
    print("Error: 'T1_naming_convention.json' not found.")
    exit()

# --- Logging Setup ---

def setup_logger():
    """Sets up a logger to append warnings to a file in the root 07_logs directory."""
    # Use a relative path to make the script portable
    log_dir = os.path.join(os.path.dirname(__file__), '..', '07_logs')
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

def sum_with_logging(series, script_name, file_name, aggregation_type, group_id, axis):
    """
    Custom aggregation function that sums a pandas Series, but logs a warning
    if suppressed ('s', 'so', 'ns') or null values are present.
    If all values are null/suppressed, returns null.
    """
    suppressed_values = ['s', 'so', 'ns']
    # Create a copy to avoid SettingWithCopyWarning
    series_cleaned = series.copy().replace(suppressed_values, pd.NA)
    
    # Check if all values in the series are null after cleaning
    if series_cleaned.isnull().all():
        return pd.NA # Return null if all values were null/suppressed
        
    # Log if any value was null/suppressed, but not all of them
    if series_cleaned.isnull().any():
        log_message = (
            f"Script: {script_name} | File: {file_name} | "
            f"{aggregation_type}Warning: Sum across axis '{axis}' for group '{group_id}' "
            "included a suppressed or null value (s, so, ns, or null)."
        )
        LOGGER.info(log_message)
        
    # Convert to numeric, coercing errors and filling remaining NaNs with 0 for summation
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
    reverse_map = {old_name: new_name for new_name, old_names in header_map.items() for old_name in old_names}
    df = df.rename(columns=reverse_map)

    all_target_headers = list(header_map.keys()) + ["Nombre d’établissements"]
    for header in all_target_headers:
        if header not in df.columns:
            print(f"  - Adding missing column: '{header}'")
            df[header] = pd.NA

    return df

def step3_aggregate_columns(df, script_name, file_name):
    """Aggregates columns with the same name, summing their values, then reorders columns."""
    print("Step 3: Aggregating duplicate columns...")
    cell1, cell2 = df.columns[0], df.columns[1]

    df_agg = df.groupby(by=df.columns, axis=1).apply(
        lambda g: g.apply(
            lambda row: sum_with_logging(
                row, script_name, file_name, "ColumnAggregation",
                df.loc[row.name, cell1], # Use the row's ID as the group identifier
                g.columns[0]
            ),
            axis=1
                        
        ) if g.shape[1] > 1 else g.iloc[:, 0]
    )


    desired_order = [cell1, cell2, "Nombre d’établissements"] + list(header_map.keys())
    ordered_columns = [col for col in desired_order if col in df_agg.columns]
    return df_agg.loc[:, ordered_columns]

def step4_aggregate_rows(df, script_name, file_name): 
    """Aggregates rows based on the primary identifier code, with logging."""
    print("Step 4: Aggregating duplicate rows...")
    id_column_name = df.columns[0]
    label_column_name = df.columns[1]
    
    # Define a custom aggregation function to use with .agg()
    def agg_with_log(series):
        # The name of the group (e.g., 'B07T09') is the name of the series
        group_id = series.name        
        row_header = df.loc[series.index[0], id_column_name]
        return sum_with_logging(series, script_name, file_name, "RowAggregation", group_id, row_header)
    
    data_cols = df.columns.drop([id_column_name, label_column_name])
    agg_dict = {col: agg_with_log for col in data_cols}
    agg_dict[label_column_name] = 'first' # Keep the first label
    
    df_agg = df.groupby(id_column_name).agg(agg_dict).reset_index()
    
    # Reorder columns to match the original dataframe's order
    ordered_cols = [col for col in df.columns if col in df_agg.columns]
    return df_agg[ordered_cols]

# --- Main Orchestrator ---

def process_t1_file(file_path, script_name):
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
    
    # Run pipeline steps sequentially
    df_step1 = step1_rename_id_headers(df, file_path)
    df_step2 = step2_rename_and_add_indicators(df_step1)
    df_step3 = step3_aggregate_columns(df_step2, script_name, os.path.basename(file_path))
    df_step4 = step4_aggregate_rows(df_step3, script_name, os.path.basename(file_path))
    
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
    # Get the name of the current script dynamically
    current_script_name = os.path.basename(__file__)
    
    example_file = r'C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw\2010\dd_irecoeacei10_excel\of_interest\step_2\2010_REG_T1.csv'
    
    # Pass the script name to the main processing function
    final_dataframe = process_t1_file(example_file, current_script_name)

    if final_dataframe is not None:
        print("\n--- Displaying a sample of the final processed DataFrame ---")
        print(final_dataframe.head())
