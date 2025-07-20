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

try:
    with open(r'C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\04_dictionaries\T2_naming_convention.json', 'r', encoding='utf-8') as file:
        header_map = json.load(file)['header_map']
except FileNotFoundError:
    print("Error: 'T2_naming_convention.json' not found.")
    exit()

# --- Logging Setup ---

def setup_logger():
    """Sets up a logger to append warnings to a file in the root 07_logs directory."""
    # Use a relative path to make the script portable
    log_dir = os.path.join(os.path.dirname(__file__), '..', '07_logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file_path = os.path.join(log_dir, 'data_cleaning_T2.log')
    
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
    else:
        print("  - Warning: Could not determine file type. ID columns not renamed.")
    
    return df

# --- Helper Function to Parse Year Ranges ---

def is_year_in_range(year_to_check, year_spec):
    """
    Checks if a given year falls within a year specification.
    The spec can be a string range "YYYY-YYYY" or a list of years [YYYY, YYYY].
    """
    if isinstance(year_spec, str) and '-' in year_spec:
        start, end = map(int, year_spec.split('-'))
        return start <= year_to_check <= end
    elif isinstance(year_spec, list):
        return year_to_check in year_spec
    return False

# --- The Main Function ---

def step2_rename_and_add_indicators(df, file_path, header_map):
    """
    Renames T2 indicator columns for both multi-index and single-header files,
    drops obsolete columns, and adds any missing standard columns.
    """
    print("Step 2: Renaming and adding indicator columns...")

    try:
        year = int(os.path.basename(file_path).split('_')[0])
    except (ValueError, IndexError):
        print("  - Warning: Could not determine year from filename. Aborting step 2.")
        return df

    # --- Build the appropriate reverse map based on the year ---
    reverse_map = {}
    if year < 2020:
        # --- Logic for Multi-Index Files (pre-2020) ---
        for target_header, sources in header_map.items():
            for source in sources:
                if source['type'] == 'multi-index' and is_year_in_range(year, source['years']):
                    # Key is a tuple: (product_string, indicator_string)
                    # This will be used to match against the DataFrame's MultiIndex columns.
                    key = (source['product_contains'], source['indicator'])
                    reverse_map[key] = target_header
        
        # Clean the DataFrame's multi-index headers
        # 1. Forward-fill the product names on the top level
        df.columns = df.columns.to_frame().ffill().to_records(index=False).tolist()
        df.columns = pd.MultiIndex.from_tuples(df.columns)

        # 2. Create a new list of single-level headers
        new_columns = []
        for product, indicator in df.columns:
            # Turn [code, code] or [label, label] tuples into just a code or label single header 
            if product == indicator:
                code_label = product
                new_columns.append(code_label)
                continue
            found_match = False
            for (map_prod, map_ind), target in reverse_map.items():
                # Use 'in' for flexible matching (e.g., "Houille" in "Houille (en milliers de tonnes)")
                if map_prod in product and map_ind == indicator:
                    new_columns.append(target)
                    found_match = True
                    break
            if not found_match:
                # If no match, keep the original tuple to identify it later for dropping
                new_columns.append((product, indicator))
        
        df.columns = new_columns

    else:
        # --- Logic for Single-Header Files (2020 and later) ---
        for target_header, sources in header_map.items():
            for source in sources:
                if source['type'] == 'single-header' and is_year_in_range(year, source['years']):
                    # Key is a simple string
                    reverse_map[source['header']] = target_header
        
        # Clean column names by removing newlines before renaming
        df = df.rename(columns=lambda c: c.replace('\n', ' ').strip())
        df = df.rename(columns=reverse_map)

    # --- Drop obsolete and unwanted columns, preserve "code" and "label" columns ---
    df_ids = df.iloc[:, :2]
    df_subset = df.iloc[:, 2:]
    cols_to_drop = []
    for col in df_subset.columns:
        # Check for original multi-index tuples or string names
        if isinstance(col, tuple) or 'stock' in str(col).lower() or 'établissements' in str(col).lower():
            cols_to_drop.append(col)
    
    # Specific columns to remove based on your request
    if year < 2020:
        # These columns won't exist after the renaming, so we check for their original tuple form
        cols_to_drop.extend([
            ('Total des énergies', 'Quantités achetées'),
            ('Total des énergies', 'Consommation'),
            ('Total des énergies', 'Prix moyen')
        ])

    df_subset = df_subset.drop(columns=cols_to_drop, errors='ignore')
    df = pd.concat([df_ids, df_subset], axis=1)

    # --- Add any missing standard 2023 columns ---
    all_target_headers = list(header_map.keys())
    for header in all_target_headers:
        if header not in df.columns:
            df[header] = pd.NA # Use pandas NA for proper null handling

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

    desired_order = [cell1, cell2] + list(header_map.keys())
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

    # Define the aggregation dictionary
    target_cell = id_column_name
    
    data_cols = df.columns.drop([id_column_name, label_column_name])
    agg_dict = {col: agg_with_log for col in data_cols}
    agg_dict[label_column_name] = 'first' # Keep the first label

    df_agg = df.groupby(id_column_name).agg(agg_dict).reset_index()
    
    # Reorder columns to match the original dataframe's order
    ordered_cols = [col for col in df.columns if col in df_agg.columns]

    return df_agg[ordered_cols]

def save_csv(df, file_path):

    # Save the processed csv file
    output_dir = os.path.join(os.path.dirname(os.path.dirname(file_path)), "step_3")
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.basename(file_path).replace('.csv', '')
    output_path = os.path.join(output_dir, f"{base_name}.csv")

    try:
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\nSuccess! Column-wise cleaned file saved to:\n{output_path}")
    except Exception as e:
        print(f"\nError saving the new file: {e}")

    return

# --- Main Orchestrator ---

def process_t2_files(base_dir, script_name):
    """ Processes all T2 files in the specified base directory."""
    for year in range(2010, 2024):
        year_path = os.path.join(base_dir, str(year))
        if not os.path.isdir(year_path):
            continue

        for root, dirs, files in os.walk(year_path):
            if os.path.basename(root) == "step_2":
                for file in files:
                    if file.endswith(".csv") and "T2" in file:
                        file_path = os.path.join(root, file)

                        LOGGER.info(f"--- Processing file: {os.path.basename(file_path)} ---")

                        year = int(os.path.basename(file_path).split('_')[0])

                        # Filtering by year in case of multi index file
                        if year < 2020:
                            try:
                                df = pd.read_csv(file_path, header=[0, 1])
                            except Exception as e:
                                print(f"Error reading CSV file: {e}")
                                return
                        else:
                            try:
                                df = pd.read_csv(file_path)
                            except Exception as e:
                                print(f"Error reading CSV file: {e}")
                                return

                        # Run pipeline steps sequentially
                        print("Starting the T2 file processing pipeline...")
                        df_step1 = step1_rename_id_headers(df, file_path)
                        df_step2 = step2_rename_and_add_indicators(df_step1, file_path, header_map)
                        df_step3 = step3_aggregate_columns(df_step2, script_name, os.path.basename(file_path))
                        df_step4 = step4_aggregate_rows(df_step3, script_name, os.path.basename(file_path))
                        save_csv(df_step4, file_path)

# Run batch cleaning loop
if __name__ == '__main__':
    # Get the name of the current script dynamically
    current_script_name = os.path.basename(__file__)
    base_data_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw"
    process_t2_files(base_data_path, current_script_name)