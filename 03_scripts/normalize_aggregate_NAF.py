import pandas as pd
import numpy as np
import os

# --- Configuration ---
# This dictionary defines how to group the old NAF codes into the new ones.
AGGREGATION_CONFIG = {
    'B07T09': {
        'target_label': 'Industries extractives (à l’exception de l’extraction de houille, de lignite et d’hydrocarbures)',
        'source_codes': ['07', '08', '09', '08 - 09', '07 - 08 - 09']
    },
    'C10T12': {
        'target_label': 'Fabrication de denrées alimentaires, de boissons et de produits à base de tabac',
        'source_codes': ['10', '11', '12', '10 - 11 - 12']
    }
}

# This dictionary defines simple label updates for existing codes.
LABEL_UPDATES = {
    '38': 'Collecte, traitement et élimination des déchets ; récupération'
}


def normalize_naf_codes_in_file(file_path):
    """
    Reads a single EACEI NAF file, aggregates rows based on the evolving
    NAF code classification, and saves the result to a new file.

    Args:
        file_path (str): The full path to the input CSV file.
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found at '{file_path}'")
        return

    print(f"--- Processing file: {os.path.basename(file_path)} ---")

    try:
        # Read the CSV, correctly identifying the two header rows.
        # This creates a MultiIndex for the columns.
        df = pd.read_csv(file_path, header=[0, 1])
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Identify the column names for the NAF code and label.
    # They will be tuples because of the MultiIndex, e.g., ('ENERGIES', 'INDICATEUR')
    code_col_tuple = df.columns[0]
    label_col_tuple = df.columns[1]

    # For reliable matching, convert the NAF code column to string type and trim whitespace.
    df[code_col_tuple] = df[code_col_tuple].astype(str).str.strip()

    # This copy will hold the rows that are not part of any aggregation.
    df_to_keep = df.copy()
    new_aggregated_rows = []

    print("Aggregating NAF codes...")
    # --- Perform Aggregations ---
    for target_code, config in AGGREGATION_CONFIG.items():
        source_codes = config['source_codes']
        target_label = config['target_label']

        # Find all rows in the original DataFrame that match the source codes.
        rows_to_aggregate = df[df[code_col_tuple].isin(source_codes)]

        if not rows_to_aggregate.empty:
            print(f"  - Found {len(rows_to_aggregate)} rows for codes {source_codes}. Grouping into '{target_code}'.")

            # Isolate only the numeric columns for summation.
            # This safely ignores the code and label columns.
            numeric_cols = rows_to_aggregate.select_dtypes(include=np.number).columns
            summed_values = rows_to_aggregate[numeric_cols].sum()

            # Create a new row as a pandas Series, which can hold the new data.
            # It's important that this Series has the same (multi-level) index as the DataFrame's columns.
            new_row = pd.Series(index=df.columns, dtype='object')
            new_row[code_col_tuple] = target_code
            new_row[label_col_tuple] = target_label
            
            # Fill in the summed values into the new row.
            new_row.update(summed_values)
            
            new_aggregated_rows.append(new_row)

            # From our "to_keep" DataFrame, remove the original rows that we just aggregated.
            df_to_keep = df_to_keep[~df_to_keep[code_col_tuple].isin(source_codes)]
        else:
            print(f"  - No rows found for codes {source_codes} in this file.")

    # --- Combine and Finalize ---
    # Convert the list of new rows into a DataFrame.
    aggregated_df = pd.DataFrame(new_aggregated_rows)

    # Concatenate the original, untouched rows with our new, aggregated rows.
    final_df = pd.concat([df_to_keep, aggregated_df], ignore_index=True)

    print("Updating labels...")
    # --- Perform Label Updates ---
    for code, new_label in LABEL_UPDATES.items():
        # Find the row with the specified code and update its label.
        if code in final_df[code_col_tuple].values:
            final_df.loc[final_df[code_col_tuple] == code, label_col_tuple] = new_label
            print(f"  - Updated label for code '{code}'.")

    # Sort the final DataFrame by the NAF code for consistency.
    final_df = final_df.sort_values(by=code_col_tuple).reset_index(drop=True)

    # --- Save the Result ---
    # Create a new filename for the cleaned output file.
    output_path = file_path.replace('.csv', '_naf_normalized.csv')
    try:
        final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\nSuccess! Normalized file saved to:\n{output_path}")
    except Exception as e:
        print(f"\nError saving the new file: {e}")

    return final_df


if __name__ == '__main__':
    # --- How to use the script ---
    # 1. Make sure this script is in the same main folder as your 'data' and 'dictionaries' folders.
    # 2. Replace the file path below with the path to the file you want to process.
    
    # Example using the 2012 file you provided.
    # Note: The 'r' before the string makes it a "raw" string, which helps with backslashes in Windows paths.
    example_file_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\data\2012\dd_irecoeacei12\of_interest\2012_NAF_T2_cleaned.csv" # Or a full path like "C:\\Users\\YourName\\...\\2012_NAF_T2_cleaned.csv"
    
    # Run the normalization function.
    normalized_dataframe = normalize_naf_codes_in_file(example_file_path)

    if normalized_dataframe is not None:
        print("\n--- Displaying a sample of the normalized data ---")
        # Display the relevant columns to verify the changes.
        id_cols = normalized_dataframe.columns[:2]
        # Find a few numeric columns to show the summed data.
        value_cols = [col for col in normalized_dataframe.columns if 'Consommation' in col[1]][:2]
        print(normalized_dataframe[id_cols.tolist() + value_cols])

