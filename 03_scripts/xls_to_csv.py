import os
import pandas as pd

def convert_excel_to_csv(root_dir):
    # Loop through all directories and subdirectories
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            # Check if the file is an Excel file
            if file.endswith('.xls') or file.endswith('.xlsx'):
                file_path = os.path.join(subdir, file)

                # Read the Excel file
                xls = pd.ExcelFile(file_path)

                # Loop through each sheet in the Excel file
                for sheet_name in xls.sheet_names:
                    # Read the sheet into a DataFrame
                    df = pd.read_excel(xls, sheet_name=sheet_name)

                    # Create the output CSV file path
                    csv_filename = f"{file[:-5]}_{sheet_name}.csv"
                    csv_path = os.path.join(subdir, csv_filename)

                    # Save the DataFrame to a CSV file
                    df.to_csv(csv_path, index=False)
                    print(f"Converted {file_path} (sheet: {sheet_name}) to {csv_path}")

# Specify the root directory where your folders are located
root_directory = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\data"
convert_excel_to_csv(root_directory)
