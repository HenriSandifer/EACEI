import os

# This script will delete all files ending with "_rows.csv" in the "of_interest" directories
# for each year from 2010 to 2023.

def delete_rows_files(base_dir):
    for year in range(2010, 2024):
        year_path = os.path.join(base_dir, str(year))
        if not os.path.isdir(year_path):
            continue

        # Walk through all directories within the year directory
        for root, dirs, files in os.walk(year_path):
            # Check if the current directory is named "of_interest"
            if os.path.basename(root) == "of_interest":
                # Walk through all directories within the "of_interest" directory
                for sub_root, sub_dirs, sub_files in os.walk(root):
                    for file in sub_files:
                        if file.endswith("_columns.csv"):
                            file_path = os.path.join(sub_root, file)
                            os.remove(file_path)
                            print(f"Deleted file: {file_path}")

# Example usage
base_data_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw"
delete_rows_files(base_data_path)
