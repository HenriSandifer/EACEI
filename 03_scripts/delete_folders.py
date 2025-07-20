import os
import shutil

# This script will delete the "step_1" folders that were created in the
# "original" directories for each year from 2010 to 2023.

def delete_removed_rows_folders(base_dir):
    for year in range(2010, 2024):
        year_path = os.path.join(base_dir, str(year))
        if not os.path.isdir(year_path):
            continue

        for root, dirs, files in os.walk(year_path):
            if os.path.basename(root) == "original":
                removed_rows_path = os.path.join(root, "step_1")
                if os.path.isdir(removed_rows_path):
                    shutil.rmtree(removed_rows_path)
                    print(f"Deleted folder: {removed_rows_path}")

# Example usage:
base_data_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw"
delete_removed_rows_folders(base_data_path)

