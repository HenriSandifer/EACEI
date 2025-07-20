import os
import shutil

# This script organizes loose CSV files into an "original" subfolder within each "of_interest" directory.

def organize_loose_files(base_dir):
    for year in range(2010, 2024):
        year_path = os.path.join(base_dir, str(year))
        if not os.path.isdir(year_path):
            continue

        for root, dirs, files in os.walk(year_path):
            if os.path.basename(root) == "of_interest":
                original_dir = os.path.join(root, "original")
                os.makedirs(original_dir, exist_ok=True)

                for file in files:
                    file_path = os.path.join(root, file)

                    # Skip files already in a subfolder
                    if os.path.isfile(file_path) and file.endswith(".csv"):
                        destination = os.path.join(original_dir, file)
                        shutil.move(file_path, destination)
                        print(f"Moved: {file_path} -> {destination}")

# Set your base directory here
base_data_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw"
organize_loose_files(base_data_path)