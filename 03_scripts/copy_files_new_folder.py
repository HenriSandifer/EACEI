import os
import shutil

def organize_and_copy_files(base_dir, target_base_dir):
    for year in range(2010, 2024):
        year_path = os.path.join(base_dir, str(year))

        if not os.path.isdir(year_path):
            print(f"  - Year directory does not exist: {year_path}")
            continue

        # Create the target directory if it doesn't exist
        target_year_path = os.path.join(target_base_dir, str(year))
        os.makedirs(target_year_path, exist_ok=True)

        # Walk through the year directory to find "step_3"
        for root, dirs, files in os.walk(year_path):
            if os.path.basename(root) == "step_3":
                for file in files:
                    if file.endswith(".csv"):
                        source_file_path = os.path.join(root, file)
                        target_file_path = os.path.join(target_year_path, file)

                        # Copy the file to the target directory
                        shutil.copy2(source_file_path, target_file_path)
                        print(f"Copied: {source_file_path} -> {target_file_path}")

# Set base and target directories
base_data_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw"
target_data_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\02_data_clean"

organize_and_copy_files(base_data_path, target_data_path)