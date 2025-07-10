import os
import shutil

def organize_excel_files(root_dir):
    # Loop through each year directory
    for year in range(2013, 2024):
        year_dir = os.path.join(root_dir, str(year))
        if os.path.isdir(year_dir):
            # Walk through all subdirectories within the year directory
            for subdir, _, files in os.walk(year_dir):
                # Create the 'excel_files' subdirectory within the current subdirectory
                excel_files_dir = os.path.join(subdir, 'excel_files')
                os.makedirs(excel_files_dir, exist_ok=True)

                # Loop through all files in the current subdirectory
                for file in files:
                    file_path = os.path.join(subdir, file)
                    if file.endswith('.xls') or file.endswith('.xlsx'):
                        # Move the Excel file to the 'excel_files' subdirectory
                        shutil.move(file_path, os.path.join(excel_files_dir, file))
                        print(f"Moved {file} to {excel_files_dir}")

# Specify the root directory where your folders are located
root_directory = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\data"
organize_excel_files(root_directory)
