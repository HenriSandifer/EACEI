import os
import shutil

def organize_and_rename_files(root_dir):
    # Loop through each year directory
    for year in range(2010, 2024):
        year_dir = os.path.join(root_dir, str(year))
        print(f"Inside folder: {year_dir} ...")

        if os.path.isdir(year_dir):
            # Loop through each directory and subdirectory within the year directory
            for subdir, _, files in os.walk(year_dir):
                # Check if there are any CSV files in the current directory
                csv_files = [file for file in files if file.lower().endswith('.csv')]
                if csv_files:
                    # Create the 'of_interest' subdirectory if it doesn't exist
                    of_interest_dir = os.path.join(subdir, 'of_interest')
                    os.makedirs(of_interest_dir, exist_ok=True)
                    print(f"Created of_interest folder in: {of_interest_dir}")

                    # Loop through all CSV files in the current directory
                    for file in csv_files:
                        file_path = os.path.join(subdir, file)
                        print(f"Dealing with file: {file_path}")

                        # Check if the file contains specific keywords but not exclusionary keywords
                        if any(keyword in file.lower() for keyword in ['naf', 'reg', 'teff', 'taille', 'effectif', "secteur d'activité", 'régions']):
                            if not any(exclusion in file.lower() for exclusion in ['regio', 'region', 'tab5', 'ia']):
                                # Determine the category
                                category = None
                                if any(keyword in file.lower() for keyword in ['naf', 'secteur d\'activité']):
                                    category = 'NAF'
                                elif any(keyword in file.lower() for keyword in ['reg', 'régions']):
                                    category = 'REG'
                                elif any(keyword in file.lower() for keyword in ['teff', 'taille', 'effectif']):
                                    category = 'TEFF'

                                # Determine the T value
                                t_value = None
                                for t in ['T1', 'T2', 'T3', 'T4', 'tab1', 'tab2', 'tab3', 'tab4']:
                                    if t.lower() in file.lower():
                                        t_value = t[-1].upper()  # Extract the number from 'tabX' and use it as 'TX'
                                        t_value = f"T{t_value}"
                                        break

                                if category and t_value:
                                    # Define a new file name based on the pattern
                                    new_file_name = f"{year}_{category}_{t_value}.csv"
                                    new_file_path = os.path.join(of_interest_dir, new_file_name)

                                    # Copy and rename the file to the 'of_interest' subdirectory
                                    shutil.copy2(file_path, new_file_path)
                                    print(f"Copied and renamed {file} to {new_file_path}")

# Specify the root directory where your folders are located
root_directory = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\data"
organize_and_rename_files(root_directory)
