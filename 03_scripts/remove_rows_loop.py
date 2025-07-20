import os

def remove_rows(file_path, output_dir):
    # Read the CSV file
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Remove trailing rows with metadata or non-data content
    cleaned_lines = [
        line for line in lines
        if not line.startswith((
            ',,,,', '"Tab', 'Tab', 'tab', 'naf', '"naf', '"NAF', 'NAF',
            '"reg', 'reg', '"REG', 'REG', 'teff', '"teff', 'TEFF', '"TEFF',
            '"Secteur', 'SECTEUR', 'Région', 'REGION', 'TAILLE', '"Note',
            'Note', 'Champ', '"Champ', '"Sources', '"Source', 'Source', 'Tranche',
            "s :", "nd :", '"La consommation'
        ))
    ]

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Build destination path
    filename = os.path.basename(file_path)
    output_path = os.path.join(output_dir, filename)

    # Write cleaned content
    with open(output_path, 'w', encoding='utf-8') as out_file:
        out_file.writelines(cleaned_lines)

    print(f"Cleaned and saved to: {output_path}")


def process_all_files(root_dir):
    for year in range(2010, 2024):
        year_dir = os.path.join(root_dir, str(year))
        if not os.path.isdir(year_dir):
            continue

        # Walk all subfolders under the year directory
        for root, dirs, files in os.walk(year_dir):
            if os.path.basename(root) == "of_interest":
                for file in files:
                    if file.endswith(".csv"):
                        file_path = os.path.join(root, file)
                        output_dir = os.path.join(root, "removed_rows")
                        remove_rows(file_path, output_dir)

# Set the root folder (you can customize this)
base_data_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw"
process_all_files(base_data_path)
