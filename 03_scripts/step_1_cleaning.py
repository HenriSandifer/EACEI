import os
import re

def clean_file(file_path, output_dir):
    # Step 1: Read the entire file as text
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Step 2: Remove newlines within quoted strings
    def clean_quoted_block(match):
        return match.group(0).replace('\n', ' ').replace('\r', '')
    content = re.sub(r'"[^"]*"', clean_quoted_block, content)

    # Step 3: Split into lines for line-based cleaning
    lines = content.splitlines(keepends=True)

    # Step 4: Remove metadata/footnote rows based on prefix
    cleaned_lines = [
        line for line in lines
        if not line.startswith((
            ',,,,', '"Tab', 'Tab', 'tab', '"tab', 'naf', '"naf', '"NAF', 'NAF',
            '"reg', 'reg', '"REG', 'REG', 'teff', '"teff', 'TEFF', '"TEFF',
            '"Secteur', 'SECTEUR', 'Région', 'REGION', 'TAILLE', '"Note',
            'Note', 'Champ', '"Champ', '"Sources', '"Source', 'Source', 'Tranche',
            "s :", "nd :", '"La consommation'
        ))
    ]

    # Step 5: Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Step 6: Write cleaned content
    output_path = os.path.join(output_dir, os.path.basename(file_path))
    with open(output_path, 'w', encoding='utf-8') as out_file:
        out_file.writelines(cleaned_lines)

    print(f"Cleaned and saved: {output_path}")

def process_all_files(base_dir):
    for year in range(2010, 2024):
        year_path = os.path.join(base_dir, str(year))
        if not os.path.isdir(year_path):
            continue

        for root, dirs, files in os.walk(year_path):
            if os.path.basename(root) == "original":
                for file in files:
                    if file.endswith(".csv"):
                        file_path = os.path.join(root, file)

                        # Get the parent directory of "original" (which is "of_interest")
                        of_interest_dir = os.path.dirname(root)

                        # Create or use "step_1" folder at the same level as "original"
                        output_dir = os.path.join(of_interest_dir, "step_1")

                        clean_file(file_path, output_dir)

# Set your root path here:
base_data_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw"
process_all_files(base_data_path)
