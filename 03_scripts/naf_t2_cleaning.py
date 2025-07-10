import pandas as pd
import re

def clean_csv(file_path):
    # Read the CSV file
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Initialize a list to store modified lines
    modified_lines = []

    # Process each line
    for i, line in enumerate(lines):
        if line.startswith("INDICATEUR"):
            modified_line = "ID,NAF," + ','.join(line.split(',')[1:])
            modified_lines.append(modified_line)
        elif line.startswith("Total"):
            modified_lines.append(f"_T,{line}")
        elif line.startswith("Total hors IAA"):
            modified_lines.append(f"_T_HIAA,{line}")
        else:
            # Replace " - " with "," and adjust quotes
            modified_line = re.sub(r'"(\d+)\s+-\s+', r'\1,"', line)
            modified_line = re.sub(r'(\d+)\s+-\s+', r'\1,', modified_line)
            modified_lines.append(modified_line)

    # Remove the last rows containing metadata
    modified_lines = [line for line in modified_lines if not line.startswith((",", '"Secteur', '"naf', '"NAF', "Note", '"Note', "Champ", '"Champ', '"Sources', '"Source', 'Source', "SECTEUR", '"Tab'))]

    # Write the modified lines back to a new CSV file
    output_file_path = file_path.replace('.csv', '_cleaned.csv')
    with open(output_file_path, 'w', encoding='utf-8') as file:
        file.writelines(modified_lines)

    print(f"Cleaned and saved: {output_file_path}")

# Example usage
file_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\data\2023\irecoeacei23_xlsx\of_interest\2023_NAF_T2.csv"
clean_csv(file_path)
