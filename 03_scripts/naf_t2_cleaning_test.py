import pandas as pd
import re

def clean_csv(file_path):
    # Read the CSV file
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Initialize a list to store modified lines
    modified_lines = []

    # Flag to indicate if we are in the section of interest
    in_section = False

    # Process each line
    for line in lines:
        # Check if the line starts with specific keywords to determine the section
        if "Valeur des achats" in line:
            in_section = True
        if in_section:
            # Replace newline characters with spaces only in the section of interest
            line = line.replace('\n', ' ')

        # Check if the line contains the end of the section
        if "Prix moyen d’électricité" in line:
            in_section = False

        # Apply other transformations
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
    modified_lines = [line for line in modified_lines if not line.startswith((",", '"Secteur', '"naf', '"NAF', "Note", '"Note', "Champ", '"Champ', '"Sources', '"Source', "SECTEUR", '"Tab'))]

    # Write the modified lines back to a new CSV file
    output_file_path = file_path.replace('.csv', '_cleaned.csv')
    with open(output_file_path, 'w', encoding='utf-8') as file:
        file.writelines(modified_lines)

    print(f"Cleaned and saved: {output_file_path}")

# Example usage
file_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\data\2020\irecoeacei20_xlsx\of_interest\2020_NAF_T2.csv"
clean_csv(file_path)
