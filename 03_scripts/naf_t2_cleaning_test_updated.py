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
    for i, line in enumerate(lines):
        # Check if the line starts the relevant section
        if "Valeur des achats" in line:
            in_section = True

        if in_section:
            # Preserve the line containing "Prix moyen d’électricité"
            if "Prix moyen d’électricité" in line:
                # Add it as-is with newline
                modified_lines.append(line)
                in_section = False
                continue
            else:
                # Remove inner-section newlines
                line = line.replace('\n', ' ')

        # Apply special formatting to multi-code lines
        # Normalize double dashes and space-dash-space variations
        if re.match(r'^\s*"?\d{2}\s*[-–—]\s*\d{2}\s*[-–—]\s*\d{2}', line):
            # Normalize all types of dashes
            line = re.sub(r'[-–—]', '-', line)

            # Remove any leading whitespace or quote
            line = line.strip().lstrip('"')

            # Extract the code group (first 8–11 chars) and the rest
            match = re.match(r'^(\d{2}-\d{2}-\d{2})\s+(.*)', line)
            if match:
                code_part = match.group(1)
                label_part = match.group(2).strip()
                line = f"{code_part},\"{label_part}\"\n"
            else:
                # If match fails, write the line back safely
                line = line + "\n"

            modified_lines.append(line)
            continue

        # Continue existing cleaning logic
        if line.startswith("INDICATEUR"):
            modified_line = "ID,NAF," + ','.join(line.split(',')[1:])
            modified_lines.append(modified_line)
        elif line.startswith("Total"):
            modified_lines.append(f"_T,{line}")
        elif line.startswith("Total hors IAA"):
            modified_lines.append(f"_T_HIAA,{line}")
        else:
            # Only apply these substitutions if the line is NOT a 3-code line
            if not re.match(r'^\s*"?\d{2}\s*[-–—]\s*\d{2}\s*[-–—]\s*\d{2}', line):
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
file_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\data\2022\irecoeacei22_xlsx\of_interest\2022_NAF_T2.csv"
clean_csv(file_path)
