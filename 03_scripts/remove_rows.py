def clean_csv(file_path):
    # Read the CSV file
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Initialize a list to store modified lines
    modified_lines = []

    # Process each line
    for i, line in enumerate(lines):
        modified_lines.append(line)

    # Remove the last rows containing metadata
    modified_lines = [line for line in modified_lines if not line.startswith((',,,,','"Tab', "Tab", "tab", "naf", '"naf', '"NAF', "NAF", '"reg', "reg", '"REG', "REG", "teff", '"teff', "TEFF", '"TEFF', '"Secteur', "SECTEUR", "Région", "REGION", "TAILLE", '"Note', "Note",  "Champ", '"Champ', '"Sources', '"Source', "Source", "Tranche"))]

    # Write the modified lines back to a new CSV file
    output_file_path = file_path.replace('.csv', '_rr.csv')
    with open(output_file_path, 'w', encoding='utf-8') as file:
        file.writelines(modified_lines)

    print(f"Cleaned and saved: {output_file_path}")

# Example usage
file_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw\2010\dd_irecoeacei10_excel\of_interest\2010_NAF_T4.csv"
clean_csv(file_path)
