import pandas as pd
import os
import re
import csv

def clean_naf_row_content(file_path):
    """
    Performs focused, row-wise content cleaning for a single NAF file.
    This script ONLY modifies the content of the first two columns (code and label).
    It does NOT aggregate data rows or modify the file's header structure.

    - Splits single-column 'code - label' entries into two.
    - Replaces old NAF codes with their 2023 standard.
    - Standardizes NAF labels to match the 2023 standard.
    - Ensures a row for code '38' exists.
    - Removes specified obsolete rows.
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found at '{file_path}'")
        return

    print(f"--- Starting row content cleaning for: {os.path.basename(file_path)} ---")

    # This dictionary maps the string prefix to find at the start of a cell
    # to the final standard code that should be used.
    # The keys are ordered from longest to shortest to ensure the most specific match is found first.
    header_keywords = [
        "Type d’énergie", "Type d'énergie", "ENERGIES", "Indicateur",
        "INDICATEUR", "Répartition", "Autoproduction", "Achats"
    ]

    CODE_PREFIX_MAP = {
        # Most specific patterns first
        '"10 — 11 — 12': 'C10T12',
        '10 - 11 - 12': 'C10T12',
        '"07 - 08 — 09': 'B07T09',
        '07 - 08 - 09': 'B07T09',
        '08 - 09 -': 'B07T09',
        '08 - 09': 'B07T09',
        # Two-digit codes followed by a separator
        '07 -': 'B07T09', '08 -': 'B07T09', '09 -': 'B07T09',
        '10 -': 'C10T12', '11 -': 'C10T12', '12 -': 'C10T12',
        # Codes that are likely already in their own cell
        '07': 'B07T09', '08': 'B07T09', '09': 'B07T09',
        '10': 'C10T12', '11': 'C10T12', '12': 'C10T12',
        '13': '13', '14': '14', '15': '15',
        '16': '16', '17': '17', '18': '18',
        '20': '20', '21': '21', '22': '22',
        '23': '23', '24': '24', '25': '25',
        '26': '26', '27': '27', '28': '28',
        '29': '29', '30': '30', '31': '31',
        '32': '32', '33': '33', '38': '38',
        'B07T09': 'B07T09', 'C10T12': 'C10T12',
        '_T': '_T', 'ID' : 'ID'
    }

    label_map = {
        'B07T09': 'Industries extractives (à l’exception de l’extraction de houille, de lignite et d’hydrocarbures)',
        'C10T12': 'Fabrication de denrées alimentaires, de boissons et de produits à base de tabac',
        '13': 'Fabrication de textiles', '14': "Industrie de l'habillement",
        '15': 'Industrie du cuir et de la chaussure',
        '16': "Travail du bois et fabrication d’articles en bois et en liège, à l’exception des meubles ; fabrication d’articles en vannerie et sparterie",
        '17': 'Industrie du papier et du carton', '18': "Imprimerie et reproduction d'enregistrements",
        '20': 'Industrie chimique', '21': 'Industrie pharmaceutique',
        '22': 'Fabrication de produits en caoutchouc et en plastique',
        '23': "Fabrication d'autres produits minéraux non métalliques", '24': 'Métallurgie',
        '25': "Fabrication de produits métalliques, à l'exception des machines et des équipements",
        '26': "Fabrication de produits informatiques, électroniques et optiques",
        '27': "Fabrication d'équipements électriques", '28': 'Fabrication de machines et équipements n.c.a.',
        '29': 'Industrie automobile', '30': "Fabrication d'autres matériels de transport",
        '31': 'Fabrication de meubles', '32': 'Autres industries manufacturières',
        '33': "Réparation et installation de machines et d'équipements",
        '38': "Collecte, traitement et élimination des déchets ; récupération",
        '_T': 'Total', 'ID' : 'NAF'
    }

    # --- Step 1: Read the file using the csv module ---
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        year = os.path.basename(file_path).split('_')[0]
        t_cat = os.path.basename(file_path).split('_')[2]
        if year >= '2020':
            print(f"  - Detected post-2020 file or T4/T1 category: {year} {t_cat}")
            try :
                header_lines = [next(reader)]
                print(f"  - Post-2020 header lines are : {header_lines}")
                num_columns = len(header_lines[0])
            except StopIteration:
                header_lines = []
                num_columns = 0
                print("  - Warning: File appears to be empty or has no header.")

        elif year < '2020':
            if t_cat != 'T4.csv' and t_cat != 'T1.csv':
                print(f"  - Detected pre-2020 file and T2/T3 category: {year} {t_cat}")
                try:
                    header_lines = [next(reader), next(reader)]
                    print(f" Pre-2020 header lines are : {header_lines}")
                    num_columns = len(header_lines[0])
                except StopIteration:
                    header_lines = []
                    num_columns = 0
                    print("  - Warning: File appears to be empty or has no header.")
            else:
                print(f"  - Detected pre-2020 file and T4/T1 category: {year} {t_cat}")
                try:
                    header_lines = [next(reader)]
                    print(f"  - Pre-2020 header lines are : {header_lines}")
                    num_columns = len(header_lines[0])
                except StopIteration:
                    header_lines = []
                    num_columns = 0
                    print("  - Warning: File appears to be empty or has no header.")
        
        data_rows = [row for row in reader]

    # --- Process header rows ---
    processed_headers = []
    for header in header_lines:
        if not header: continue

        first_header_cell = header[0].strip()
        second_header_cell = header[1].strip()
        print(f"Processing header: {first_header_cell[:50]}...")  # Show first 50 characters for context    

        
        if first_header_cell == "" and second_header_cell == "":
            first_header_cell = "ID"
            second_header_cell = "NAF"
            needs_renaming = False
            header = [first_header_cell, second_header_cell] + header[2:]
            processed_headers.append(header)
            break

        if "ID" in first_header_cell:
            needs_renaming = False
            print(f"  - First header cell is an ID: {first_header_cell}")

        if any(keyword in first_header_cell for keyword in header_keywords) or first_header_cell == "":
            first_header_cell = "ID - NAF"
            needs_renaming = True
            print(f"   - First header cell triggered renaming: {first_header_cell}")
            
        if needs_renaming:
            print(f"  - Renaming header cell: {first_header_cell}")
            # --- Logic for combined cells that need splitting ---
            found_split = False
            for f in first_header_cell:
                if first_header_cell.startswith("ID"):
                    # Found the code prefix. Now, split the cell.
                    ID_part = "ID"
                    print(f"  - Found code prefix: {ID_part}")
                    # Get the rest of the string after the prefix
                    category_part = first_header_cell[len(ID_part):].strip()
                    print(f"  - Label part before cleanup: '{category_part}'")
                    # Remove any leading separator (like '-' or '–') from the label part
                    category_part = re.sub(r'^\s*[-–]\s*', '', category_part).strip()
                    print(f"  - Label part after cleanup: '{category_part}'")
                    # Reconstruct the row correctly
                    header = [ID_part, category_part] + header[1:]
                    print(f"  - Reconstructed header: {header[:4]}")  # Show first 4 cells
                    
                    # If the year is pre-2020, forward fill the first row
                    if year < '2020':
                        # Split first row
                        target_row = header
                        # Forward fill the first row manually
                        filled_row = []
                        last_val = None  # Use None to represent empty cells initially
                        for cell in target_row:
                            # Check if the cell is not empty
                            if cell:  # This checks for non-empty strings
                                last_val = cell
                            filled_row.append(last_val if last_val is not None else "")    
                        header = filled_row 
                    found_split = True
                    break

            if not found_split:
                 print(f"  - Warning: Could not find a split pattern for combined cell: '{first_header_cell}'")
        
        if not header: continue
        
        processed_headers.append(header)
        
    # --- Step 2: Process each data row ---
    processed_rows = []
    for row in data_rows:
        if not row: continue
        
        first_cell = row[0].strip()
        print(f"Processing row: {first_cell[:50]}...")  # Show first 50 characters for context

        if first_cell.startswith("Total hors IAA") or first_cell.startswith("_T_HIAA"):
            print(f"  - Removing obsolete row: {first_cell}")
            continue

        # Detect if the first cell contains a label (i.e., needs splitting)
        # and is not 'B07T09' or 'C10T12' directly,
        # by checking for any alphabetic characters.
        if 'B07T09' in first_cell or 'C10T12' in first_cell or '_T' in first_cell:
            is_combined = False
            print(f"  - First cell is already a code: {first_cell}")

        if "Total" in first_cell:
            first_cell = "_T - Total"
            is_combined = True
    
        else:
            is_combined = bool(re.search(r'[a-zA-Z]', first_cell)) and not first_cell == 'B07T09' and not first_cell == 'C10T12' and not first_cell == '_T'
            print(f"  - Detected combined cell: {is_combined}")

        if is_combined:
            print(f"  - Processing combined cell: {first_cell}")
            # --- Logic for combined cells that need splitting ---
            found_split = False
            for prefix in CODE_PREFIX_MAP.keys():
                if first_cell.startswith(prefix):
                    # Found the code prefix. Now, split the cell.
                    code_part = prefix
                    print(f"  - Found code prefix: {code_part}")
                    # Get the rest of the string after the prefix
                    label_part = first_cell[len(prefix):].strip()
                    print(f"  - Label part before cleanup: '{label_part}'")
                    # Remove any leading separator (like '-' or '–') from the label part
                    label_part = re.sub(r'^\s*[-–]\s*', '', label_part).strip()
                    print(f"  - Label part after cleanup: '{label_part}'")
                    
                    # Reconstruct the row correctly
                    row = [code_part, label_part] + row[1:]
                    print(f"  - Reconstructed row: {row[:4]}")  # Show first 4 cells
                    found_split = True
                    break
            if not found_split:
                 print(f"  - Warning: Could not find a split pattern for combined cell: '{first_cell}'")

        # --- Standardize codes and labels for ALL rows (split or not) ---
        if not row: continue

        # Standardize the code (first cell)
        code_cell = row[0].strip()
        final_code = CODE_PREFIX_MAP.get(code_cell, code_cell) # Find replacement, or keep original
        row[0] = final_code

        # Standardize the label (second cell) based on the final code
        if len(row) > 1 and final_code in label_map:
            row[1] = label_map[final_code]
        
        processed_rows.append(row)

    # --- Step 3: Ensure row for code '38' exists ---
    found_38 = any(row and row[0].strip() == '38' for row in processed_rows)
    if not found_38:
        print("  - Code '38' not found. Creating a new row.")
        # Use num_columns from the header to create a row of the correct length
        new_row_38 = ['38', label_map['38']] + ['0'] * (num_columns - 1)
        processed_rows.append(new_row_38)

    # --- Step 4: Save the final content using the csv module ---
    output_dir = os.path.dirname(file_path)
    base_name = os.path.basename(file_path).replace('.csv', '')
    output_path = os.path.join(output_dir, f"{base_name}_38.csv")

    try:
        with open(output_path, 'w', encoding='utf-8-sig', newline='') as f_out:
            writer = csv.writer(f_out)
            writer.writerows(processed_headers)
            writer.writerows(processed_rows)
        print(f"\nSuccess! Row-content-cleaned file saved to:\n{output_path}")
    except Exception as e:
        print(f"\nError saving the new file: {e}")

if __name__ == '__main__':
    # --- How to use the script ---
    # Place this script in your project folder.
    # Replace the path below with the path to the NAF file you want to clean.
    input_file_path = r'C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw\2010\dd_irecoeacei10_excel\of_interest\step_1\2010_NAF_T3.csv'
    
    # Run the cleaning function
    clean_naf_row_content(input_file_path)
