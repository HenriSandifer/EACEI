import os
import csv
import re

def clean_teff_row_content(file_path):
    print(f"--- Starting TEFF cleaning for: {os.path.basename(file_path)} ---")

    # Header keywords to identify (but not remove!)
    header_keywords = [
        "Type d’énergie", "Type d'énergie", "ENERGIES", "Indicateur",
        "INDICATEUR", 'Répartition', 'Autoproduction',
        'Achats'
    ]

    teff_map = {
        "20 à 49 employés": "20 à 49 salariés",
        "50 à 99 employés": "50 à 99 salariés",
        "100 à 249 employés": "100 à 249 salariés",
        "250 à 499 employés": "250 à 499 salariés",
        "500 à 999 employés": "500 salariés et plus",
        "1 000 à 1 999 employés": "500 salariés et plus",
        "2 000 employés ou plus": "500 salariés et plus",
        "500 salariés ou plus": "500 salariés et plus",
        "Total industrie": "Total"
        }

    teff_codes = {
        "20 à 49 salariés": "20-49",
        "50 à 99 salariés": "50-99",
        "100 à 249 salariés": "100-249",
        "250 à 499 salariés": "250-499",
        "500 salariés et plus": "500+",
        "Total": "_T"
    }

    # --- Read the file using the csv module ---
    # --- Differentiate between pre-2020 and post-2020 files ---
   
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        year = os.path.basename(file_path).split('_')[0]
        t_cat = os.path.basename(file_path).split('_')[2]
        if year >= '2020':
            print(f"  - Detected post-2020 file or T4/T1 category: {year} {t_cat}")
            try :
                header_lines = [next(reader)]
                print(f"  - Post-2020 header lines are : {header_lines}")
            except StopIteration:
                header_lines = []
                print("  - Warning: File appears to be empty or has no header.")

        elif year < '2020':
            if t_cat != 'T4.csv' and t_cat != 'T1.csv':
                print(f"  - Detected pre-2020 file and T2/T3 category: {year} {t_cat}")
                try:
                    header_lines = [next(reader), next(reader)]
                    print(f" Pre-2020 header lines are : {header_lines}")
                except StopIteration:
                    header_lines = []
                    print("  - Warning: File appears to be empty or has no header.")
            else:
                print(f"  - Detected pre-2020 file and T4/T1 category: {year} {t_cat}")
                try:
                    header_lines = [next(reader)]
                    print(f"  - Pre-2020 header lines are : {header_lines}")
                except StopIteration:
                    header_lines = []
                    print("  - Warning: File appears to be empty or has no header.")
        
        data_rows = [row for row in reader]

    # --- Process header rows ---
    processed_headers = []

    for header in header_lines:
        if not header: continue

        first_header_cell = header[0].strip()
        second_header_cell = header[1].strip()
        print(f"Processing header: {first_header_cell[:50]}...")

        if first_header_cell == "" and second_header_cell == "":
            first_header_cell = "ID"
            second_header_cell = "TEFF"
            needs_renaming = False
            header = [first_header_cell, second_header_cell] + header[2:]
            processed_headers.append(header)
            print(f"   - Both first cells initially empty")
            break

        if "ID" in first_header_cell:
            needs_renaming = False
            print(f"   - First header cell is ID: {first_header_cell}")

        if any(keyword in first_header_cell for keyword in header_keywords) or first_header_cell == "":
            first_header_cell = "ID - TEFF"
            needs_renaming = True
            print(f"   - First header cell triggered renaming: {first_header_cell}")
            
        if needs_renaming:
            print(f"   - Renaming header cell: {first_header_cell}")
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
                    print(f"  - Reconstructed header: {header[:4]}")

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

    # --- Process each data row ---
    cleaned_rows = []

    for row in data_rows:
        if not row:
            continue

        first_cell = row[0].strip()

        # ✅ Preserve header rows as-is
        if any(first_cell.startswith(h) for h in header_keywords):
            cleaned_rows.append(row)
            continue

        teff_name = first_cell
        original_teff = teff_name

        # Normalize
        teff_name = teff_map.get(teff_name, teff_name)
        if teff_name == "_remove row_":
            print(f"  - Skipping row for teff: {original_teff}")
            continue

        teff_code = teff_codes.get(teff_name, "")
        new_row = [teff_code, teff_name] + row[1:]
        cleaned_rows.append(new_row)

    # Save output
    output_path = file_path.replace(".csv", "_rows.csv")
    try:
        with open(output_path, 'w', encoding='utf-8-sig', newline='') as f_out:
            writer = csv.writer(f_out)
            writer.writerows(processed_headers)
            writer.writerows(cleaned_rows)
        print(f"✅ Saved cleaned file to: {output_path}")
    except Exception as e:
        print(f"❌ Error saving cleaned file: {e}")


if __name__ == '__main__':
    input_file_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw\2023\irecoeacei23_xlsx\of_interest\step_1\2023_TEFF_T4.csv"
    clean_teff_row_content(input_file_path)
