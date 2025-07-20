import os
import csv
import re

def clean_reg_row_content(file_path):
    print(f"--- Starting region cleaning for: {os.path.basename(file_path)} ---")

    # Header keywords to identify (but not remove!)
    header_keywords = [
        "Type d’énergie", "Type d'énergie", "ENERGIES", "Indicateur", "Indicateurs",
        "INDICATEUR", "Répartition", "Autoproduction", "Achats"
    ]

    region_map = {
        "Alsace": "Grand Est", "Aquitaine": "Nouvelle-Aquitaine", "Auvergne": "Auvergne-Rhône-Alpes",
        "Basse-Normandie": "Normandie", "Bourgogne": "Bourgogne-Franche-Comté", "Bretagne": "Bretagne",
        "Centre": "Centre-Val de Loire", "Champagne-Ardenne": "Grand Est", "Corse": "_remove row_",
        "Franche-Comté": "Bourgogne-Franche-Comté", "Haute-Normandie": "Normandie",
        "Île-de-France": "Ile-de-France", "Languedoc-Roussillon": "Occitanie", "Limousin": "Nouvelle-Aquitaine",
        "Lorraine": "Grand Est", "Midi-Pyrénées": "Occitanie", "Nord-Pas-de-Calais": "Hauts-de-France",
        "Pays de la Loire": "Pays de la Loire", "Picardie": "Hauts-de-France",
        "Poitou-Charentes": "Nouvelle-Aquitaine", "PACA et Corse": "Provence-Alpes-Côte d'Azur",
        "Provence-Alpes-Côte d'Azur et Corse": "Provence-Alpes-Côte d'Azur",
        "Provence-Alpes-Côte d'Azur": "Provence-Alpes-Côte d'Azur", "Rhône-Alpes": "Auvergne-Rhône-Alpes",
        "Départements d'Outre-mer": "Départements d’Outre-mer", "DOM": "Départements d’Outre-mer", "Dom": "Départements d’Outre-mer",
        "Toutes Régions": "France", "Toutes régions": "France", "France entière": "France"
    }

    region_codes = {
        "Ile-de-France": "IDF", "Centre-Val de Loire": "CVL", "Bourgogne-Franche-Comté": "BFC",
        "Normandie": "NOR", "Hauts-de-France": "HDF", "Grand Est": "GRE", "Pays de la Loire": "PAL",
        "Bretagne": "BRE", "Nouvelle-Aquitaine": "NOA", "Occitanie": "OCC", "Auvergne-Rhône-Alpes": "ARA",
        "Provence-Alpes-Côte d'Azur": "PAC", "France": "FRA", "Toutes régions": "FRA", "Toutes Régions": "FRA",
        "Départements d’Outre-mer": "DOM"
    }

    
    found_dom = False

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
            second_header_cell = "REG"
            needs_renaming = False
            header = [first_header_cell, second_header_cell] + header[2:]
            processed_headers.append(header)
            print(f" Both first cells empty")
            break

        if "ID" in first_header_cell:
            needs_renaming = False
            print(f"  - First header cell is an ID: {first_header_cell}")

        if any(keyword in first_header_cell for keyword in header_keywords) or first_header_cell == "":
            first_header_cell = "ID - REG"
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

        region_name = first_cell
        original_region = region_name

        # Normalize
        region_name = region_map.get(region_name, region_name)
        if region_name == "_remove row_":
            print(f"  - Skipping row for region: {original_region}")
            continue

        if region_name == "Départements d’Outre-mer" or region_name == "Départements d'Outre-mer":
            found_dom = True

        region_code = region_codes.get(region_name, "")
        new_row = [region_code, region_name] + row[1:]
        cleaned_rows.append(new_row)

    # Add missing DOM row if needed
    if not found_dom:
        print("  - Adding missing row: Départements d’Outre-mer")
        blank_cols = len(cleaned_rows[-1]) - 2 if cleaned_rows else 10
        dom_row = ["DOM", "Départements d’Outre-mer"] + [''] * blank_cols
        cleaned_rows.append(dom_row)

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
    input_file_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw\2011\dd_irecoeacei11_excel\of_interest\step_1\2011_REG_T1.csv"
    clean_reg_row_content(input_file_path)
