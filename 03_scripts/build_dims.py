import os
import json
import pandas as pd


def build_dims():
    
    # Paths
    INPUT_MAPPING = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\04_dictionaries\id_mapping.json"
    OUTPUT_DIR = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\05_database_final"

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load the mapping file
    def load_mapping(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    mapping = load_mapping(INPUT_MAPPING)

    # 1) dim_naf, dim_reg, dim_teff
    for dim in ("NAF", "REG", "TEFF"):
        records = mapping.get(dim, [])
        if not records:
            continue

        # Build DataFrame
        df = pd.DataFrame(records)
        # Standardize column names
        if dim == "NAF":
            df = df.rename(columns={"naf_id": "naf_id", "naf_code": "naf_code", "naf_label": "naf_label"})
            cols = ["naf_id", "naf_code", "naf_label"]
            out = "naf_dim.csv"
        elif dim == "REG":
            df = df.rename(columns={"reg_id": "reg_id", "reg_code": "reg_code", "reg_label": "reg_label"})
            cols = ["reg_id", "reg_code", "reg_label"]
            out = "reg_dim.csv"
        else:  # TEFF
            df = df.rename(columns={"teff_id": "teff_id", "teff_code": "teff_code", "teff_label": "teff_label"})
            cols = ["teff_id", "teff_code", "teff_label"]
            out = "teff_dim.csv"

        # Reorder and save
        df[cols].to_csv(os.path.join(OUTPUT_DIR, out), index=False, encoding='utf-8-sig')
        print(f"Written {out}")

    # 2) dim_year
    years = list(range(2010, 2024))
    df_year = pd.DataFrame({"year_id": years, "year": years})
    df_year.to_csv(os.path.join(OUTPUT_DIR, "year_dim.csv"), index=False, encoding='utf-8-sig')
    print("Written year_dim.csv")

    # 3) dim_indicator (all T1-T4)
    ind_list = []
    for set_name in ("T1", "T2", "T3", "T4"):
        for rec in mapping.get(set_name, []):
            entry = {
                "ind_id": rec["ind_id"],
                "ind_set": set_name,
                "ind_code": rec[f"{set_name}_code"],
                "ind_label": rec[f"{set_name}_label"],
                "unit": rec.get(f"{set_name}_unit", ""),
                "unit_label": rec.get(f"{set_name}_unit_label", "")
            }
            ind_list.append(entry)

    df_ind = pd.DataFrame(ind_list)
    cols = ["ind_id", "ind_set", "ind_code", "ind_label", "unit", "unit_label"]

    df_ind[cols].to_csv(os.path.join(OUTPUT_DIR, "ind_dim.csv"), index=False, encoding='utf-8-sig')
    print("Written ind_dim.csv")

if __name__ == '__main__':
    build_dims()