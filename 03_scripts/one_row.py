import re
import os

def remove_newlines_in_quotes(file_path):
    # Read the entire file as raw text
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Pattern: find newlines inside quoted strings
    # This looks for "..." blocks and removes any \n inside them
    def clean_quoted_block(match):
        return match.group(0).replace('\n', ' ').replace('\r', '')

    cleaned_content = re.sub(r'"[^"]*"', clean_quoted_block, content)

    # Build output filename
    base, ext = os.path.splitext(file_path)
    new_path = base + "_onerow" + ext

    # Save cleaned version
    with open(new_path, 'w', encoding='utf-8') as f:
        f.write(cleaned_content)

    print(f"Cleaned file saved to: {new_path}")

# Example usage:
file_path = r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\01_data_raw\2021\irecoeacei21_xlsx\of_interest\removed_rows\2021_REG_T2.csv"
remove_newlines_in_quotes(file_path)
