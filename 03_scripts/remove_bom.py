def remove_bom(input_file, output_file):
    with open(input_file, 'rb') as f_in:
        content = f_in.read()

    # Remove UTF-8 BOM if present
    if content.startswith(b'\xef\xbb\xbf'):
        content = content[3:]

    with open(output_file, 'wb') as f_out:
        f_out.write(content)

# Example usage
input_csv_file = r'C:\Users\Henri\Documents\Data_Science\Portfolio\SQL\Datasets\EACEI\05_database_final\faits_naf.csv'
output_csv_file = r'C:\Users\Henri\Documents\Data_Science\Portfolio\SQL\Datasets\EACEI\05_database_final\nobom_faits_naf.csv'
remove_bom(input_csv_file, output_csv_file)

if __name__ == "__main__":
    print("Removed BOM encoding.")