import os
import csv

def process_csv_files(directory):
    # Loop through each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv') and filename != 'year_dim.csv' and filename != 'faits_naf.csv' and filename != 'faits_reg.csv' and filename != 'faits_teff.csv':
            input_file_path = os.path.join(directory, filename)
            output_file_path = os.path.join(directory, f"processed_{filename}")

            # Remove BOM and ensure all fields are enclosed in double quotes
            with open(input_file_path, 'rb') as f_in:
                content = f_in.read()

            # Remove UTF-8 BOM if present
            if content.startswith(b'\xef\xbb\xbf'):
                content = content[3:]

            # Decode content to string and process with csv module
            content_str = content.decode('utf-8')
            content_lines = content_str.splitlines()

            # Write processed content to output file
            with open(output_file_path, 'w', newline='', encoding='utf-8') as f_out:
                writer = csv.writer(f_out, quoting=csv.QUOTE_ALL)
                reader = csv.reader(content_lines)
                for row in reader:
                    writer.writerow(row)

# Specify the directory containing CSV files
directory = r'C:\Users\Henri\Documents\Data_Science\Portfolio\SQL\Datasets\EACEI\05_database_final'

# Process all CSV files in the directory
process_csv_files(directory)

if __name__ == "__main__":
    print("Removed BOM and added double quotes to all cells")