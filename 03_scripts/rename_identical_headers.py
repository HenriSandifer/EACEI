import os
import csv

def update_csv_headers(base_dir):
    for year in range(2010, 2024):
        year_dir = os.path.join(base_dir, str(year))
        if not os.path.isdir(year_dir):
            continue

        for filename in os.listdir(year_dir):
            if "T4" in filename and filename.endswith('.csv'):
                file_path = os.path.join(year_dir, filename)
                temp_file_path = os.path.join(year_dir, f'temp_{filename}')

                with open(file_path, mode='r', newline='', encoding='utf-8') as infile, \
                     open(temp_file_path, mode='w', newline='', encoding='utf-8') as outfile:

                    reader = csv.reader(infile)
                    writer = csv.writer(outfile)

                    headers = next(reader)  # Read the header row
                    if 'T4 Consommation d’électricité' in headers:
                        index = headers.index('T4 Consommation d’électricité')
                        headers[index] = 'T4 Consommation d’électricité T4'  # Update the header

                    writer.writerow(headers)  # Write the updated header

                    # Write the rest of the rows
                    for row in reader:
                        writer.writerow(row)

                # Replace the original file with the updated file
                os.remove(file_path)
                os.rename(temp_file_path, file_path)

# Specify the base directory
base_data_path = r'C:\Users\Henri\Documents\Data_Science\Portfolio\SQL\Datasets\EACEI\02_data_clean'

# Process all T1 CSV files in the year folders
update_csv_headers(base_data_path)

if __name__ == "__main__":
    print("Replaced electrical consumption headers in T4 files")