import csv

input_file = r'C:\Users\Henri\Documents\Data_Science\Portfolio\SQL\Datasets\EACEI\05_database_final\faits_naf.csv'
output_file = r'C:\Users\Henri\Documents\Data_Science\Portfolio\SQL\Datasets\EACEI\05_database_final\faits_naf_null.csv'

with open(input_file, mode='r', newline='', encoding='utf-8') as infile, \
     open(output_file, mode='w', newline='', encoding='utf-8') as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)

    for row in reader:
        # Replace empty strings in the 'value' column with \N
        if len(row) >= 4:  # Assuming 'value' is the 4th column
            if row[3] == '':
                row[3] = '\\N'
        writer.writerow(row)

if __name__ == '__main__':
    print("Added N to null cells")