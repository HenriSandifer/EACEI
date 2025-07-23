import csv

# Input and output file paths
input_file = r'C:\Users\Henri\Documents\Data_Science\Portfolio\SQL\Datasets\EACEI\05_database_final\nobom_faits_naf.csv'
output_file = r'C:\Users\Henri\Documents\Data_Science\Portfolio\SQL\Datasets\EACEI\05_database_final\faits_naf_quotes.csv'

# Read the input CSV and write to output with all fields enclosed in double quotes
with open(input_file, mode='r', newline='', encoding='utf-8') as infile, \
     open(output_file, mode='w', newline='', encoding='utf-8') as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)

    for row in reader:
        writer.writerow(row)

if __name__ == "__main__":
    print("Added double quotes")