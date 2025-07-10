import pandas as pd

def forward_fill_first_row(csv_path, output_path=None):
    """
    Forward fills the first row of a CSV file where header rows contain empty cells 
    between labeled cells (used for hierarchical headers).

    Parameters:
        csv_path (str): Path to the input CSV file.
        output_path (str, optional): Path to save the updated file. 
                                     If None, returns the DataFrame.
    """
    # Load file without interpreting any headers
    df = pd.read_csv(csv_path, header=None)

    # Forward fill the first row
    df.iloc[0] = df.iloc[0].ffill()

    # Save or return
    if output_path:
        df.to_csv(output_path, index=False, header=False)
        print(f"Forward-filled CSV saved to: {output_path}")
    else:
        return df

# Example usage:
# df_cleaned = forward_fill_first_row("2012_NAF_T2_cleaned.csv")
# Or to save the file:
forward_fill_first_row(r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\data\2012\dd_irecoeacei12\of_interest\2012_NAF_T2_cleaned.csv", r"C:\Users\Henri\Documents\Data Science\Portfolio\SQL\Datasets\EACEI\data\2012\dd_irecoeacei12\of_interest\2012_NAF_T2_ffilled.csv")


