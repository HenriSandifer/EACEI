import pytest
import pandas as pd
import numpy as np
import os

def load_original_data(year):
    """Load the original data file for a given year with proper multi-index handling."""
    base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                            '01_data_raw', str(year), f'dd_irecoeacei{year[2:]}_excel',
                            'of_interest', f'{year}_NAF_T2_cleaned_ffill.csv')
    # Read with multi-index headers
    return pd.read_csv(base_path, header=[0, 1], encoding='utf-8')

def load_standardized_data(year):
    """Load the standardized data file for a given year."""
    base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                            '01_data_raw', str(year), f'dd_irecoeacei{year[2:]}_excel',
                            'of_interest', f'{year}_NAF_T2_cleaned_ffill_standardized.csv')
    return pd.read_csv(base_path, encoding='utf-8')

def safe_float_conversion(value):
    """Safely convert a value to float, handling 's' and NaN."""
    if pd.isna(value) or value == 's':
        return 0.0
    return float(value)

def test_mineral_combustibles_sum_2010():
    """Test that mineral combustibles are correctly summed in the standardized file for 2010."""
    # Load both original and standardized data
    original_df = load_original_data('2010')
    standardized_df = load_standardized_data('2010')
    
    # Get the total row (marked with '_T')
    original_total = original_df[original_df.iloc[:, 1] == 'Total']
    standardized_total = standardized_df[standardized_df['naf_code'] == '_T']
    
    # Extract values using multi-index structure
    houille = safe_float_conversion(original_total[('Houille (en milliers de tonnes)', 'Quantités achetées')].iloc[0])
    lignite = safe_float_conversion(original_total[('Lignite - charbon pauvre (en milliers de tonnes)', 'Quantités achetées')].iloc[0])
    coke = safe_float_conversion(original_total[('Coke de houille (en milliers de tonnes)', 'Quantités achetées')].iloc[0])
    
    # Calculate expected sum
    expected_sum = houille + lignite + coke
    
    # Get actual sum from standardized file
    actual_sum = safe_float_conversion(standardized_total['Combustibles minéraux solides achetés (en milliers de tonnes)'].iloc[0])
    
    # Assert that the sums match
    assert abs(expected_sum - actual_sum) < 0.01, \
        f"Expected sum {expected_sum} doesn't match actual sum {actual_sum} for Total row"

def test_random_industry_rows_2010():
    """Test random industry rows to ensure data consistency."""
    # Load both datasets
    original_df = load_original_data('2010')
    standardized_df = load_standardized_data('2010')
    
    # Select 10 random rows (excluding the total rows)
    non_total_rows = original_df[~original_df.iloc[:, 1].isin(['Total', 'Total hors IAA'])]
    sample_size = min(10, len(non_total_rows))
    random_indices = np.random.choice(non_total_rows.index, size=sample_size, replace=False)
    
    for idx in random_indices:
        original_row = original_df.iloc[idx]
        naf_code = original_row.iloc[0]
        standardized_row = standardized_df[standardized_df['naf_code'] == naf_code]
        
        if not standardized_row.empty:
            # Extract values using multi-index structure
            houille = safe_float_conversion(original_row[('Houille (en milliers de tonnes)', 'Quantités achetées')])
            lignite = safe_float_conversion(original_row[('Lignite - charbon pauvre (en milliers de tonnes)', 'Quantités achetées')])
            coke = safe_float_conversion(original_row[('Coke de houille (en milliers de tonnes)', 'Quantités achetées')])
            
            expected_sum = houille + lignite + coke
            actual_sum = safe_float_conversion(standardized_row['Combustibles minéraux solides achetés (en milliers de tonnes)'].iloc[0])
            
            assert abs(expected_sum - actual_sum) < 0.01, \
                f"NAF code {naf_code}: Expected sum {expected_sum} doesn't match actual sum {actual_sum}"

def test_suppressed_values_handling_2010():
    """Test the handling of suppressed values ('s') in the data transformation."""
    original_df = load_original_data('2010')
    standardized_df = load_standardized_data('2010')
    
    # Find rows where all mineral combustible values are 's'
    for idx in original_df.index:
        row = original_df.iloc[idx]
        naf_code = row.iloc[0]
        
        values = [
            row[('Houille (en milliers de tonnes)', 'Quantités achetées')],
            row[('Lignite - charbon pauvre (en milliers de tonnes)', 'Quantités achetées')],
            row[('Coke de houille (en milliers de tonnes)', 'Quantités achetées')]
        ]
        
        # Check if all values are suppressed
        if all(val == 's' for val in values):
            std_row = standardized_df[standardized_df['naf_code'] == naf_code]
            if not std_row.empty:
                std_value = std_row['Combustibles minéraux solides achetés (en milliers de tonnes)'].iloc[0]
                # The standardized value should be empty or NaN when all input values are suppressed
                assert pd.isna(std_value) or std_value == '', \
                    f"NAF code {naf_code}: Expected NaN or empty for all suppressed values"

def test_column_mapping_consistency_2010():
    """Test that the column mapping is consistent across the dataset."""
    original_df = load_original_data('2010')
    standardized_df = load_standardized_data('2010')
    
    # Verify that all NAF codes are preserved
    original_naf_codes = set(original_df.iloc[:, 0])
    standardized_naf_codes = set(standardized_df['naf_code'])
    
    assert original_naf_codes == standardized_naf_codes, \
        "NAF codes don't match between original and standardized datasets"
