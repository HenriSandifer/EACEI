import pytest
import pandas as pd
import numpy as np
import os
from io import StringIO

# Region name to code mapping
REGION_MAPPING = {
    'Auvergne-Rhône-Alpes': 'ARA',
    'Bourgogne-Franche-Comté': 'BFC',
    'Bretagne': 'BRE',
    'Centre-Val de Loire': 'CVL',
    "Départements d'Outre-mer": 'DOM',
    'France': 'FRA',
    'Grand Est': 'GRE',
    'Hauts-de-France': 'HDF',
    'Ile-de-France': 'IDF',
    'Nouvelle-Aquitaine': 'NOA',
    'Normandie': 'NOR',
    'Occitanie': 'OCC',
    "Provence-Alpes-Côte d'Azur": 'PAC',
    'Pays de la Loire': 'PAL'
}

def safe_float_conversion(value):
    """Safely convert a value to float, handling 's' and NaN."""
    if pd.isna(value) or value == 's' or value == '':
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def sum_region_values(df, columns):
    """Sum values for specified columns, handling 's' values."""
    return df[columns].replace(['s', ''], '0').astype(float).sum().sum()

# Test data for controlled tests
TEST_INPUT_DATA = """ID,REG,Houille,Lignite - charbon pauvre,Coke de houille,Gaz naturel de réseau
R1,ARA,10,20,30,100
R2,ARA,15,25,35,200"""

TEST_EXPECTED_OUTPUT = """reg_code,reg_label,Consommation de combustibles minéraux solides,Consommation de gaz naturel et autres gaz
ARA,Auvergne-Rhône-Alpes,135.0,300.0"""

def load_test_data():
    """Load the test data into pandas DataFrames."""
    input_df = pd.read_csv(StringIO(TEST_INPUT_DATA))
    expected_df = pd.read_csv(StringIO(TEST_EXPECTED_OUTPUT))
    # Convert numeric columns to float
    numeric_cols = ['Consommation de combustibles minéraux solides', 'Consommation de gaz naturel et autres gaz']
    expected_df[numeric_cols] = expected_df[numeric_cols].astype(float)
    return input_df, expected_df

def load_real_data(year='2010'):
    """Load the real input and output data files."""
    base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '01_data_raw', str(year),
                            f'dd_irecoeacei{year[2:]}_excel', 'of_interest', 'step_2')
    
    input_path = os.path.join(base_path, f'{year}_REG_T1.csv')
    output_path = os.path.join(base_path, f'{year}_REG_T1_columns.csv')
    
    print(f"\nLoading data from:")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    
    input_df = pd.read_csv(input_path)
    output_df = pd.read_csv(output_path)
    
    print(f"\nInput data shape: {input_df.shape}")
    print(f"Output data shape: {output_df.shape}")
    
    print("\nInput file unique regions:")
    print(input_df['REG'].unique())
    print("\nOutput file unique regions:")
    print(output_df['reg_code'].unique())
    
    # Add region codes to input DataFrame
    input_df['reg_code'] = input_df['REG'].map(REGION_MAPPING)
    
    return input_df, output_df

def test_mineral_combustibles_aggregation_controlled():
    """Test column aggregation with controlled test data."""
    input_df, expected_df = load_test_data()
    
    # Sum the mineral combustibles columns
    mineral_cols = ['Houille', 'Lignite - charbon pauvre', 'Coke de houille']
    mineral_sum = sum_region_values(input_df, mineral_cols)
    
    # Create result DataFrame
    result = pd.DataFrame({
        'reg_code': ['ARA'],
        'Consommation de combustibles minéraux solides': [mineral_sum]
    })
    
    # Convert to float for comparison
    result['Consommation de combustibles minéraux solides'] = result['Consommation de combustibles minéraux solides'].astype(float)
    
    # Compare with expected output
    pd.testing.assert_series_equal(
        result['Consommation de combustibles minéraux solides'],
        expected_df['Consommation de combustibles minéraux solides'],
        check_names=False
    )

def test_mineral_combustibles_real_data():
    """Test mineral combustibles aggregation with real 2010 data."""
    input_df, output_df = load_real_data()
    
    # Test specific ARA rows
    ara_rows = input_df[input_df['reg_code'] == 'ARA'].copy()
    
    # Calculate sums for ARA rows
    mineral_cols = ['Houille', 'Lignite - charbon pauvre', 'Coke de houille']
    ara_values = ara_rows[mineral_cols].replace(['s', ''], '0').astype(float)
    ara_sum = ara_values.sum().sum()
    
    # Get the final aggregated value from output
    ara_output = float(output_df[output_df['reg_code'] == 'ARA']['Consommation de combustibles minéraux solides'].iloc[0])
    
    # Print debug information
    print(f"\nARA rows data:")
    print(ara_rows[mineral_cols])
    print("\nARA values after conversion:")
    print(ara_values)
    print(f"\nCalculated sum: {ara_sum}")
    print(f"Output value: {ara_output}")
    
    # Assert that sums match (with small tolerance for floating point arithmetic)
    assert abs(ara_sum - ara_output) < 0.01, \
        f"ARA region aggregation mismatch. Expected {ara_sum}, got {ara_output}"

def test_row_aggregation_invariants():
    """Test mathematical properties that must hold true for the aggregation."""
    input_df, output_df = load_real_data()
    
    # For each region in the output
    for reg_code in output_df['reg_code'].unique():
        if reg_code == 'FRA':  # Skip France total
            continue
            
        # Get all rows for this region from input
        reg_rows = input_df[input_df['reg_code'] == reg_code].copy()
        
        # Get the aggregated row from output
        reg_output = output_df[output_df['reg_code'] == reg_code]
        
        # Test mineral combustibles aggregation
        mineral_cols = ['Houille', 'Lignite - charbon pauvre', 'Coke de houille']
        reg_values = reg_rows[mineral_cols].replace(['s', ''], '0').astype(float)
        input_sum = reg_values.sum().sum()
        output_value = float(reg_output['Consommation de combustibles minéraux solides'].iloc[0])
        
        # Print debug information if the assertion would fail
        if abs(input_sum - output_value) >= 0.01:
            print(f"\nRegion {reg_code} data:")
            print(f"Original values:")
            print(reg_rows[mineral_cols])
            print(f"\nConverted values:")
            print(reg_values)
            print(f"Calculated sum: {input_sum}")
            print(f"Output value: {output_value}")
        
        assert abs(input_sum - output_value) < 0.01, \
            f"Region {reg_code} mineral combustibles mismatch. Expected {input_sum}, got {output_value}"

def test_total_conservation():
    """Test that the total energy consumption is conserved after aggregation."""
    input_df, output_df = load_real_data()
    
    # Calculate total consumption from input (excluding France total)
    input_regions = input_df[input_df['reg_code'] != 'FRA'].copy()
    # Group by region code and sum the total net values
    input_totals = input_regions.groupby('reg_code')['Total net'].apply(
        lambda x: x.replace(['s', ''], '0').astype(float).sum()
    )
    input_total = input_totals.sum()
    
    # Calculate total consumption from output (excluding France total)
    output_regions = output_df[output_df['reg_code'] != 'FRA'].copy()
    output_values = output_regions['Total net de la consommation'].replace(['s', ''], '0').astype(float)
    output_total = output_values.sum()
    
    # Print debug information
    print(f"\nTotal conservation test:")
    print(f"Input values by region (after grouping):")
    for reg_code in sorted(input_totals.index):
        print(f"{reg_code}: {input_totals[reg_code]}")
    print(f"\nOutput values by region:")
    for reg, val in zip(output_regions['reg_code'], output_values):
        print(f"{reg}: {val}")
    print(f"\nInput total: {input_total}")
    print(f"Output total: {output_total}")
    
    # Assert that totals match (with small tolerance for floating point arithmetic)
    assert abs(input_total - output_total) < 0.01, \
        f"Total energy conservation failed. Input total: {input_total}, Output total: {output_total}"

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--html=tests/reports/aggregation_report.html'])
