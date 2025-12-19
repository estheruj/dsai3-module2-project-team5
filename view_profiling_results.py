"""
Quick viewer for data profiling results
"""
import pandas as pd

# Load the profiling results
profile_df = pd.read_csv('data_profiling_results.csv')

print("\n" + "="*100)
print("DATA PROFILING RESULTS - SAMPLE VIEW")
print("="*100)

# Display key columns for each table
key_metrics = [
    'table_name', 'column_name', 'data_type', 'total_count', 
    'null_count', 'percent_null', 'distinct_count', 'percent_distinct',
    'mean', 'std_dev', 'min_value', 'max_value'
]

# Show first 10 rows with key metrics
print("\nFirst 10 Columns Profiled:")
print("-"*100)
print(profile_df[key_metrics].head(10).to_string())

print("\n\n" + "="*100)
print("SUMMARY STATISTICS BY TABLE")
print("="*100)

for table in profile_df['table_name'].unique():
    table_data = profile_df[profile_df['table_name'] == table]
    print(f"\n{table}:")
    print(f"  - Number of columns: {len(table_data)}")
    print(f"  - Avg null percentage: {table_data['percent_null'].mean():.2f}%")
    print(f"  - Columns with >10% nulls: {len(table_data[table_data['percent_null'] > 10])}")
    print(f"  - High cardinality columns (>50% distinct): {len(table_data[table_data['percent_distinct'] > 50])}")

print("\n\n" + "="*100)
print("CRITICAL DATA QUALITY ISSUES")
print("="*100)

# High null percentage
high_nulls = profile_df[profile_df['percent_null'] > 50][['table_name', 'column_name', 'percent_null']]
if len(high_nulls) > 0:
    print("\n⚠ Columns with >50% NULL values:")
    for _, row in high_nulls.iterrows():
        print(f"  - {row['table_name']}.{row['column_name']}: {row['percent_null']:.2f}%")
else:
    print("\n✓ No columns with >50% NULL values")

# Potential primary keys
pk_candidates = profile_df[profile_df['percent_distinct'] >= 99][['table_name', 'column_name', 'percent_distinct']]
if len(pk_candidates) > 0:
    print("\nℹ Potential Primary Key Columns (≥99% distinct):")
    for _, row in pk_candidates.iterrows():
        print(f"  - {row['table_name']}.{row['column_name']}: {row['percent_distinct']:.2f}%")

print("\n" + "="*100)
print("\nFor full details, see:")
print("  • data_profiling_results.csv")
print("  • data_profiling_report.html") 
print("  • data_profiling_summary.txt")
print("  • data_profiling_output/ directory for visualizations")
print()
