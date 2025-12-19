"""
Display all available profiling metrics
"""
import pandas as pd

# Load the profiling results
profile_df = pd.read_csv('data_profiling_results.csv')

print("\n" + "="*100)
print("ALL PROFILING METRICS CALCULATED")
print("="*100)

print("\n📊 Complete List of Metrics (Column Names in CSV):")
print("-"*100)

metrics = list(profile_df.columns)
for i, metric in enumerate(metrics, 1):
    print(f"{i:2d}. {metric}")

print("\n\n" + "="*100)
print("METRICS GROUPED BY CATEGORY")
print("="*100)

categories = {
    "Identification": [
        "table_name",
        "column_name"
    ],
    "Basic Information": [
        "data_type",
        "total_count"
    ],
    "Null/Missing Analysis": [
        "null_count",
        "not_null_count",
        "nan_count",
        "percent_null"
    ],
    "Uniqueness & Cardinality": [
        "distinct_count",
        "unique_count",
        "not_unique_count",
        "percent_distinct"
    ],
    "Statistical Measures": [
        "mean",
        "median",
        "std_dev"
    ],
    "Range & Boundaries": [
        "min_value",
        "max_value"
    ],
    "Quantiles": [
        "q1_25",
        "q2_50_median",
        "q3_75"
    ],
    "Zero Analysis": [
        "zero_count",
        "percent_zeros"
    ],
    "Length Metrics": [
        "min_length",
        "max_length",
        "avg_length"
    ],
    "Size Metrics": [
        "min_size",
        "max_size",
        "avg_size"
    ],
    "Frequency Analysis": [
        "most_frequent_value",
        "highest_frequency",
        "lowest_frequency",
        "top_10_values",
        "top_10_frequencies"
    ]
}

for category, metric_list in categories.items():
    print(f"\n📌 {category}:")
    for metric in metric_list:
        if metric in metrics:
            print(f"   ✓ {metric}")

print("\n\n" + "="*100)
print("STATISTICS")
print("="*100)
print(f"Total Metrics Calculated: {len(metrics)}")
print(f"Total Tables Profiled: {profile_df['table_name'].nunique()}")
print(f"Total Columns Profiled: {len(profile_df)}")
print(f"Total Rows Across All Tables: {profile_df['total_count'].sum():,}")

print("\n\n" + "="*100)
print("EXAMPLE: Full Profile of a Single Column")
print("="*100)

# Show complete profile of one interesting column
example = profile_df[
    (profile_df['table_name'] == 'olist_order_items_dataset') & 
    (profile_df['column_name'] == 'price')
].iloc[0]

print(f"\n🔸 Table: {example['table_name']}")
print(f"🔸 Column: {example['column_name']}\n")

for metric in metrics[2:]:  # Skip table_name and column_name
    value = example[metric]
    if pd.notna(value):
        if isinstance(value, float):
            print(f"   {metric:30s} : {value:.4f}")
        else:
            value_str = str(value)
            if len(value_str) > 80:
                value_str = value_str[:77] + "..."
            print(f"   {metric:30s} : {value_str}")

print("\n" + "="*100)
print("All metrics are available in data_profiling_results.csv")
print("="*100 + "\n")
