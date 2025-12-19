"""
Comprehensive Data Profiling for Brazilian E-commerce Dataset
This script performs extensive profiling on all CSV files with statistical and visual analysis.
"""

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set style for better visualizations
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (15, 8)

# Define data directory
DATA_DIR = Path("data/kaggle-raw")

# List of CSV files to profile
CSV_FILES = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv"
]


def calculate_column_profile(df, column_name, table_name):
    """
    Calculate comprehensive profiling metrics for a single column.
    
    Parameters:
    -----------
    df : pd.DataFrame
        The dataframe containing the column
    column_name : str
        Name of the column to profile
    table_name : str
        Name of the table/dataset
        
    Returns:
    --------
    dict : Dictionary containing all profiling metrics
    """
    col_data = df[column_name]
    total_count = len(col_data)
    
    profile = {
        'table_name': table_name,
        'column_name': column_name,
        'data_type': str(col_data.dtype),
        'total_count': total_count,
        'null_count': col_data.isna().sum(),
        'not_null_count': col_data.notna().sum(),
        'nan_count': col_data.isna().sum(),  # Same as null in pandas
        'percent_null': (col_data.isna().sum() / total_count * 100) if total_count > 0 else 0,
    }
    
    # Distinct count and uniqueness
    profile['distinct_count'] = col_data.nunique()
    profile['unique_count'] = (col_data.value_counts() == 1).sum()
    profile['not_unique_count'] = total_count - profile['unique_count']
    profile['percent_distinct'] = (profile['distinct_count'] / total_count * 100) if total_count > 0 else 0
    
    # Remove null values for further analysis
    col_data_clean = col_data.dropna()
    
    # Numeric columns
    if pd.api.types.is_numeric_dtype(col_data):
        profile['min_value'] = col_data_clean.min() if len(col_data_clean) > 0 else None
        profile['max_value'] = col_data_clean.max() if len(col_data_clean) > 0 else None
        profile['mean'] = col_data_clean.mean() if len(col_data_clean) > 0 else None
        profile['median'] = col_data_clean.median() if len(col_data_clean) > 0 else None
        profile['std_dev'] = col_data_clean.std() if len(col_data_clean) > 0 else None
        
        # Quantiles
        if len(col_data_clean) > 0:
            profile['q1_25'] = col_data_clean.quantile(0.25)
            profile['q2_50_median'] = col_data_clean.quantile(0.50)
            profile['q3_75'] = col_data_clean.quantile(0.75)
        else:
            profile['q1_25'] = None
            profile['q2_50_median'] = None
            profile['q3_75'] = None
        
        # Count zeros
        profile['zero_count'] = (col_data == 0).sum()
        profile['percent_zeros'] = (profile['zero_count'] / total_count * 100) if total_count > 0 else 0
        
        # Length/Size metrics for numeric (in terms of string representation)
        col_str = col_data_clean.astype(str)
        profile['min_length'] = col_str.str.len().min() if len(col_str) > 0 else None
        profile['max_length'] = col_str.str.len().max() if len(col_str) > 0 else None
        profile['avg_length'] = col_str.str.len().mean() if len(col_str) > 0 else None
        
    # String/Object columns
    else:
        profile['min_value'] = None
        profile['max_value'] = None
        profile['mean'] = None
        profile['median'] = None
        profile['std_dev'] = None
        profile['q1_25'] = None
        profile['q2_50_median'] = None
        profile['q3_75'] = None
        profile['zero_count'] = 0
        profile['percent_zeros'] = 0
        
        # String length analysis
        if len(col_data_clean) > 0:
            str_lengths = col_data_clean.astype(str).str.len()
            profile['min_length'] = str_lengths.min()
            profile['max_length'] = str_lengths.max()
            profile['avg_length'] = str_lengths.mean()
            
            # Min/Max size (in bytes)
            profile['min_size'] = col_data_clean.astype(str).str.len().min()
            profile['max_size'] = col_data_clean.astype(str).str.len().max()
            profile['avg_size'] = col_data_clean.astype(str).str.len().mean()
        else:
            profile['min_length'] = None
            profile['max_length'] = None
            profile['avg_length'] = None
            profile['min_size'] = None
            profile['max_size'] = None
            profile['avg_size'] = None
    
    # Frequency analysis (for all types)
    if len(col_data_clean) > 0:
        value_counts = col_data.value_counts()
        profile['most_frequent_value'] = value_counts.index[0] if len(value_counts) > 0 else None
        profile['highest_frequency'] = value_counts.iloc[0] if len(value_counts) > 0 else 0
        profile['lowest_frequency'] = value_counts.iloc[-1] if len(value_counts) > 0 else 0
        
        # Top 10 most frequent values
        top_10 = value_counts.head(10)
        profile['top_10_values'] = list(top_10.index)
        profile['top_10_frequencies'] = list(top_10.values)
    else:
        profile['most_frequent_value'] = None
        profile['highest_frequency'] = 0
        profile['lowest_frequency'] = 0
        profile['top_10_values'] = []
        profile['top_10_frequencies'] = []
    
    return profile


def profile_all_datasets():
    """
    Profile all CSV files and return a comprehensive DataFrame with all metrics.
    
    Returns:
    --------
    pd.DataFrame : DataFrame containing profiling results for all columns in all tables
    """
    all_profiles = []
    datasets = {}
    
    print("=" * 80)
    print("STARTING DATA PROFILING ANALYSIS")
    print("=" * 80)
    print()
    
    for csv_file in CSV_FILES:
        file_path = DATA_DIR / csv_file
        table_name = csv_file.replace('.csv', '')
        
        print(f"Processing: {table_name}")
        
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            datasets[table_name] = df
            
            print(f"  - Shape: {df.shape}")
            print(f"  - Columns: {len(df.columns)}")
            
            # Profile each column
            for column in df.columns:
                profile = calculate_column_profile(df, column, table_name)
                all_profiles.append(profile)
            
            print(f"  ✓ Completed\n")
            
        except Exception as e:
            print(f"  ✗ Error reading {csv_file}: {str(e)}\n")
            continue
    
    # Create DataFrame from all profiles
    profile_df = pd.DataFrame(all_profiles)
    
    return profile_df, datasets


def create_profiling_visualizations(profile_df, datasets):
    """
    Create comprehensive visualizations for the profiling results.
    
    Parameters:
    -----------
    profile_df : pd.DataFrame
        DataFrame containing all profiling results
    datasets : dict
        Dictionary of all loaded datasets
    """
    print("\n" + "=" * 80)
    print("GENERATING VISUALIZATIONS")
    print("=" * 80)
    print()
    
    # Create output directory for plots
    output_dir = Path("data_profiling_output")
    output_dir.mkdir(exist_ok=True)
    
    # 1. Null Percentage Overview
    plt.figure(figsize=(16, 10))
    null_data = profile_df[['table_name', 'column_name', 'percent_null']].copy()
    null_data['table_column'] = null_data['table_name'] + '.' + null_data['column_name']
    null_data = null_data.sort_values('percent_null', ascending=False).head(30)
    
    sns.barplot(data=null_data, y='table_column', x='percent_null', palette='Reds_r')
    plt.title('Top 30 Columns by Null Percentage', fontsize=16, fontweight='bold')
    plt.xlabel('Null Percentage (%)', fontsize=12)
    plt.ylabel('Table.Column', fontsize=12)
    plt.tight_layout()
    plt.savefig(output_dir / '01_null_percentage_overview.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 01_null_percentage_overview.png")
    plt.close()
    
    # 2. Data Type Distribution
    plt.figure(figsize=(12, 6))
    dtype_counts = profile_df['data_type'].value_counts()
    sns.barplot(x=dtype_counts.index, y=dtype_counts.values, palette='viridis')
    plt.title('Distribution of Data Types Across All Columns', fontsize=16, fontweight='bold')
    plt.xlabel('Data Type', fontsize=12)
    plt.ylabel('Count', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(output_dir / '02_data_type_distribution.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 02_data_type_distribution.png")
    plt.close()
    
    # 3. Distinct Count Percentage
    plt.figure(figsize=(16, 10))
    distinct_data = profile_df[['table_name', 'column_name', 'percent_distinct']].copy()
    distinct_data['table_column'] = distinct_data['table_name'] + '.' + distinct_data['column_name']
    distinct_data = distinct_data.sort_values('percent_distinct', ascending=False).head(30)
    
    sns.barplot(data=distinct_data, y='table_column', x='percent_distinct', palette='Greens')
    plt.title('Top 30 Columns by Distinct Value Percentage', fontsize=16, fontweight='bold')
    plt.xlabel('Distinct Percentage (%)', fontsize=12)
    plt.ylabel('Table.Column', fontsize=12)
    plt.tight_layout()
    plt.savefig(output_dir / '03_distinct_percentage.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 03_distinct_percentage.png")
    plt.close()
    
    # 4. Zero Percentage (for numeric columns)
    numeric_data = profile_df[profile_df['zero_count'] > 0].copy()
    if len(numeric_data) > 0:
        plt.figure(figsize=(16, 10))
        numeric_data['table_column'] = numeric_data['table_name'] + '.' + numeric_data['column_name']
        numeric_data = numeric_data.sort_values('percent_zeros', ascending=False).head(20)
        
        sns.barplot(data=numeric_data, y='table_column', x='percent_zeros', palette='Oranges')
        plt.title('Top 20 Numeric Columns by Zero Percentage', fontsize=16, fontweight='bold')
        plt.xlabel('Zero Percentage (%)', fontsize=12)
        plt.ylabel('Table.Column', fontsize=12)
        plt.tight_layout()
        plt.savefig(output_dir / '04_zero_percentage.png', dpi=300, bbox_inches='tight')
        print("✓ Saved: 04_zero_percentage.png")
        plt.close()
    
    # 5. Data Quality Heatmap
    fig, axes = plt.subplots(2, 2, figsize=(18, 14))
    
    # Create pivot tables for heatmaps
    for table_name in profile_df['table_name'].unique()[:4]:  # First 4 tables
        idx = list(profile_df['table_name'].unique()).index(table_name)
        ax = axes[idx // 2, idx % 2]
        
        table_data = profile_df[profile_df['table_name'] == table_name][
            ['column_name', 'percent_null', 'percent_distinct', 'percent_zeros']
        ].set_index('column_name')
        
        sns.heatmap(table_data.T, annot=True, fmt='.1f', cmap='RdYlGn_r', 
                    ax=ax, cbar_kws={'label': 'Percentage'})
        ax.set_title(f'Data Quality Metrics: {table_name}', fontsize=12, fontweight='bold')
        ax.set_xlabel('Column', fontsize=10)
        ax.set_ylabel('Metric', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(output_dir / '05_quality_heatmap_part1.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 05_quality_heatmap_part1.png")
    plt.close()
    
    # Continue with remaining tables
    remaining_tables = profile_df['table_name'].unique()[4:]
    if len(remaining_tables) > 0:
        n_tables = len(remaining_tables)
        n_rows = (n_tables + 1) // 2
        fig, axes = plt.subplots(n_rows, 2, figsize=(18, 7 * n_rows))
        axes = axes.flatten() if n_rows > 1 else [axes]
        
        for idx, table_name in enumerate(remaining_tables):
            ax = axes[idx] if n_rows > 1 else axes[idx]
            
            table_data = profile_df[profile_df['table_name'] == table_name][
                ['column_name', 'percent_null', 'percent_distinct', 'percent_zeros']
            ].set_index('column_name')
            
            sns.heatmap(table_data.T, annot=True, fmt='.1f', cmap='RdYlGn_r', 
                        ax=ax, cbar_kws={'label': 'Percentage'})
            ax.set_title(f'Data Quality Metrics: {table_name}', fontsize=12, fontweight='bold')
            ax.set_xlabel('Column', fontsize=10)
            ax.set_ylabel('Metric', fontsize=10)
        
        # Hide unused subplots
        for idx in range(len(remaining_tables), len(axes)):
            axes[idx].axis('off')
        
        plt.tight_layout()
        plt.savefig(output_dir / '06_quality_heatmap_part2.png', dpi=300, bbox_inches='tight')
        print("✓ Saved: 06_quality_heatmap_part2.png")
        plt.close()
    
    # 6. Numeric Distributions (for selected numeric columns)
    numeric_cols = profile_df[profile_df['data_type'].str.contains('int|float', case=False, na=False)]
    
    for table_name, dataset in datasets.items():
        numeric_columns = dataset.select_dtypes(include=[np.number]).columns
        
        if len(numeric_columns) > 0:
            n_cols = min(len(numeric_columns), 6)  # Max 6 columns per plot
            n_rows = (n_cols + 2) // 3
            
            fig, axes = plt.subplots(n_rows, 3, figsize=(18, 5 * n_rows))
            
            # Handle different subplot configurations
            if n_rows > 1:
                axes = axes.flatten()
            elif n_rows == 1 and isinstance(axes, np.ndarray):
                axes = axes
            else:
                axes = [axes]
            
            for idx, col in enumerate(numeric_columns[:6]):
                if isinstance(axes, np.ndarray):
                    ax = axes[idx]
                else:
                    ax = axes[idx] if isinstance(axes, list) else axes
                    
                data = dataset[col].dropna()
                
                if len(data) > 0:
                    sns.histplot(data, kde=True, ax=ax, color='skyblue', edgecolor='black')
                    ax.set_title(f'{col}\n(mean: {data.mean():.2f}, std: {data.std():.2f})', 
                                fontsize=10)
                    ax.set_xlabel('')
                    ax.set_ylabel('Frequency')
            
            # Hide unused subplots
            axes_list = axes if isinstance(axes, (list, np.ndarray)) else [axes]
            for idx in range(len(numeric_columns[:6]), len(axes_list)):
                if isinstance(axes_list, np.ndarray) or isinstance(axes_list, list):
                    axes_list[idx].axis('off')
            
            plt.suptitle(f'Numeric Column Distributions: {table_name}', 
                        fontsize=14, fontweight='bold', y=1.00)
            plt.tight_layout()
            plt.savefig(output_dir / f'07_distributions_{table_name}.png', dpi=300, bbox_inches='tight')
            print(f"✓ Saved: 07_distributions_{table_name}.png")
            plt.close()
    
    # 7. Scatter plots for numeric relationships
    print("\n  Creating scatter plots for numeric relationships...")
    
    # Orders dataset - price vs freight
    if 'olist_order_items_dataset' in datasets:
        df_items = datasets['olist_order_items_dataset']
        if 'price' in df_items.columns and 'freight_value' in df_items.columns:
            plt.figure(figsize=(12, 8))
            sample_size = min(5000, len(df_items))
            sample_data = df_items.sample(n=sample_size, random_state=42)
            
            plt.scatter(sample_data['price'], sample_data['freight_value'], 
                       alpha=0.5, s=20, c='steelblue', edgecolors='black', linewidth=0.5)
            plt.xlabel('Price', fontsize=12)
            plt.ylabel('Freight Value', fontsize=12)
            plt.title('Price vs Freight Value (Order Items)', fontsize=16, fontweight='bold')
            plt.grid(True, alpha=0.3)
            
            # Add correlation
            corr = sample_data[['price', 'freight_value']].corr().iloc[0, 1]
            plt.text(0.05, 0.95, f'Correlation: {corr:.3f}', 
                    transform=plt.gca().transAxes, fontsize=12,
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
            
            plt.tight_layout()
            plt.savefig(output_dir / '08_scatter_price_vs_freight.png', dpi=300, bbox_inches='tight')
            print("✓ Saved: 08_scatter_price_vs_freight.png")
            plt.close()
    
    # Payments dataset - payment value vs installments
    if 'olist_order_payments_dataset' in datasets:
        df_payments = datasets['olist_order_payments_dataset']
        if 'payment_value' in df_payments.columns and 'payment_installments' in df_payments.columns:
            plt.figure(figsize=(12, 8))
            sample_size = min(5000, len(df_payments))
            sample_data = df_payments.sample(n=sample_size, random_state=42)
            
            plt.scatter(sample_data['payment_installments'], sample_data['payment_value'], 
                       alpha=0.5, s=20, c='coral', edgecolors='black', linewidth=0.5)
            plt.xlabel('Payment Installments', fontsize=12)
            plt.ylabel('Payment Value', fontsize=12)
            plt.title('Payment Installments vs Payment Value', fontsize=16, fontweight='bold')
            plt.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig(output_dir / '09_scatter_installments_vs_value.png', dpi=300, bbox_inches='tight')
            print("✓ Saved: 09_scatter_installments_vs_value.png")
            plt.close()
    
    # Reviews dataset - score distribution
    if 'olist_order_reviews_dataset' in datasets:
        df_reviews = datasets['olist_order_reviews_dataset']
        if 'review_score' in df_reviews.columns:
            plt.figure(figsize=(12, 8))
            score_counts = df_reviews['review_score'].value_counts().sort_index()
            
            sns.barplot(x=score_counts.index, y=score_counts.values, palette='coolwarm')
            plt.xlabel('Review Score', fontsize=12)
            plt.ylabel('Count', fontsize=12)
            plt.title('Distribution of Review Scores', fontsize=16, fontweight='bold')
            
            # Add percentages on bars
            total = score_counts.sum()
            for i, (idx, val) in enumerate(score_counts.items()):
                plt.text(i, val, f'{val:,}\n({val/total*100:.1f}%)', 
                        ha='center', va='bottom', fontsize=10)
            
            plt.tight_layout()
            plt.savefig(output_dir / '10_review_score_distribution.png', dpi=300, bbox_inches='tight')
            print("✓ Saved: 10_review_score_distribution.png")
            plt.close()
    
    print()


def generate_summary_report(profile_df, datasets):
    """
    Generate a comprehensive summary of findings from the data profiling.
    
    Parameters:
    -----------
    profile_df : pd.DataFrame
        DataFrame containing all profiling results
    datasets : dict
        Dictionary of all loaded datasets
        
    Returns:
    --------
    str : Formatted summary report
    """
    summary = []
    summary.append("\n" + "=" * 80)
    summary.append("DATA PROFILING SUMMARY OF FINDINGS")
    summary.append("=" * 80)
    summary.append("")
    
    # Overall statistics
    total_tables = profile_df['table_name'].nunique()
    total_columns = len(profile_df)
    total_rows = sum(df.shape[0] for df in datasets.values())
    
    summary.append("1. OVERALL DATASET STATISTICS")
    summary.append("-" * 80)
    summary.append(f"   • Total Tables: {total_tables}")
    summary.append(f"   • Total Columns: {total_columns}")
    summary.append(f"   • Total Rows (across all tables): {total_rows:,}")
    summary.append("")
    
    # Table-level statistics
    summary.append("2. TABLE-LEVEL SUMMARY")
    summary.append("-" * 80)
    for table_name, df in datasets.items():
        summary.append(f"   • {table_name}:")
        summary.append(f"     - Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        summary.append(f"     - Memory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    summary.append("")
    
    # Data Quality Issues
    summary.append("3. DATA QUALITY FINDINGS")
    summary.append("-" * 80)
    
    # High null percentage columns
    high_nulls = profile_df[profile_df['percent_null'] > 50].sort_values('percent_null', ascending=False)
    if len(high_nulls) > 0:
        summary.append("   ⚠ COLUMNS WITH HIGH NULL PERCENTAGE (>50%):")
        for _, row in high_nulls.head(10).iterrows():
            summary.append(f"     - {row['table_name']}.{row['column_name']}: "
                          f"{row['percent_null']:.2f}% null ({row['null_count']:,} of {row['total_count']:,})")
    else:
        summary.append("   ✓ No columns with high null percentage (>50%)")
    summary.append("")
    
    # Columns with all unique values (potential primary keys)
    unique_cols = profile_df[profile_df['percent_distinct'] >= 99]
    if len(unique_cols) > 0:
        summary.append("   ℹ POTENTIAL PRIMARY KEY COLUMNS (≥99% distinct values):")
        for _, row in unique_cols.iterrows():
            summary.append(f"     - {row['table_name']}.{row['column_name']}: "
                          f"{row['percent_distinct']:.2f}% distinct")
    summary.append("")
    
    # Columns with high zero percentage
    high_zeros = profile_df[profile_df['percent_zeros'] > 50].sort_values('percent_zeros', ascending=False)
    if len(high_zeros) > 0:
        summary.append("   ⚠ NUMERIC COLUMNS WITH HIGH ZERO PERCENTAGE (>50%):")
        for _, row in high_zeros.head(10).iterrows():
            summary.append(f"     - {row['table_name']}.{row['column_name']}: "
                          f"{row['percent_zeros']:.2f}% zeros")
    summary.append("")
    
    # Data type distribution
    summary.append("4. DATA TYPE ANALYSIS")
    summary.append("-" * 80)
    dtype_dist = profile_df['data_type'].value_counts()
    for dtype, count in dtype_dist.items():
        summary.append(f"   • {dtype}: {count} columns ({count/len(profile_df)*100:.1f}%)")
    summary.append("")
    
    # Statistical insights for numeric columns
    numeric_profiles = profile_df[profile_df['data_type'].str.contains('int|float', case=False, na=False)]
    if len(numeric_profiles) > 0:
        summary.append("5. NUMERIC COLUMN INSIGHTS")
        summary.append("-" * 80)
        
        # Columns with high variability (high std dev relative to mean)
        numeric_profiles_clean = numeric_profiles.dropna(subset=['mean', 'std_dev'])
        numeric_profiles_clean = numeric_profiles_clean[numeric_profiles_clean['mean'] != 0]
        numeric_profiles_clean['cv'] = numeric_profiles_clean['std_dev'] / numeric_profiles_clean['mean'].abs()
        high_var = numeric_profiles_clean.nlargest(5, 'cv')
        
        if len(high_var) > 0:
            summary.append("   Top 5 columns with highest variability (Coefficient of Variation):")
            for _, row in high_var.iterrows():
                summary.append(f"     - {row['table_name']}.{row['column_name']}: "
                              f"CV = {row['cv']:.2f} (mean: {row['mean']:.2f}, std: {row['std_dev']:.2f})")
        summary.append("")
    
    # String length analysis
    string_profiles = profile_df[~profile_df['data_type'].str.contains('int|float', case=False, na=False)]
    if len(string_profiles) > 0:
        summary.append("6. STRING COLUMN INSIGHTS")
        summary.append("-" * 80)
        
        # Longest average string columns
        string_profiles_clean = string_profiles.dropna(subset=['avg_length'])
        longest_strings = string_profiles_clean.nlargest(5, 'avg_length')
        
        if len(longest_strings) > 0:
            summary.append("   Top 5 columns with longest average string length:")
            for _, row in longest_strings.iterrows():
                summary.append(f"     - {row['table_name']}.{row['column_name']}: "
                              f"avg = {row['avg_length']:.1f} chars "
                              f"(min: {row['min_length']}, max: {row['max_length']})")
        summary.append("")
    
    # Cardinality insights
    summary.append("7. CARDINALITY ANALYSIS")
    summary.append("-" * 80)
    
    # Low cardinality columns (potential categorical)
    low_card = profile_df[
        (profile_df['distinct_count'] <= 20) & 
        (profile_df['distinct_count'] > 1)
    ].sort_values('distinct_count')
    
    if len(low_card) > 0:
        summary.append("   Low cardinality columns (≤20 distinct values - good for categorical analysis):")
        for _, row in low_card.head(10).iterrows():
            summary.append(f"     - {row['table_name']}.{row['column_name']}: "
                          f"{row['distinct_count']} distinct values")
    summary.append("")
    
    # Recommendations
    summary.append("8. RECOMMENDATIONS")
    summary.append("-" * 80)
    
    recommendations = []
    
    # Null handling
    if len(high_nulls) > 0:
        recommendations.append("   • Address high null percentages in columns - consider imputation or exclusion")
    
    # Data type optimization
    object_cols = profile_df[profile_df['data_type'] == 'object']
    low_card_objects = object_cols[object_cols['distinct_count'] <= 50]
    if len(low_card_objects) > 0:
        recommendations.append(f"   • Convert {len(low_card_objects)} low-cardinality object columns to categorical dtype for memory efficiency")
    
    # ID columns
    if len(unique_cols) > 0:
        recommendations.append(f"   • {len(unique_cols)} columns identified as potential primary/foreign keys - verify relationships")
    
    # High zero columns
    if len(high_zeros) > 0:
        recommendations.append("   • Investigate columns with high zero percentages - may indicate sparse data or data quality issues")
    
    # Outliers
    numeric_with_stats = numeric_profiles.dropna(subset=['mean', 'std_dev', 'max_value'])
    potential_outliers = numeric_with_stats[
        numeric_with_stats['max_value'] > (numeric_with_stats['mean'] + 3 * numeric_with_stats['std_dev'])
    ]
    if len(potential_outliers) > 0:
        recommendations.append(f"   • {len(potential_outliers)} numeric columns may contain outliers - consider outlier detection and treatment")
    
    if len(recommendations) > 0:
        for rec in recommendations:
            summary.append(rec)
    else:
        summary.append("   ✓ No major data quality issues detected")
    
    summary.append("")
    summary.append("=" * 80)
    summary.append("END OF SUMMARY")
    summary.append("=" * 80)
    
    return "\n".join(summary)


def main():
    """
    Main execution function for data profiling.
    """
    print("\n" + "🔍" * 40)
    print("  COMPREHENSIVE DATA PROFILING ANALYSIS")
    print("  Brazilian E-commerce Dataset")
    print("🔍" * 40 + "\n")
    
    # Step 1: Profile all datasets
    profile_df, datasets = profile_all_datasets()
    
    # Step 2: Save profiling results to CSV
    output_file = "data_profiling_results.csv"
    profile_df.to_csv(output_file, index=False)
    print(f"\n✓ Profiling results saved to: {output_file}")
    
    # Step 3: Create visualizations
    create_profiling_visualizations(profile_df, datasets)
    
    # Step 4: Generate and save summary report
    summary_report = generate_summary_report(profile_df, datasets)
    
    summary_file = "data_profiling_summary.txt"
    with open(summary_file, 'w') as f:
        f.write(summary_report)
    
    print(summary_report)
    print(f"\n✓ Summary report saved to: {summary_file}")
    
    # Step 5: Create detailed profiling HTML report
    print("\n" + "=" * 80)
    print("CREATING DETAILED HTML REPORT")
    print("=" * 80)
    
    html_content = generate_html_report(profile_df, datasets)
    html_file = "data_profiling_report.html"
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✓ Interactive HTML report saved to: {html_file}")
    
    print("\n" + "✅" * 40)
    print("  DATA PROFILING COMPLETE!")
    print("✅" * 40)
    print(f"\nGenerated files:")
    print(f"  • {output_file} - Complete profiling metrics in CSV format")
    print(f"  • {summary_file} - Summary of key findings")
    print(f"  • {html_file} - Interactive HTML report with detailed tables")
    print(f"  • data_profiling_output/ - Directory with all visualization plots")
    print()


def generate_html_report(profile_df, datasets):
    """
    Generate an HTML report with detailed profiling information.
    
    Parameters:
    -----------
    profile_df : pd.DataFrame
        DataFrame containing all profiling results
    datasets : dict
        Dictionary of all loaded datasets
        
    Returns:
    --------
    str : HTML content
    """
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Profiling Report - Brazilian E-commerce</title>
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }
            h1 {
                color: #2c3e50;
                border-bottom: 3px solid #3498db;
                padding-bottom: 10px;
            }
            h2 {
                color: #34495e;
                margin-top: 30px;
                border-bottom: 2px solid #95a5a6;
                padding-bottom: 5px;
            }
            h3 {
                color: #7f8c8d;
                margin-top: 20px;
            }
            table {
                border-collapse: collapse;
                width: 100%;
                background-color: white;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin: 20px 0;
            }
            th {
                background-color: #3498db;
                color: white;
                padding: 12px;
                text-align: left;
                font-weight: bold;
                position: sticky;
                top: 0;
            }
            td {
                border: 1px solid #ddd;
                padding: 10px;
            }
            tr:nth-child(even) {
                background-color: #f9f9f9;
            }
            tr:hover {
                background-color: #f0f0f0;
            }
            .metric {
                font-weight: bold;
                color: #2980b9;
            }
            .warning {
                color: #e74c3c;
                font-weight: bold;
            }
            .good {
                color: #27ae60;
                font-weight: bold;
            }
            .info-box {
                background-color: #ecf0f1;
                border-left: 4px solid #3498db;
                padding: 15px;
                margin: 20px 0;
            }
            .container {
                max-width: 1400px;
                margin: 0 auto;
                background-color: white;
                padding: 30px;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
            }
            .summary-stats {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
                margin: 20px 0;
            }
            .stat-card {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 20px;
                border-radius: 8px;
                text-align: center;
            }
            .stat-value {
                font-size: 32px;
                font-weight: bold;
                margin: 10px 0;
            }
            .stat-label {
                font-size: 14px;
                opacity: 0.9;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 Comprehensive Data Profiling Report</h1>
            <h2>Brazilian E-commerce Dataset</h2>
            
            <div class="info-box">
                <strong>Report Generated:</strong> """ + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') + """
            </div>
    """
    
    # Summary statistics
    html += """
            <h2>📈 Overall Statistics</h2>
            <div class="summary-stats">
                <div class="stat-card">
                    <div class="stat-label">Total Tables</div>
                    <div class="stat-value">""" + str(profile_df['table_name'].nunique()) + """</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Total Columns</div>
                    <div class="stat-value">""" + str(len(profile_df)) + """</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Total Rows</div>
                    <div class="stat-value">""" + f"{sum(df.shape[0] for df in datasets.values()):,}" + """</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Total Memory (MB)</div>
                    <div class="stat-value">""" + f"{sum(df.memory_usage(deep=True).sum() for df in datasets.values()) / 1024**2:.1f}" + """</div>
                </div>
            </div>
    """
    
    # Detailed profiling table by table
    html += """
            <h2>📋 Detailed Profiling Results by Table</h2>
    """
    
    for table_name in sorted(profile_df['table_name'].unique()):
        table_data = profile_df[profile_df['table_name'] == table_name].copy()
        
        html += f"""
            <h3>{table_name}</h3>
            <p><strong>Shape:</strong> {datasets[table_name].shape[0]:,} rows × {datasets[table_name].shape[1]} columns</p>
            <div style="overflow-x: auto;">
                <table>
                    <thead>
                        <tr>
                            <th>Column</th>
                            <th>Data Type</th>
                            <th>Total Count</th>
                            <th>Null %</th>
                            <th>Distinct</th>
                            <th>Distinct %</th>
                            <th>Mean</th>
                            <th>Std Dev</th>
                            <th>Min</th>
                            <th>Max</th>
                            <th>Zeros %</th>
                            <th>Most Frequent</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        
        for _, row in table_data.iterrows():
            null_class = 'warning' if row['percent_null'] > 50 else 'good' if row['percent_null'] == 0 else ''
            distinct_class = 'good' if row['percent_distinct'] > 90 else ''
            
            mean_str = f"{row['mean']:.2f}" if pd.notna(row['mean']) else 'N/A'
            std_str = f"{row['std_dev']:.2f}" if pd.notna(row['std_dev']) else 'N/A'
            min_str = str(row['min_value'])[:30] if pd.notna(row['min_value']) else 'N/A'
            max_str = str(row['max_value'])[:30] if pd.notna(row['max_value']) else 'N/A'
            freq_str = str(row['most_frequent_value'])[:40] if pd.notna(row['most_frequent_value']) else 'N/A'
            
            html += f"""
                        <tr>
                            <td><strong>{row['column_name']}</strong></td>
                            <td>{row['data_type']}</td>
                            <td>{row['total_count']:,}</td>
                            <td class="{null_class}">{row['percent_null']:.2f}%</td>
                            <td>{row['distinct_count']:,}</td>
                            <td class="{distinct_class}">{row['percent_distinct']:.2f}%</td>
                            <td>{mean_str}</td>
                            <td>{std_str}</td>
                            <td>{min_str}</td>
                            <td>{max_str}</td>
                            <td>{row['percent_zeros']:.2f}%</td>
                            <td>{freq_str}</td>
                        </tr>
            """
        
        html += """
                    </tbody>
                </table>
            </div>
        """
    
    html += """
        </div>
    </body>
    </html>
    """
    
    return html


if __name__ == "__main__":
    main()
