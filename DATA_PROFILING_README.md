# 📊 Data Profiling Analysis - Brazilian E-commerce Dataset

## Overview
This document provides a comprehensive data profiling analysis for all tables in the Brazilian E-commerce dataset. The analysis includes statistical metrics, data quality assessments, visualizations, and recommendations.

---

## 🎯 Executive Summary

### Dataset Overview
- **Total Tables**: 9
- **Total Columns**: 53
- **Total Rows**: 1,550,922
- **Total Memory**: ~308 MB

### Key Findings
1. **Data Quality**: Generally excellent with only 2 columns having >50% null values
2. **Data Types**: Balanced mix of object (62%), float (23%), and integer (15%) columns
3. **Primary Keys**: 12 columns identified as potential primary/foreign keys
4. **Data Completeness**: Most tables have 0% null values except reviews table

---

## 📁 Generated Files

### 1. **data_profiling_results.csv** (27 KB)
Complete profiling metrics for all 53 columns across 9 tables.

**Metrics Included**:
- **Basic Info**: table_name, column_name, data_type, total_count
- **Null Analysis**: null_count, not_null_count, nan_count, percent_null
- **Uniqueness**: distinct_count, unique_count, not_unique_count, percent_distinct
- **Statistical**: mean, median, std_dev, q1_25, q2_50, q3_75
- **Range**: min_value, max_value
- **Size Metrics**: min_length, max_length, avg_length, min_size, max_size, avg_size
- **Frequency**: most_frequent_value, highest_frequency, lowest_frequency
- **Top Values**: top_10_values, top_10_frequencies
- **Special Counts**: zero_count, percent_zeros

### 2. **data_profiling_report.html** (46 KB)
Interactive HTML report with styled tables showing all profiling metrics organized by table. 
- Color-coded warnings for data quality issues
- Sortable and scrollable tables
- Summary statistics cards
- Professional styling with gradient backgrounds

### 3. **data_profiling_summary.txt** (5 KB)
Structured text summary with:
- Overall dataset statistics
- Table-level summaries
- Data quality findings
- Statistical insights
- Recommendations

### 4. **data_profiling_output/** (Directory with 17 visualizations)

#### Visualization Files:

**Quality Overview Plots:**
- `01_null_percentage_overview.png` - Top 30 columns by null percentage
- `02_data_type_distribution.png` - Distribution of data types
- `03_distinct_percentage.png` - Top 30 columns by distinct values
- `04_zero_percentage.png` - Top 20 numeric columns with zeros

**Data Quality Heatmaps:**
- `05_quality_heatmap_part1.png` - Quality metrics for first 4 tables
- `06_quality_heatmap_part2.png` - Quality metrics for remaining tables

**Distribution Plots (per table):**
- `07_distributions_olist_customers_dataset.png`
- `07_distributions_olist_geolocation_dataset.png`
- `07_distributions_olist_order_items_dataset.png`
- `07_distributions_olist_order_payments_dataset.png`
- `07_distributions_olist_order_reviews_dataset.png`
- `07_distributions_olist_products_dataset.png`
- `07_distributions_olist_sellers_dataset.png`
- `07_distributions_product_category_name_translation.png`

**Relationship Scatter Plots:**
- `08_scatter_price_vs_freight.png` - Price vs Freight Value with correlation
- `09_scatter_installments_vs_value.png` - Payment Installments vs Value
- `10_review_score_distribution.png` - Distribution of review scores

---

## 📊 Detailed Profiling Metrics

### All Metrics Calculated:

| Category | Metrics |
|----------|---------|
| **Basic** | table_name, column_name, data_type, total_count |
| **Null Analysis** | null_count, not_null_count, percent_null, nan_count |
| **Uniqueness** | distinct_count, unique_count, percent_distinct |
| **Statistics** | mean, median, std_dev, min_value, max_value |
| **Quantiles** | q1 (25%), q2 (50%), q3 (75%) |
| **String Length** | min_length, max_length, avg_length |
| **Size** | min_size, max_size, avg_size |
| **Frequency** | most_frequent_value, highest_frequency, lowest_frequency |
| **Top Values** | top_10_most_frequent_values with frequencies |
| **Special Counts** | zero_count, percent_zeros |
| **Percentages** | percent_null, percent_zeros, percent_distinct |

---

## 🔍 Key Findings by Table

### 1. **olist_customers_dataset** (99,441 rows × 5 columns)
- ✅ **Perfect Data Quality**: 0% null values
- 🔑 **Primary Key**: customer_id (100% unique)
- 📍 **Geographic**: 27 states, 4,119 cities
- 💾 **Memory**: 26.59 MB

**Columns**: customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state

### 2. **olist_geolocation_dataset** (1,000,163 rows × 5 columns)
- ✅ **Complete Data**: No null values
- 🗺️ **High Precision**: 717K+ unique lat/lng combinations
- 📊 **Coverage**: 19,015 unique zip codes
- 💾 **Memory**: 130.26 MB (largest table)

**Columns**: geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state

### 3. **olist_order_items_dataset** (112,650 rows × 7 columns)
- ✅ **Complete**: 0% null values
- 💰 **Price Range**: $0.85 - $6,735 (mean: $120.65, std: $183.63)
- 🚚 **Freight Range**: $0 - $409.68 (mean: $19.99, std: $15.81)
- 🔗 **Relationships**: Links orders, products, and sellers
- 💾 **Memory**: 35.99 MB

**High Variability**: Price has CV=1.52 indicating significant price dispersion

### 4. **olist_order_payments_dataset** (103,886 rows × 5 columns)
- ✅ **Complete Data**: No nulls
- 💳 **Payment Types**: 5 distinct types (low cardinality - good for categorization)
- 💰 **Payment Value**: Mean $154.10, high variability (CV=1.41)
- 📊 **Installments**: Mean 2.85, range 1-29
- 💾 **Memory**: 16.23 MB

### 5. **olist_order_reviews_dataset** (99,224 rows × 7 columns)
- ⚠️ **Data Quality Issues**:
  - `review_comment_title`: **88.34% null**
  - `review_comment_message`: **58.70% null**
- ⭐ **Review Scores**: 5 distinct values (1-5 stars)
- 🔑 **Near-unique IDs**: review_id (99.18% distinct)
- 💾 **Memory**: 39.12 MB

**Recommendation**: Consider these columns optional or implement imputation strategy

### 6. **olist_orders_dataset** (99,441 rows × 8 columns)
- ✅ **Excellent Quality**: Only 0.62% avg null percentage
- 🔑 **Primary Keys**: order_id, customer_id (100% unique)
- 📅 **Timestamps**: High uniqueness (99.43% for purchase_timestamp)
- 📊 **Order Status**: 8 distinct statuses (low cardinality)
- 💾 **Memory**: 52.94 MB

### 7. **olist_products_dataset** (32,951 rows × 9 columns)
- ✅ **Low Nulls**: Average 0.83% null percentage
- 🔑 **Primary Key**: product_id (100% unique)
- ⚖️ **Weight**: Highly variable (CV=1.88, mean: 2.3kg, std: 4.3kg)
- 📸 **Photos**: 19 distinct photo quantities
- 💾 **Memory**: 6.30 MB

**High Variability**: Product weight shows extreme variation - possible outliers

### 8. **olist_sellers_dataset** (3,095 rows × 4 columns)
- ✅ **Perfect Data**: 0% null values
- 🔑 **Primary Key**: seller_id (100% unique)
- 📍 **Geographic**: Covers all 27 states
- 💾 **Memory**: 0.59 MB (smallest table)

### 9. **product_category_name_translation** (71 rows × 3 columns)
- ✅ **Perfect Quality**: No nulls
- 🔑 **All Unique**: All 3 columns have 100% unique values
- 🌐 **Translation Table**: Portuguese to English category names
- 💾 **Memory**: 0.01 MB

---

## 🚨 Critical Data Quality Issues

### High Null Percentages
1. **olist_order_reviews_dataset.review_comment_title**: 88.34% null
2. **olist_order_reviews_dataset.review_comment_message**: 58.70% null

**Impact**: Review text analysis will be limited
**Recommendation**: Mark as optional fields, focus on review_score (0% null)

### Potential Outliers
14 numeric columns may contain outliers (values > mean + 3*std):
- Product weights (CV=1.88)
- Prices (CV=1.52)
- Payment values (CV=1.41)

**Recommendation**: Implement outlier detection and treatment before modeling

---

## 💡 Recommendations

### 1. **Data Quality**
- ✅ Address high null percentages in review text fields
- ✅ Consider these columns optional or implement imputation
- ✅ Document the intentional nature of nulls (e.g., optional comments)

### 2. **Data Type Optimization**
- 🔧 Convert 5 low-cardinality object columns to `category` dtype:
  - `payment_type` (5 values)
  - `review_score` (5 values)
  - `order_status` (8 values)
  - `product_photos_qty` (19 values)
  - State columns (27 values)
- 💾 **Expected Savings**: ~15-20% memory reduction

### 3. **Relationships**
- 🔗 Verify 12 identified primary/foreign key relationships
- 🔗 Create entity-relationship diagram
- 🔗 Validate referential integrity

### 4. **Outlier Treatment**
- 📊 Investigate 14 numeric columns with potential outliers
- 📊 Use IQR method or domain knowledge to handle extremes
- 📊 Consider robust scaling for modeling

### 5. **Feature Engineering**
- 📅 Extract date components from timestamp columns (year, month, day, hour)
- 💰 Create price categories (low, medium, high)
- ⭐ Group review scores (positive: 4-5, neutral: 3, negative: 1-2)
- 🚚 Calculate price-to-weight ratios
- 📍 Create regional groupings from geographic data

---

## 📈 Statistical Highlights

### Highest Variability Columns (Coefficient of Variation)
1. **product_weight_g**: CV = 1.88 (highly dispersed)
2. **price**: CV = 1.52 (significant price range)
3. **payment_value**: CV = 1.41 (varied payment amounts)

### Longest String Columns (Average Length)
1. **review_comment_message**: 68.6 characters
2. **ID columns**: 32 characters (standardized UUID format)

### Low Cardinality Columns (Good for Categorization)
- **payment_type**: 5 values
- **review_score**: 5 values  
- **order_status**: 8 values
- **product_photos_qty**: 19 values
- **States**: 27 values

---

## 🎨 Visualizations Summary

### Quality Overview (4 plots)
- Null percentage ranking
- Data type distribution
- Distinct value percentage
- Zero value percentage

### Heatmaps (2 plots)
- Color-coded quality metrics by table
- Shows null%, distinct%, and zero% side-by-side

### Distributions (8 plots)
- Histograms with KDE for all numeric columns
- Grouped by table for easy comparison
- Includes mean and standard deviation

### Relationships (3 plots)
- Price vs Freight correlation analysis
- Payment patterns visualization
- Review score distribution with percentages

---

## 🔧 Scripts Provided

### 1. **data_profiling_analysis.py**
Main profiling script that:
- Loads all 9 CSV files
- Calculates all metrics
- Generates visualizations
- Creates reports

**Usage**:
```bash
python data_profiling_analysis.py
```

### 2. **view_profiling_results.py**
Quick viewer for results:
- Shows formatted sample of metrics
- Displays summary statistics
- Highlights critical issues

**Usage**:
```bash
python view_profiling_results.py
```

---

## 📊 Sample Data Profiling Output

### Example: Order Items - Price Column
```
Column: price
Data Type: float64
Total Count: 112,650
Null Count: 0 (0.0%)
Distinct Count: 5,968 (5.30%)
Mean: $120.65
Median: $74.99
Std Dev: $183.63
Min: $0.85
Max: $6,735.00
Q1 (25%): $39.90
Q2 (50%): $74.99
Q3 (75%): $134.90
Zero Count: 0 (0.0%)
Most Frequent: $59.90 (597 occurrences)
Top 10 Values: [59.90, 49.90, 39.90, 79.90, 99.90, 29.90, 69.90, 89.90, 19.90, 109.90]
```

---

## 🔍 How to Use the Results

### For Data Scientists
1. Review `data_profiling_results.csv` for complete metrics
2. Check visualizations in `data_profiling_output/`
3. Implement recommendations before modeling
4. Use insights for feature engineering

### For Business Analysts
1. Open `data_profiling_report.html` in browser
2. Review `data_profiling_summary.txt` for key findings
3. Use visualizations for presentations
4. Focus on data quality issues section

### For Data Engineers
1. Address high null columns in ETL pipeline
2. Implement data type optimization
3. Set up data quality monitoring
4. Validate primary/foreign key relationships

---

## 📝 Next Steps

1. ✅ **Review all generated files** ✓
2. 🔧 **Implement data type optimizations**
3. 🚨 **Address data quality issues**
4. 🔗 **Validate relationships**
5. 📊 **Create outlier treatment strategy**
6. 🎯 **Design feature engineering pipeline**
7. 📈 **Set up data quality monitoring**

---

## 📞 Support

For questions or issues with the profiling analysis:
- Review the comprehensive CSV file for raw metrics
- Check visualizations for graphical insights
- Read the summary text file for executive overview
- Open HTML report for interactive exploration

---

**Generated**: December 5, 2025
**Total Processing Time**: ~30 seconds
**Datasets Analyzed**: 9 tables, 53 columns, 1.5M+ rows
