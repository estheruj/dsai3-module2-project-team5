### 1. Run dbt Transformations

After data is loaded into BigQuery, navigate to the dbt project and run transformations:

```bash
cd brazillian_ecommerce_project
```

Run **dbt clean** to clean any existing dependencies

```bash
dbt clean
```
Run **dbt deps** to install packages from packages.yml

```bash
dbt deps
```
Run **dbt run** to materializes dbt models (tables, views) 

```bash
dbt run
```
Run **dbt test** to executes the data quality tests defined in the dbt project

```bash
dbt test
```

### 2. Staging layer
SQL place in "\models\staging\"
SQL with prefix "stg_"

- Standardize naming
- Cast types
- Trim strings
- Basic null handling
- city/state/category → trim + upper/lower standardization (consistent choice)


### 3. Marts
SQL place in "\models\marts\"
SQL with prefix "dim_", "fact_", "util_"

Business logic + final star schema
- All joins, aggregations, derived metrics
- Conformed dimensions
- Fact grains locked and documented

### 4. Utility Marts
- To avoid repeating logic
- Not an intermediate layer — they are marts, consumed by final dims/facts.

### 5. Database Schema

#### Staging
\models\staging\
- stg_category_translation.sql
- stg_customers.sql
- stg_geolocation.sql
- stg_order_items.sql
- stg_order_payments.sql
- stg_order_reviews.sql
- stg_orders.sql
- stg_products.sql
- stg_sellers.sql

![Alt text](db_schema/Staging.png)


#### Dim
\models\marts\
- dim_category.sql
- dim_customers.sql
- dim_date.sql
- dim_location.sql
- dim_products.sql
- dim_sellers
#### Fact
\models\marts\
- fact_order_items.sql
- fact_orders.sql
- fact_payments.sql
- fact_reviews.sql
#### Util
\models\marts\
- util_geo_zip_centroid.sql
- util_orders_delivery_metrics.sql
- util_payments_by_order.sql
- util_products_enriched.sql

![Alt text](db_schema/Marts.png)



# 6. DBT Test

This section outlines the data quality checks and tests implemented in the Brazilian E-commerce dbt project. These tests are crucial for ensuring the reliability, integrity, and usability of the data models before they are consumed for analysis.

Our testing strategy focuses on validating the core assumptions of our data warehouse design, specifically in three key areas:

1.  **Integrity (Key Validation):** Ensuring primary keys are unique and non-null, and foreign key relationships (joins) hold true.
2.  **Validity (Value Constraints):** Confirming column values adhere to expected formats, ranges, or a fixed set of allowed values.
3.  **Completeness (Metric Checks):** Guaranteeing that critical metric columns contain at least one value.



## Implemented Tests Overview

The following standard and custom dbt tests are extensively used across the **Dimension (`dim_*`), Fact (`fact_*`), and Utility (`util_*`)** models to maintain a high level of data quality.

### Key and Referential Integrity Checks

| Model | Column | Test | Purpose |
| :--- | :--- | :--- | :--- |
| `dim_category` | `product_category_name` | `unique`, `not_null` | Ensures each category name is a unique, valid primary key. |
| `dim_date` | `date_key`, `full_date` | `unique`, `not_null` | Guarantees date keys and full dates are unique and complete. |
| `dim_location` | `location_key`, `zip_prefix` | `unique`, `not_null` | Validates the uniqueness of the location surrogate key and the zip prefix. |
| `dim_customers` | `location_key` | `relationships` (to `dim_location`) | Ensures every customer location links to an existing location record. |
| `dim_sellers` | `location_key` | `relationships` (to `dim_location`) | Ensures every seller location links to an existing location record. |
| `fact_order_items`| `product_id` | `relationships` (to `dim_products`) | Enforces that every order item corresponds to a valid product. |
| `fact_orders` | `customer_id` | `relationships` (to `dim_customers`) | Ensures every order is tied back to a valid customer. |
| `fact_reviews` | `order_id` | `relationships` (to `fact_orders`) | Ensures every review corresponds to a completed order. |

### ✅ Value and Completeness Checks

| Model | Column | Test | Purpose |
| :--- | :--- | :--- | :--- |
| `dim_date` | `is_weekend` | `accepted_values` (['TRUE', 'FALSE']) | Ensures the boolean flag contains only expected values. |
| `fact_order_items`| `price` | `not_null`, `dbt_utils.at_least_one` | Ensures the item price is present and confirms the column is populated across the dataset. |
| `fact_payments` | `payment_value` | `not_null`, `dbt_utils.at_least_one` | Ensures the payment value is present and confirms the column is populated across the dataset. |
| `fact_reviews` | `review_score` | `accepted_values` ([1, 2, 3, 4, 5]) | Restricts the review score to the valid range of 1 to 5. |

## 🧪 How to Run Tests

All data quality checks are executed using the `dbt test` command.

### Run All Tests

To execute all tests defined in the `schema.yml` files across the entire project:

```bash
dbt test