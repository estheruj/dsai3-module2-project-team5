# Brazilian E-commerce Data Pipeline
A comprehensive end-to-end analytics engineering project that ingests and transforms Brazilian e-commerce data from Kaggle into an analytics-ready data warehouse in BigQuery, using dbt for dimensional modeling, Power BI for business insights, and Dagster for pipeline orchestration.

## Project Overview

This project implements an end-to-end analytics engineering pipeline using the 
[Brazilian E-commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

Raw transactional data is ingested into Google BigQuery and transformed using dbt into a **dimensional (star-schema) data warehouse** designed for scalable business analytics and BI consumption.

The pipeline includes:

- **Data Extraction & Loading (EL)**  
  Using Meltano to ingest raw CSV datasets and load them into Google BigQuery

- **Analytics Engineering & Transformation**  
  Using dbt to apply staging, dimensional modeling, business logic, and data quality tests

- **Business Intelligence & Analysis**  
  - Exploratory data analysis (EDA) performed using Python for initial data understanding
  - Analytics and dashboards enabled in Power BI based on an analytics-ready star schema


## Project Structure

```
.
├── data/
│   └── kaggle-raw/          # Raw CSV datasets from Kaggle
├── environment/
│   └── proj2_environment.yml  # Conda environment configuration
├── meltano-csv/             # Meltano ETL pipeline configuration
│   ├── meltano.yml          # Meltano project configuration
│   └── README.md            # Detailed Meltano setup instructions
├── brazillian_ecommerce_project/  # dbt project for data transformations
│   ├── models/              # dbt models (SQL transformations)
│   ├── analyses/            # Analysis queries
│   └── dbt_project.yml      # dbt project configuration
├── logs/                    # Application logs
└── dsai-module2-project-c41b83e002bf.json  # GCP service account credentials
```

## Prerequisites

- **Conda** (Miniconda or Anaconda)
- **Google Cloud Platform (GCP)** account with BigQuery access
- **GCP Service Account** credentials JSON file
- **Python 3.10+**

## Quick Start

### 1. Create Conda Environment

Create and activate the project environment:

```bash
conda env create -f environment/proj2_environment.yml
conda activate proj2
```

### 2. Configure GCP Credentials

Place your GCP service account key file (`dsai-module2-project-c41b83e002bf.json`) in the project root directory.

**Note:** The credentials file is already added to `.gitignore` to prevent accidental commits.

### 3. Set Up Meltano ETL Pipeline

Navigate to the Meltano project directory and follow the setup:

```bash
cd meltano-csv
meltano install
meltano run tap-csv target-bigquery
```

For detailed Meltano setup instructions, see [meltano-csv/README.md](meltano-csv/README.md).

### 4. Run dbt Transformations

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

## Data Sources

The project processes the following datasets from the Brazilian E-commerce dataset:

- `olist_customers_dataset.csv` - Customer information
- `olist_geolocation_dataset.csv` - Geographic location data
- `olist_order_items_dataset.csv` - Order items details
- `olist_order_payments_dataset.csv` - Payment information
- `olist_order_reviews_dataset.csv` - Customer reviews
- `olist_orders_dataset.csv` - Order information
- `olist_products_dataset.csv` - Product catalog
- `olist_sellers_dataset.csv` - Seller information
- `product_category_name_translation.csv` - Product category translations

## BigQuery Configuration

- **Project:** `dsai-module2-project`
- **Dataset:** `brazilian_ecommerce`
- **Location:** `US`

## Loading Data from Kaggle (Alternative Method)

If you need to download or load data directly from Kaggle using the Kaggle API, you can use the following Python code:

```python
# Install dependencies as needed:
# pip install kagglehub[pandas-datasets]
import kagglehub
from kagglehub import KaggleDatasetAdapter

# Set the path to the file you'd like to load
file_path = ""

# Load the latest version
df = kagglehub.load_dataset(
  KaggleDatasetAdapter.PANDAS,
  "olistbr/brazilian-ecommerce",
  file_path,
  # Provide any additional arguments like 
  # sql_query or pandas_kwargs. See the 
  # documentation for more information:
  # https://github.com/Kaggle/kagglehub/blob/main/README.md#kaggledatasetadapterpandas
)

print("First 5 records:", df.head())
```
## Analytics & Data Modeling Overview
The modeling approach follows Kimball dimensional modeling best practices, with clearly defined grains, facts, and conformed dimensions to support repeatable, trustworthy business metrics.

  **Business Questions Supported:**
  - Customer behavior and repeat purchase patterns
  - Customer experience drivers (delivery delays, reviews, complaints)
  - Product and category performance
  - Seller performance
  - Geographic demand vs supply mismatch

  **Chosen Business Processes & Grains**
  - Order lifecycle & delivery:	fact_order	(One row per order)
  - Sales & revenue:	fact_order_item	(One row per order item)
  - Payments:	fact_payment	One row per payment transaction
  - Reviews & satisfaction	fact_review	One row per review

  **Key modeling decision**
  - Customer retention and CLV are calculated using customer_unique_id (true customer identity across multiple orders)
  - Reusable transformations are implemented as utility models (`util_*`) within the marts layer. These models are consumed directly by final fact and dimension tables to avoid duplicated logic.

  ## Final Star Schema (Deliverables)
      ### Dimensions
      - `dim_customer`
      - `dim_seller`
      - `dim_products`
      - `dim_category`
      - `dim_location`
      - `dim_date`
      ### Facts
      - `fact_orders`
      - `fact_order_items`
      - `fact_payments`
      - `fact_reviews`
      ### Utility (within marts)
      - `util_products_enriched`
      - `util_payments_by_order`
      - `util_geo_zip_centroid`
      - `util_orders_delivery_metrics` (optional)

## Important Notes

### Encoding Configuration

The `product_category_name_translation` CSV file requires UTF-8 with BOM encoding. In `meltano-csv/meltano.yml`, ensure this entity uses `encoding: utf-8-sig` instead of `utf-8`.

### Windows-Specific Issues

If you encounter a `BrokenPipeError: [WinError 109]` on Windows during Meltano execution, this is typically a non-fatal error related to subprocess cleanup. Verify that your data was successfully loaded in BigQuery despite the error message.

## Documentation

- **Meltano Setup**: See [meltano-csv/README.md](meltano-csv/README.md) for detailed ETL pipeline setup
- **dbt Project**: See [brazillian_ecommerce_project/README.md](brazillian_ecommerce_project/README.md) for dbt transformation documentation

## Troubleshooting

### Verify Data Load

After running the Meltano pipeline, check your BigQuery console to confirm all tables were created successfully in the `brazilian_ecommerce` dataset.

### Environment Issues

If you encounter dependency issues, ensure you're using the correct conda environment:

```bash
conda activate proj2
```

## License

This project uses the Brazilian E-commerce Public Dataset by Olist, available on [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
