# Meltano ELT Pipeline

This directory contains the Meltano configuration and setup for extracting CSV data and loading it into BigQuery.

## Prerequisites

- Conda (or Miniconda/Anaconda)
- Google Cloud Platform (GCP) account with BigQuery access
- GCP service account credentials JSON file

## Setup Instructions

### 1. Create Conda Environment

Create the project environment from the environment file:

```bash
conda env create -f ../environment/proj2_environment.yml
```

### 2. Activate Environment

Activate the conda environment:

```bash
conda activate proj2
```

### 3. Configure GCP Credentials

Place your GCP service account key file in the project root directory. The key file should be named `dsai-module2-project-c41b83e002bf.json` (or update the path in `meltano.yml` accordingly).

**Note:** The credentials file is already added to `.gitignore` to prevent accidental commits.

### 4. Navigate to Meltano Project

Change to the meltano project directory:

```bash
cd meltano-csv
```

### 5. Install Meltano Plugins

Install the required extractors and loaders:

```bash
meltano install
```

This will install:
- `tap-csv`: CSV extractor
- `target-bigquery`: BigQuery loader

### 6. Run the ELT Pipeline

Execute the full pipeline to extract data from CSV files and load into BigQuery:

```bash
meltano run tap-csv target-bigquery
```

## Important Notes

### Encoding Configuration

The `product_category_name_translation` entity requires UTF-8 with BOM encoding. In `meltano.yml`, ensure this entity uses:

```yaml
- entity: product_category_name_translation
  path: ../data/kaggle-raw/product_category_name_translation.csv
  keys: [product_category_name]
  encoding: utf-8-sig  # Note: utf-8-sig is required for this file
```

**Note:** The `utf-8-sig` encoding handles the Byte Order Mark (BOM) character that may be present in the CSV file.

## Project Structure

- `meltano.yml`: Main configuration file defining extractors and loaders
- `catalog.json`: Auto-generated catalog of available streams
- `plugins/`: Installed Meltano plugins
- `extract/`: Extracted data (if using file-based extraction)
- `load/`: Loaded data artifacts
- `output/`: Pipeline output logs

## Data Sources

The pipeline extracts data from the following CSV files located in `../data/kaggle-raw/`:

- `olist_customers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `product_category_name_translation.csv`

## BigQuery Configuration

- **Project:** `dsai-module2-project`
- **Dataset:** `brazilian_ecommerce`
- **Location:** `US`
- **Method:** `batch_job`

### Verify Data Load

After running the pipeline, check your BigQuery console to confirm all tables were created successfully in the `brazilian_ecommerce` dataset.

