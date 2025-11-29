# Executable Plan: Kaggle → Supabase → BigQuery Pipeline

## Phase 1: Setup & Prerequisites

### Step 1.1: Install Required Tools
```bash
# Navigate to project directory
cd "/Users/estherng/Library/CloudStorage/OneDrive-Personal/Module 2/dsai3-module2-project-team5"

# Install Kaggle CLI
pip install kaggle

# Install Meltano (if not already installed)
pip install meltano

# Verify installations
kaggle --version
meltano --version
```

### Step 1.2: Setup Kaggle API Credentials
```bash
# 1. Go to https://www.kaggle.com/settings/account
# 2. Click "Create New API Token" - legacy API credentials (downloads kaggle.json)
# 3. Move the file to correct location:

mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json

# 4. Verify it works:
kaggle datasets list --max-results 5
```

### Step 1.3: Get Supabase Connection Details
```bash
# Connection details needed from: https://supabase.com/dashboard
# Find in Settings > Database > Connection String

# Example format (DO NOT share publicly):
# postgresql://postgres.USERNAME:PASSWORD@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres

# Save these environment variables in .env file
export SUPABASE_HOST="aws-1-ap-southeast-1.pooler.supabase.com"
export SUPABASE_PORT="5432"
export SUPABASE_DATABASE="postgres"
export SUPABASE_USER="postgres.USERNAME"
export SUPABASE_PASSWORD="your-password-here"
export SUPABASE_SCHEMA="public"
```

### Step 1.4: Verify BigQuery Credentials
```bash
# Your service account key already exists:
# /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module 2/big-data-478014-72045ca6f7e4.json

# Verify access:
gcloud auth activate-service-account --key-file="/Users/estherng/Library/CloudStorage/OneDrive-Personal/Module 2/dsai3-module-2-project-8f80674f6390.json"

# List datasets to verify:
bq ls --project_id=dsai3-module-2-project
```

---

## Phase 2: Download Kaggle Data

### Step 2.1: Download Brazilian Ecommerce Dataset
```bash
# Create data directory
mkdir -p /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw

# Download dataset
kaggle datasets download -d olistbr/brazilian-ecommerce \
  -p /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw

# Unzip files
cd /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw
unzip -o brazilian-ecommerce.zip

# List downloaded files
ls -lh
```

### Step 2.2: Verify Downloaded Data
```bash
# Check what files were downloaded
cd /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw

# Expected files:
# - olist_customers_dataset.csv
# - olist_geolocation_dataset.csv
# - olist_order_items_dataset.csv
# - olist_order_payments_dataset.csv
# - olist_order_reviews_dataset.csv
# - olist_orders_dataset.csv
# - olist_products_dataset.csv
# - olist_sellers_dataset.csv
# - product_category_name_translation.csv

# Check row counts
wc -l *.csv
```

---

## Phase 3: Setup Meltano Project Structure

### Step 3.1: Create Meltano Project
```bash
# Navigate to pipelines directory
cd "/Users/estherng/Library/CloudStorage/OneDrive-Personal/Module 2/5m-data-2.6-data-pipelines-orchestration"

# Initialize new Meltano project for Kaggle pipeline
meltano init meltano-kaggle
cd meltano-kaggle

# Verify structure
ls -la
```

### Step 3.2: Create Project Structure
```bash
# From meltano-kaggle/ directory:

# Create subdirectories for organization
mkdir -p extract load transform
mkdir -p data/raw data/staging data/warehouse
mkdir -p logs output
mkdir -p .env.secrets

# Create .gitignore
cat > .gitignore << 'EOF'
# Meltano
.meltano/
*.db
*.log

# Environment
.env
.env.*.secret

# Data
data/raw/
data/staging/
data/warehouse/

# Credentials
service-account*.json
kaggle.json

# Python
__pycache__/
*.pyc
.venv/
venv/

# OS
.DS_Store
*.swp
EOF

git add .gitignore
git commit -m "Add .gitignore"
```

---

## Phase 4: Configure Meltano Extractors & Loaders

### Step 4.1: Create meltano.yml Configuration
```bash
# From meltano-kaggle/ directory
cat > meltano.yml << 'EOF'
version: 1
default_environment: dev
project_id: kaggle-to-supabase-to-bigquery
project_name: kaggle-to-supabase-to-bigquery

environments:
  - name: dev
  - name: staging
  - name: prod

plugins:
  extractors:
    - name: tap-csv
      variant: meltanolabs
      pip_url: git+https://github.com/MeltanoLabs/tap-csv.git
      config:
        files:
          - entity: customers
            path: /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw/olist_customers_dataset.csv
            keys: [customer_id]
            encoding: utf-8
            
          - entity: geolocation
            path: /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw/olist_geolocation_dataset.csv
            keys: [zip_code_prefix]
            encoding: utf-8
            
          - entity: order_items
            path: /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw/olist_order_items_dataset.csv
            keys: [order_id, order_item_id]
            encoding: utf-8
            
          - entity: order_payments
            path: /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw/olist_order_payments_dataset.csv
            keys: [order_id, payment_sequential]
            encoding: utf-8
            
          - entity: order_reviews
            path: /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw/olist_order_reviews_dataset.csv
            keys: [review_id]
            encoding: utf-8
            
          - entity: orders
            path: /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw/olist_orders_dataset.csv
            keys: [order_id]
            encoding: utf-8
            
          - entity: products
            path: /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw/olist_products_dataset.csv
            keys: [product_id]
            encoding: utf-8
            
          - entity: sellers
            path: /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw/olist_sellers_dataset.csv
            keys: [seller_id]
            encoding: utf-8
            
          - entity: product_category_translation
            path: /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/dsai3-module2-project-team5/data/kaggle-raw/product_category_name_translation.csv
            keys: [product_category_name]
            encoding: utf-8

  loaders:
    # Step 1: Load to Supabase (Postgres)
    - name: target-postgres
      variant: transferwise
      pip_url: pipelinewise-target-postgres
      config:
        host: ${SUPABASE_HOST}
        port: ${SUPABASE_PORT}
        database: ${SUPABASE_DATABASE}
        username: ${SUPABASE_USER}
        password: ${SUPABASE_PASSWORD}
        schema: ${SUPABASE_SCHEMA:public}
        
    # Step 2: Load to BigQuery
    - name: target-bigquery
      variant: z3z1ma
      pip_url: git+https://github.com/z3z1ma/target-bigquery.git
      config:
        project: big-data-478014
        dataset: kaggle_olist
        location: US
        method: batch_job
        denormalized: true
        credentials_path: /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/big-data-478014-72045ca6f7e4.json
        batch_size: 104857600
        timeout: 600

EOF
cat meltano.yml
```

### Step 4.2: Create .env File with Credentials
```bash
# From meltano-kaggle/ directory
cat > .env << 'EOF'
# Supabase (Postgres) Configuration
export SUPABASE_HOST="aws-1-ap-southeast-1.pooler.supabase.com"
export SUPABASE_PORT="5432"
export SUPABASE_DATABASE="postgres"
export SUPABASE_USER="postgres.USERNAME"  # Replace with actual username
export SUPABASE_PASSWORD="your-password"  # Replace with actual password
export SUPABASE_SCHEMA="public"

# BigQuery Configuration
export TARGET_BIGQUERY_CREDENTIALS_PATH="/Users/estherng/Library/CloudStorage/OneDrive-Personal/Module 2/big-data-478014-72045ca6f7e4.json"
export TARGET_BIGQUERY_PROJECT="big-data-478014"
export TARGET_BIGQUERY_DATASET="kaggle_olist"
export TARGET_BIGQUERY_METHOD="batch_job"
export TARGET_BIGQUERY_DENORMALIZED="true"

# Meltano
export MELTANO_ENVIRONMENT="dev"
EOF

# IMPORTANT: Update .env with actual credentials!
echo "TODO: Update .env with actual Supabase credentials"
```

### Step 4.3: Install Extractors & Loaders
```bash
# From meltano-kaggle/ directory

# Add plugins to meltano.yml and install dependencies
meltano install

# Verify installation
meltano invoke tap-csv --help
meltano invoke target-postgres --help
meltano invoke target-bigquery --help
```

---

## Phase 5: Test Individual Components

### Step 5.1: Test CSV Extractor
```bash
# From meltano-kaggle/ directory

# Test discovery (list all tables/entities)
meltano invoke tap-csv --discover > catalog.json
cat catalog.json | head -50

# Test extraction to JSON (first 100 rows of customers table)
meltano run tap-csv target-jsonl --select customers-* 2>&1 | head -100
```

### Step 5.2: Create BigQuery Dataset
```bash
# Create dataset for Kaggle data (if not exists)
gcloud auth activate-service-account --key-file="/Users/estherng/Library/CloudStorage/OneDrive-Personal/Module 2/big-data-478014-72045ca6f7e4.json"

gcloud bq mk --dataset \
  --location=US \
  --description="Kaggle Brazilian Ecommerce Dataset" \
  big-data-478014:kaggle_olist

gcloud bq ls --project_id=big-data-478014
```

### Step 5.3: Setup Supabase Tables (Optional - Pre-create for Schema Control)
```bash
# Option A: Let Meltano create tables automatically (easier)
# Tables will be created on first load

# Option B: Pre-create tables manually in Supabase UI for schema control
# 1. Go to Supabase Dashboard > Table Editor
# 2. Create tables matching CSV structure (or let Meltano do it)
# 3. Define primary keys and relationships

# For now, proceed with automatic table creation
```

---

## Phase 6: Execute Full Pipeline

### Step 6.1: Load Data to Supabase (First Run - Small Test)
```bash
# From meltano-kaggle/ directory

# IMPORTANT: Source environment variables first!
source .env

# Run extraction from CSV to Supabase
# Start with ONE table to test
meltano run tap-csv target-postgres --select customers-* --log-level=DEBUG

# Monitor the output for errors
# Expected: Table "customers" created in Supabase with ~100K rows
```

### Step 6.2: Verify Data in Supabase
```bash
# Connect to Supabase via psql
psql postgresql://postgres.USERNAME:PASSWORD@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres

# In psql:
\dt public.*;                           # List all tables
SELECT COUNT(*) FROM public.customers;  # Verify data loaded
SELECT * FROM public.customers LIMIT 5; # Preview data
\q                                      # Exit
```

### Step 6.3: Load All Tables to Supabase
```bash
# From meltano-kaggle/ directory

# Load all tables to Supabase
source .env
meltano run tap-csv target-postgres --log-level=INFO

# Monitor output - this will take several minutes
# Expected tables in Supabase public schema:
# - customers, geolocation, order_items, order_payments
# - order_reviews, orders, products, sellers
# - product_category_translation
```

### Step 6.4: Extract from Supabase to BigQuery
```bash
# From 5m-data-2.6-data-pipelines-orchestration/meltano-kaggle/ directory

# Configure tap-postgres to read from Supabase
# Update meltano.yml with tap-postgres config:

cat >> meltano.yml << 'EOF'
    # Add tap-postgres to extract from Supabase
    - name: tap-postgres
      variant: meltanolabs
      pip_url: meltanolabs-tap-postgres
      config:
        host: ${SUPABASE_HOST}
        port: ${SUPABASE_PORT}
        database: ${SUPABASE_DATABASE}
        user: ${SUPABASE_USER}
        password: ${SUPABASE_PASSWORD}
        schema: ${SUPABASE_SCHEMA:public}
        # Select all tables
        select:
          - public-customers.*
          - public-geolocation.*
          - public-order_items.*
          - public-order_payments.*
          - public-order_reviews.*
          - public-orders.*
          - public-products.*
          - public-sellers.*
          - public-product_category_translation.*
EOF

# Install new extractor
meltano install

# Run pipeline: Supabase → BigQuery
source .env
meltano run tap-postgres target-bigquery --log-level=INFO

# Monitor - data flows from Supabase to BigQuery
# Tables created in big-data-478014.kaggle_olist dataset
```

---

## Phase 7: Verify Results

### Step 7.1: Verify BigQuery Data
```bash
# Activate service account
gcloud auth activate-service-account --key-file="/Users/estherng/Library/CloudStorage/OneDrive-Personal/Module 2/big-data-478014-72045ca6f7e4.json"

# List tables in kaggle_olist dataset
gcloud bq ls --dataset_id=big-data-478014:kaggle_olist

# Check row counts
gcloud bq query --use_legacy_sql=false '
  SELECT table_name, row_count FROM `big-data-478014.kaggle_olist.__TABLES__`
  ORDER BY row_count DESC;
'

# Preview data
gcloud bq query --use_legacy_sql=false '
  SELECT * FROM `big-data-478014.kaggle_olist.customers`
  LIMIT 10;
'
```

### Step 7.2: Verify Supabase Data
```bash
# Connect to Supabase and check row counts
psql postgresql://postgres.USERNAME:PASSWORD@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres << 'SQL'
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
SQL
```

### Step 7.3: Compare Row Counts
```bash
# Verify data consistency across all layers:
# 1. CSV files (source)
# 2. Supabase tables (middle layer)
# 3. BigQuery tables (destination)

# CSV source counts:
cd "/Users/estherng/Library/CloudStorage/OneDrive-Personal/Module 2/dsai3-module2-project-team5/data/kaggle-raw"
for file in *.csv; do echo "$file: $(( $(wc -l < "$file") - 1 )) rows"; done

# Compare with Supabase and BigQuery counts from steps above
```

---

## Phase 8: Schedule & Automate (Optional)

### Step 8.1: Create Run Script
```bash
# From meltano-kaggle/ directory
cat > run_pipeline.sh << 'EOF'
#!/bin/bash

# Kaggle → Supabase → BigQuery Pipeline

set -e  # Exit on error

echo "=== Starting Kaggle Pipeline ==="
echo "Step 1: Extracting from Kaggle CSV → Supabase"
source .env
meltano run tap-csv target-postgres --log-level=INFO

echo ""
echo "Step 2: Extracting from Supabase → BigQuery"
meltano run tap-postgres target-bigquery --log-level=INFO

echo ""
echo "=== Pipeline Complete ==="
echo "Check BigQuery dataset: big-data-478014:kaggle_olist"
EOF

chmod +x run_pipeline.sh
./run_pipeline.sh
```

### Step 8.2: Schedule with Cron (Optional)
```bash
# Edit crontab to schedule daily runs
crontab -e

# Add line to run pipeline daily at 2 AM:
# 0 2 * * * cd /Users/estherng/Library/CloudStorage/OneDrive-Personal/Module\ 2/5m-data-2.6-data-pipelines-orchestration/meltano-kaggle && ./run_pipeline.sh >> logs/pipeline-$(date +\%Y\%m\%d).log 2>&1
```

### Step 8.3: Integrate with Dagster Orchestration (Optional)
```bash
# Reference: /5m-data-2.6-data-pipelines-orchestration/dagster-orchestration/

# Create Dagster asset for this pipeline (future enhancement)
# Can orchestrate alongside existing HDB resale pipeline
```

---

## Phase 9: Troubleshooting & Common Issues

### Issue: "File not found" in tap-csv
```bash
# Solution: Verify file paths are correct
ls -la "/Users/estherng/Library/CloudStorage/OneDrive-Personal/Module 2/dsai3-module2-project-team5/data/kaggle-raw/"
```

### Issue: Supabase Connection Refused
```bash
# Solution: Verify credentials and network access
psql postgresql://postgres.USERNAME:PASSWORD@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres -c "SELECT 1;"
```

### Issue: BigQuery Authentication Failed
```bash
# Solution: Verify service account key
gcloud auth activate-service-account --key-file="/Users/estherng/Library/CloudStorage/OneDrive-Personal/Module 2/big-data-478014-72045ca6f7e4.json"
gcloud auth list
```

### Issue: Timeout or Memory Errors
```bash
# Solution: Reduce batch size in meltano.yml
# Change: batch_size: 104857600 → batch_size: 52428800
```

---

## Summary: Data Flow

```
┌─────────────────────────────────┐
│ Kaggle Brazilian Ecommerce      │
│ 9 CSV Files (~50MB)             │
└──────────────┬──────────────────┘
               │ (tap-csv)
               ▼
┌─────────────────────────────────┐
│ Supabase (PostgreSQL)           │
│ public schema                   │
│ 9 tables (~50MB)                │
└──────────────┬──────────────────┘
               │ (tap-postgres)
               ▼
┌─────────────────────────────────┐
│ Google BigQuery                 │
│ kaggle_olist dataset            │
│ 9 tables (~100MB denormalized)  │
└─────────────────────────────────┘
```

---

## Files Created/Modified

- `meltano.yml` - Main configuration
- `.env` - Environment variables (credentials)
- `.gitignore` - Git ignore patterns
- `run_pipeline.sh` - Automated run script
- `catalog.json` - Extracted schema (auto-generated)

---

## Next Steps After Pipeline Success

1. ✅ Verify all data in BigQuery
2. ✅ Create dbt models for transformation (reference: 5m-data-2.3-data-encoding-creation-flow/notebooks/)
3. ✅ Set up data validation tests
4. ✅ Document business logic and schemas
5. ✅ Share BigQuery access with team
6. ✅ Integrate with Dagster for orchestration
