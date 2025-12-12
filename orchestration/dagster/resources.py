from pathlib import Path
from dagster_dbt import DbtCliResource
from dagster_meltano import MeltanoResource

# Project directories inside the container
DBT_PROJECT_DIR = Path("/opt/project/brazillian_ecommerce_project")
MELTANO_PROJECT_DIR = Path("/opt/project/meltano-csv")

resources = {
    "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    "meltano": MeltanoResource(project_dir=MELTANO_PROJECT_DIR),
}


