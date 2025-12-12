from dagster import Definitions

# Import from local package root (/opt/dagster/app is on PYTHONPATH)
from resources import resources  # type: ignore
from jobs.elt_job import elt_job  # type: ignore
from schedules import daily_elt_schedule  # type: ignore

defs = Definitions(
	jobs=[elt_job],
	schedules=[daily_elt_schedule],
	resources=resources,
)


