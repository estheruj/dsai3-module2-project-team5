from dagster import ScheduleDefinition
from jobs.elt_job import elt_job

daily_elt_schedule = ScheduleDefinition(
    job=elt_job,
    cron_schedule="0 2 * * *",
    execution_timezone="UTC",
    name="daily_elt_02utc",
)


