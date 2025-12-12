from dagster import job, op, In, Out, Nothing


@op(required_resource_keys={"meltano"}, out=Out(Nothing))
def extract_load(context):
    # Run Meltano EL: tap-csv -> BigQuery
    context.resources.meltano.run("tap-csv target-bigquery")


@op(required_resource_keys={"dbt"}, ins={"start": In(Nothing)})
def transform_and_test(context):  # start is implicit; only used to enforce order
    # dbt build = run + test (serves as transform and DQ checks)
    context.resources.dbt.cli(["build"], context=context).wait()


@job
def elt_job():
    transform_and_test(start=extract_load())


