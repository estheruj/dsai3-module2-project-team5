import os
import subprocess
from dagster import job, op, In, Out, Nothing
from resources import MELTANO_PROJECT_DIR


@op(required_resource_keys={"meltano"}, out=Out(Nothing))
def extract_load(context):
    # Clear Meltano cache to avoid stale configuration issues
    env = os.environ.copy()
    meltano_dir = MELTANO_PROJECT_DIR / ".meltano"
    if meltano_dir.exists():
        context.log.info(f"Clearing Meltano cache at {meltano_dir}")
        subprocess.run(
            ["rm", "-rf", str(meltano_dir)],
            check=True,
        )
    
    # Ensure Meltano plugins are installed
    context.log.info(f"Ensuring Meltano plugins are installed in {MELTANO_PROJECT_DIR}")
    install = subprocess.run(
        ["meltano", "install"],
        cwd=str(MELTANO_PROJECT_DIR),
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if install.stdout:
        context.log.info(install.stdout)
    if install.returncode != 0:
        if install.stderr:
            context.log.error(install.stderr)
        raise RuntimeError(f"meltano install failed with exit code {install.returncode}")

    # Run Meltano EL: tap-csv -> BigQuery via CLI to ensure clear success/failure
    context.log.info(f"Running Meltano EL in {MELTANO_PROJECT_DIR}")
    completed = subprocess.run(
        ["meltano", "run", "tap-csv", "target-bigquery"],
        cwd=str(MELTANO_PROJECT_DIR),
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    context.log.info(completed.stdout)
    if completed.returncode != 0:
        context.log.error(completed.stderr)
        raise RuntimeError(f"Meltano run failed with exit code {completed.returncode}")


@op(required_resource_keys={"dbt"}, ins={"start": In(Nothing)})
def transform_and_test(context):  # start is implicit; only used to enforce order
    # dbt build = run + test (serves as transform and DQ checks)
    context.resources.dbt.cli(["build"], context=context).wait()


@job
def elt_job():
    transform_and_test(start=extract_load())


