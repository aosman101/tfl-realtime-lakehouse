from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import duckdb, pandas as pd, great_expectations as gx

def gx_validate():
    """Run a minimal, idempotent GX validation on recent staging rows.

    Uses get-or-add semantics for data source, asset, suite and checkpoint so repeated
    DAG runs don't fail due to name collisions.
    """
    con = duckdb.connect('/opt/airflow/data/silver/tfl.duckdb')
    df = con.sql("select line_id, stop_id, time_to_station_s from staging.stg_arrivals limit 10000").df()

    if df.empty:
        print("GX: No rows returned from staging.stg_arrivals; skipping validation")
        return

    context = gx.get_context()

    # Data source & asset (idempotent)
    try:
        ds = context.data_sources.get("pd")
    except Exception:
        ds = context.data_sources.add_pandas("pd")

    try:
        asset = ds.get_asset("arrivals")
    except Exception:
        asset = ds.add_dataframe_asset("arrivals")

    # Batch definition (safe to re-add with same name)
    try:
        batch_def = asset.get_batch_definition("full")
    except Exception:
        batch_def = asset.add_batch_definition_whole_dataframe("full")

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    # Suite (ensure expectations exist once)
    try:
        suite = context.suites.get("arrivals_suite")
    except Exception:
        suite = context.suites.add(gx.core.expectation_suite.ExpectationSuite(name="arrivals_suite"))

    # Add expectations if missing
    expectation_types = {getattr(e, "type", None) for e in getattr(suite, "expectations", [])}
    if "expect_column_values_to_be_between" not in expectation_types:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="time_to_station_s", min_value=0, max_value=3600, severity="warning"
            )
        )
    if "expect_column_values_to_not_be_null" not in expectation_types:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="line_id", severity="warning"
            )
        )

    # Validation definition & checkpoint (idempotent)
    try:
        vd = context.validation_definitions.get("arrivals_validation")
    except Exception:
        vd = context.validation_definitions.add(
            gx.core.validation_definition.ValidationDefinition(
                name="arrivals_validation", data=batch_def, suite=suite
            )
        )

    try:
        checkpoint = context.checkpoints.get("checkpoint")
    except Exception:
        checkpoint = context.checkpoints.add(
            gx.checkpoint.checkpoint.Checkpoint(name="checkpoint", validation_definitions=[vd])
        )

    result = checkpoint.run()
    # Print a brief summary to logs
    try:
        print(result.describe())
    except Exception:
        pass

with DAG(
    dag_id="tfl_transform_dag",
    start_date=datetime(2025,1,1),
    schedule=None, catchup=False, tags=["tfl","dbt","quality"]
) as dag:
    # dbt build (emit lineage via dbt-ol)
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="dbt-ol build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
    )
    gx_check = PythonOperator(task_id="gx_validate", python_callable=gx_validate)

    dbt_build >> gx_check
