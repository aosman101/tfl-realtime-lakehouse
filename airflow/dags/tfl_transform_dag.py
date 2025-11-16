from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import duckdb, pandas as pd, great_expectations as gx

def gx_validate():
    con = duckdb.connect('/opt/airflow/data/silver/tfl.duckdb')
    df = con.sql("select line_id, stop_id, time_to_station_s from staging.stg_arrivals limit 10000").df()
    context = gx.get_context()
    ds = context.data_sources.add_pandas("pd")
    asset = ds.add_dataframe_asset("arrivals")
    batch_def = asset.add_batch_definition_whole_dataframe("full")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add(gx.core.expectation_suite.ExpectationSuite(name="arrivals_suite"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="time_to_station_s", min_value=0, max_value=3600, severity="warning"
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="line_id", severity="warning"
    ))
    vd = context.validation_definitions.add(
        gx.core.validation_definition.ValidationDefinition(name="arrivals_validation", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add(gx.checkpoint.checkpoint.Checkpoint(
        name="checkpoint", validation_definitions=[vd]
    ))
    result = checkpoint.run()
    print(result.describe())

with DAG(
    dag_id="tfl_transform",
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
