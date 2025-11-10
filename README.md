# tfl-realtime-lakehouse
Real-time transport data from London on your laptop. Airflow ingests TfL arrivals, stores them in Parquet format. dbt and DuckDB transform the data, Great Expectations validates it, and OpenLineage, along with Marquez, showcases the lineage.
