# Load data from dbt Cloud Audit logs

This pipeline is incremental and automatically tacks the latest audit log recorded. This cursor about the last load date is stored in a table in the destination

The example attached loads to DuckDB, but it is very easy to switch to another [destination](https://dlthub.com/docs/dlt-ecosystem/destinations/), which include

- The major DWs
- Filesystem and S3

Loading to another destination might require installing some additional Python package from `pip`

## Run the pipeline locally

- install `poetry`
- do a `poetry install` (once) to install the relevant packages
- set the env vars from `.envrc.example` in your local env
- do a `poetry run python dlt_dbt_cloud/dbt_cloud_audit_logs.py` to create the DuckDB data
