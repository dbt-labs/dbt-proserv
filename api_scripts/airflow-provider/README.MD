# Add a DbtCloudRunJobCreateOperator

This code contains a new operator called `DbtCloudRunJobCreateOperator`.

It accepts all the same arguments as [`DbtCloudRunJobOperator` from the dbt Cloud provider](https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/operators.html#trigger-a-dbt-cloud-job) but if the parameters provided are `project_name`, `environment_name` and `job_name` and no job exists with this name, it will create the the job in the relevant project and environment before running it. Next time the DAG is run, it will pick the existing job and not create a new one.

`default_steps_on_create` is an additional parameter that can be set to configure what steps would be set in the job if it is created by the Operator.

## Example

Here is an example of DAG calling this operator

```python
from datetime import datetime

from airflow.models import DAG
from dags.dbt_cloud_run_job_create_operator import DbtCloudRunJobCreateOperator

DBT_CLOUD_CONN_ID = "dbt_cloud"

with DAG(
    dag_id="dbt_cloud_run_job_create",
    default_args={"dbt_cloud_conn_id": DBT_CLOUD_CONN_ID},
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    trigger_dbt_cloud_job_run = DbtCloudRunJobCreateOperator(
        task_id="trigger_dbt_cloud_job_run",
        project_name="My dbt project",
        environment_name="Prod",
        job_name="My daily job that gets triggered from Airflow",
        check_interval=10,
        timeout=300,
        retry_from_failure=False,
        default_steps_on_create=["dbt build -s my_model+"],
    )

    trigger_dbt_cloud_job_run
```

## Requirements

- having the exising provider `apache-airflow-providers-dbt-cloud` installed with a version >= 4.2.0
- providing the permission to create jobs to the service token

## Installation

Just copy paste the code in `dbt_cloud_run_job_create_operator.py` into your `dags` folder and import it in your other DAG files (see example above).

