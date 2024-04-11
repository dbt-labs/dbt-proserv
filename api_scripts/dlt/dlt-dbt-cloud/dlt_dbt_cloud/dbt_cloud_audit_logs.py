import dlt
from datetime import datetime, timezone
from dlt.sources.helpers import requests
import pendulum

NUM_RECORDS = 50

def _parse_datetime_string_to_dbt_cloud_format(datetime_string: str):
    return pendulum.parse(datetime_string).isoformat(timespec='milliseconds')[:23] + 'Z'

def get_audit_logs(dbt_cloud_url: str, dbt_cloud_account_id: int, dbt_cloud_api_key: str, logged_at_start: str, logged_at_end: str):

    iso_logged_at_start = _parse_datetime_string_to_dbt_cloud_format(logged_at_start)
    iso_logged_at_end = _parse_datetime_string_to_dbt_cloud_format(logged_at_end)

    url_no_offset = f"https://{dbt_cloud_url}/api/v3/accounts/{dbt_cloud_account_id}/audit-logs/?limit={NUM_RECORDS}&logged_at_start={iso_logged_at_start}&logged_at_end={iso_logged_at_end}"
    response = requests.get(url_no_offset, headers={"Authorization": f"Bearer {dbt_cloud_api_key}"})
    response.raise_for_status()

    request_no_offset = response.json()
    total_count = request_no_offset["extra"]["pagination"]["total_count"]

    # we start with the max offset to be able to load records from the oldest to the newest
    # that way we can use the incremental loading feature and re-run the pipeline from the last success point
    offset = total_count // NUM_RECORDS * NUM_RECORDS

    while offset > 0:
        url_with_offset = f"{url_no_offset}&offset={offset}"
        response = requests.get(url_with_offset, headers={"Authorization": f"Bearer {dbt_cloud_api_key}"})
        response.raise_for_status()
        yield response.json()["data"]
        offset -= NUM_RECORDS
    
    # because we stop when offset is 0, we need to yield the first page that we queried initially
    yield request_no_offset["data"]

@dlt.resource(primary_key="id", write_disposition="append")
def audit_logs(
    dbt_cloud_url : str = dlt.secrets.value,
    dbt_cloud_account_id: int = dlt.secrets.value,
    dbt_cloud_api_key: str = dlt.secrets.value,
    created_at = dlt.sources.incremental("created_at", initial_value="2024-01-01T00:00:00Z"),

):
    print(f"Start value: {created_at.last_value}")

    for page in get_audit_logs(dbt_cloud_url, dbt_cloud_account_id, dbt_cloud_api_key, logged_at_start=created_at.start_value, logged_at_end=datetime.now(timezone.utc).isoformat()):
        yield page
        print(f"Last value: {created_at.last_value}")

# we keep the event_context as a complex/JSON because it varies widely
audit_logs.apply_hints(columns={"event_context": {"data_type": "complex"}})

pipeline = dlt.pipeline(
    pipeline_name="dbt_cloud_audit_logs",
    destination="duckdb",
    dataset_name="dbt_cloud_data",
)

pipeline.run(audit_logs())