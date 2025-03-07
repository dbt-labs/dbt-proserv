import json
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudResourceLookupError


class DbtCloudRunJobCreateOperator(DbtCloudRunJobOperator):
    """
    Custom operator to create a new dbt Cloud job if it doesn't exist.
    """

    # same template fields as DbtCloudRunJobOperator + some additional fields
    template_fields = (
        "dbt_cloud_conn_id",
        "job_id",
        "project_name",
        "environment_name",
        "job_name",
        "account_id",
        "trigger_reason",
        "steps_override",
        "schema_override",
        "additional_run_config",
        "default_steps_on_create",
    )

    default_job = {
        "id": None,
        "name": "<NAME_TO_UPDATE>",
        "account_id": "<ACCOUNT_ID_TO_UPDATE>",
        "project_id": "<PROJECT_ID_TO_UPDATE>",
        "environment_id": "<ENVIRONMENT_ID_TO_UPDATE>",
        "state": 1,
        "execute_steps": "<STEPS_TO_UPDATE>",
        "dbt_version": None,
        "deferring_environment_id": None,
        "lifecycle_webhooks": False,
        "lifecycle_webhooks_url": None,
        "generate_docs": False,
        "run_generate_sources": False,
        "job_completion_trigger_condition": None,
        "triggers": {
            "github_webhook": False,
            "git_provider_webhook": False,
            "on_merge": False,
            "schedule": False,
        },
        "settings": {"threads": 4, "target_name": "default"},
        "schedule": {
            "date": {
                "type": "interval_cron",
                "days": [0, 1, 2, 3, 4, 5, 6],
                "cron": "2 */12 * * 0,1,2,3,4,5,6",
            },
            "time": {"type": "every_hour", "interval": 12},
        },
        "execution": {"timeout_seconds": 0},
        "run_lint": False,
        "errors_on_lint_failure": True,
        "run_compare_changes": False,
        "compare_changes_flags": "--select state:modified",
        "deferring_job_definition_id": None,
    }

    def __init__(self, default_steps_on_create=["dbt parse"], **kwargs) -> None:
        super().__init__(**kwargs)
        self.default_steps_on_create = default_steps_on_create

    def create_job(self, account_id: int):
        """
        Create a new dbt Cloud job if it doesn't exist.

        This method retrieves connection details from the hook which was initialized
        in the parent operator with the connection ID provided during initialization.
        """
        print("Creating job")

        list_projects_responses = self.hook.list_projects(
            account_id=account_id,
            name_contains=self.project_name,
        )
        projects = [
            project
            for response in list_projects_responses
            for project in response.json()["data"]
            if project["name"] == self.project_name
        ]

        if len(projects) == 0:
            raise DbtCloudResourceLookupError(
                f"No project found with name {self.project_name}"
            )
        elif len(projects) > 1:
            raise DbtCloudResourceLookupError(
                f"Multiple projects found with name {self.project_name}"
            )
        project_id = projects[0]["id"]

        list_environments_responses = self.hook.list_environments(
            account_id=account_id,
            project_id=project_id,
        )
        environments = [
            environment
            for response in list_environments_responses
            for environment in response.json()["data"]
            if environment["name"] == self.environment_name
        ]

        if len(environments) == 0:
            raise DbtCloudResourceLookupError(
                f"No environment found with name {self.environment_name}"
            )
        elif len(environments) > 1:
            raise DbtCloudResourceLookupError(
                f"Multiple environments found with name {self.environment_name}"
            )
        environment_id = environments[0]["id"]

        job_payload = self.default_job
        job_payload["name"] = self.job_name
        job_payload["account_id"] = int(account_id)
        job_payload["project_id"] = int(project_id)
        job_payload["environment_id"] = int(environment_id)
        job_payload["execute_steps"] = self.default_steps_on_create

        # make the payload a json string as a dict raises an error
        job_payload = json.dumps(job_payload)

        job = self.hook._run_and_get_response(
            method="POST",
            endpoint=f"{account_id}/jobs/",
            payload=job_payload,
        )
        print(f"Job created with ID: {job.json()['data']['id']}")

    def execute(self, context):
        print("Checking if job exists")
        try:
            # Try to get the job by name using the hook
            self.hook.get_job_by_name(
                project_name=self.project_name,
                environment_name=self.environment_name,
                job_name=self.job_name,
            )
        except DbtCloudResourceLookupError:
            print("The job does not exist! Creating it now...")
            # Create the job and get its ID
            account_id = self.account_id
            if account_id is None:
                # Fallback to getting it from the connection object
                connection = self.hook.get_connection(self.dbt_cloud_conn_id)
                account_id = connection.login
            self.create_job(account_id=account_id)

        # Call the parent execute method to run the job
        return super().execute(context)
