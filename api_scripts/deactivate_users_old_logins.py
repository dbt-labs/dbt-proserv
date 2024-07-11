from datetime import datetime
import requests
import logging

INACTIVE_STATE = 2

API_KEY = "TBD"
ACCOUNT_ID = TBD
HOST = "cloud.getdbt.com"
MAX_DAYS_INACTIVE = 90

# logging.getLogger().setLevel(logging.INFO)


def get_page_users_old_login(
    url: str, headers: dict, days_since_last_login: int
) -> (list[dict], int, datetime):
    """Query a page of the dbt Cloud API and return the users with last login more 
    than a certain number of days ago as well as the total number of users and the last login date for
    all the users in the list.

    Args:
        url (str): The URL to query the dbt Cloud API for users
        headers (dict): Headers for the API Call with the Authorization token
        days_since_last_login (int): The number of days before a user is considered inactive

    Returns:
        (list[dict]: The list of users with last login more than a certain number of days ago
        (int): The total number of users in the account, used to know when to stop querying
        (datetime): The last login date for all the users in the list, used to know when to stop querying
    """
    users = requests.get(url, headers=headers).json()

    users_old_login = []
    for user in users["data"]:
        last_login_date_time_str = user["last_login"][:19]
        last_login = datetime.strptime(last_login_date_time_str, "%Y-%m-%dT%H:%M:%S")
        if (datetime.now() - last_login).days > days_since_last_login:
            users_old_login.append(user)

    last_login_from_list_str = (
        users["data"][-1]["last_login"][:19] if users["data"] else "2024-01-01T00:00:00"
    )
    last_login_from_list = datetime.strptime(last_login_from_list_str, "%Y-%m-%dT%H:%M:%S")

    return (
        users_old_login,
        users["extra"]["pagination"]["total_count"],
        last_login_from_list,
    )


def get_users_old_login(
    account_id: int, api_key: str, host: str, days_since_last_login: int
) -> list[dict]:
    """Get the list of users with last login more than a certain number of days ago. 
    The API is paginated, so this function will query all the pages of the API.

    Args:
        account_id (int): The dbt Cloud Account ID
        api_key (str): The API Key for the dbt Cloud Account, can be a User key or Service token
        host (str): The dbt Cloud Host, e.g. cloud.getdbt.com or emea.dbt.com
        days_since_last_login (int): The number of days before a user is considered inactive

    Returns:
        list[dict]: A list of dictionaries with the users that have not logged in for more than a certain number of days
    """

    URL = f"https://{host}/api/v3/accounts/{account_id}/users/?limit=100&order_by=last_login"
    headers = {"Authorization": "Bearer " + api_key}

    users_old_login, total_users, last_login_from_list = get_page_users_old_login(
        URL, headers, days_since_last_login
    )
    retrieved_users = len(users_old_login)
    logging.info(
        f"Found {retrieved_users} users with last login more than {days_since_last_login} days ago for now"
    )

    while (retrieved_users < total_users) and (
        datetime.now() - last_login_from_list
    ).days > days_since_last_login:
        logging.info(
            f"Retrieving more users, already retrieved {retrieved_users} out of {total_users}"
        )
        URL = f"https://{host}/api/v3/accounts/{account_id}/users/?limit=100&order_by=last_login&offset={retrieved_users}"
        (
            additional_users_old_login,
            total_users,
            last_login_from_list,
        ) = get_page_users_old_login(URL, headers, days_since_last_login)

        # some users might have been deleted, so we need to stop if we didn't get any users
        if not additional_users_old_login:
            break

        retrieved_users += len(additional_users_old_login)
        users_old_login.extend(additional_users_old_login)
        logging.info(
            f"Found {retrieved_users} users with last login more than {days_since_last_login} days ago for now"
        )

    return users_old_login


def deactivate_user(
    user_id: int, permission_id: int, account_id: int, api_key: str, host: str
) -> dict:
    """Deactivate a user in dbt Cloud

    Args:
        user_id (int): The internal User ID in dbt Cloud
        permission_id (int): The internal Permission ID in dbt Cloud
        account_id (int): The dbt Cloud Account ID
        api_key (str): THe API Key for the dbt Cloud Account, can be a User key or Service token
        host (str): The dbt Cloud Host, e.g. cloud.getdbt.com or emea.dbt.com

    Returns:
        dict: The response from the API call to deactivate the user
    """
    headers = {"Authorization": "Bearer " + api_key}

    data_deactivate = {
        "account_id": account_id,
        "id": permission_id,
        "user_id": user_id,
        "state": INACTIVE_STATE,
    }

    delete_perm = requests.post(
        f"https://{host}/api/v2/accounts/{account_id}/permissions/{permission_id}/",
        headers=headers,
        json=data_deactivate,
    )
    return delete_perm.json()


inactive_users = get_users_old_login(ACCOUNT_ID, API_KEY, HOST, MAX_DAYS_INACTIVE)

for user in inactive_users:
    permissions = user["permissions"][0]
    logging.info(
        f"Deactivating user {user['id']} ({user['email']}) with permission {permissions['id']}"
    )
    # TODO: uncomment the following line to deactivate the users
    # deactivate_user(user['id'], permissions['id'], ACCOUNT_ID, API_KEY, HOST)
