import pickle
import os
import time

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

from utils import rate_limited_calls_per_min


SCOPES = ["https://www.googleapis.com/auth/contacts"]


def auth():
    creds = None
    token_path = "token.pickle"
    if os.path.exists(token_path):
        with open(token_path, "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    return build("people", "v1", credentials=creds)


def get_contact_labels(service):
    results = service.contactGroups().list().execute()
    labels = [group["name"] for group in results.get("contactGroups", [])]
    return labels


def get_contact_group_resource_name(service, label_name):
    results = service.contactGroups().list().execute()
    for group in results.get("contactGroups", []):
        if group["name"] == label_name:
            return group["resourceName"]
    return None


def get_label_id(service, label_name):
    label_resource_name = get_contact_group_resource_name(service, label_name)
    if label_resource_name is None:
        print(f"No label found for name {label_name}")
        return None
    return label_resource_name


@rate_limited_calls_per_min(90)
def get_contacts_api_call(service, page_token):
    return (
        service.people()
        .connections()
        .list(
            resourceName="people/me",
            pageSize=2000,
            pageToken=page_token,
            requestMask_includeField="person.names,person.phoneNumbers,person.memberships",
        )
        .execute()
    )


def fetch_contacts(service, label_resource_name, initial_backoff=2, max_retries=6):
    print("Fetching contacts", end="")
    contacts = []
    page_token = None
    retries = 0
    backoff_time = initial_backoff
    while True:
        try:
            result = get_contacts_api_call(service, page_token)
            connections = result.get("connections", [])
            label_filtered_connections = [
                connection
                for connection in connections
                if any(
                    membership.get("contactGroupMembership", {}).get(
                        "contactGroupResourceName", ""
                    )
                    == label_resource_name
                    for membership in connection.get("memberships", [])
                )
            ]
            contacts += label_filtered_connections
            page_token = result.get("nextPageToken")
            print(".", end="", flush=True)
            if not page_token:
                break
            backoff_time = initial_backoff
        except HttpError as error:
            if error.resp.status in (503, 429) and retries < max_retries:
                print(f"Error {error.resp.status}.")
                print(f"  Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)
                retries += 1
                backoff_time *= 2
            else:
                print(f"\nAn error occurred: {error}")
                return None
    print(f"\nFound {len(contacts)} contacts")
    return contacts


@rate_limited_calls_per_min(90)
def delete_contact_api_call(service, contact_resource_name):
    service.people().deleteContact(resourceName=contact_resource_name).execute()


def delete_contacts(service, contacts, initial_backoff=2, max_retries=6):
    print("Deleting contacts...")
    total_contacts = len(contacts)
    backoff_time = initial_backoff
    for index, contact in enumerate(contacts):
        retries = 0
        while True:
            try:
                print(
                    f"Deleting {index + 1} of {total_contacts}: {contact['resourceName']}"
                )

                delete_contact_api_call(service, contact["resourceName"])
                break
            except HttpError as error:
                if error.resp.status in (429, 503) and retries < max_retries:
                    print(f"Error {error.resp.status}.")
                    print(f"  Retrying in {backoff_time} seconds...")
                    time.sleep(backoff_time)
                    retries += 1
                    backoff_time *= 2
                else:
                    print(f"\nAn error occurred: {error}")
                    break
        backoff_time = initial_backoff
    print("Deletion completed")
