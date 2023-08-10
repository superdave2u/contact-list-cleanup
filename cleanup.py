"""
Clean up contacts based on labels
"""
import csv
import pickle
import os.path
import time
import argparse

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

from filters import record_filters

SCOPES = ["https://www.googleapis.com/auth/contacts"]


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


def fetch_contacts(service, label_resource_name, max_retries=5):
    print("Fetching contacts", end="")
    contacts = []
    page_token = None
    retries = 0
    backoff_time = 1
    while True:
        try:
            result = (
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
            backoff_time = 1  # Resetting backoff time
        except HttpError as error:
            if (
                error.resp.status in (503, 429) and retries < max_retries
            ):  # Service Unavailable or Rate Limiting
                print(
                    f"\nError {error.resp.status}. Retrying in {backoff_time} seconds..."
                )
                time.sleep(backoff_time)
                retries += 1
                backoff_time *= 2  # Increasing backoff time for each retry
            else:
                print(f"\nAn error occurred: {error}")
                return None
    print(f"\nFound {len(contacts)} contacts")
    return contacts


def filter_contacts(contacts):
    print(f"Processing {len(contacts)} contacts", end="")
    skipped_contacts = []
    to_delete_contacts = []
    for contact in contacts:
        result = record_filters().handle(contact)
        if result:
            # Skip the contact
            skipped_contacts.append(contact)
        else:
            # Add to the deletion list
            to_delete_contacts.append(contact)
        print(".", end="", flush=True)
    print(".")
    print(f"{len(skipped_contacts)} keep, {len(to_delete_contacts)} delete")
    return skipped_contacts, to_delete_contacts


def save_to_files(skipped_contacts, to_delete_contacts):
    def format_contact(contact):
        name = contact["names"][0]["displayName"] if contact.get("names") else "Unknown"
        phone_numbers = contact.get("phoneNumbers", [])
        formatted_numbers = "|".join(
            "".join(filter(str.isdigit, number["value"]))[-10:]
            for number in phone_numbers
        )
        return formatted_numbers, name

    print("Writing skipped contacts to CSV...")
    with open("skipped_contacts.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Phone Numbers", "Name", "Reason"])
        for contact in skipped_contacts:
            formatted_numbers, name = format_contact(contact)
            reason = record_filters().handle(contact)[1]
            writer.writerow([formatted_numbers, name, reason])

    print("Writing to_delete contacts to CSV...")
    with open("to_delete_contacts.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Phone Numbers", "Name", "Reason"])
        for contact in to_delete_contacts:
            formatted_numbers, name = format_contact(contact)
            writer.writerow([formatted_numbers, name, "To delete"])


def delete_contacts(service, contacts, backoff=2, max_retries=6):
    print("Deleting contacts...")
    backoff_time = backoff
    for contact in contacts:
        retries = 0
        while True:
            try:
                print(f"Deleting contact: {contact['resourceName']}")
                service.people().deleteContact(
                    resourceName=contact["resourceName"]
                ).execute()
                break  # If successful, move on to the next contact
            except HttpError as error:
                if (
                    error.resp.status in (429, 503) and retries < max_retries
                ):  # Rate Limiting or Service Unavailable
                    print(
                        f"\nError {error.resp.status}. Retrying in {backoff_time} seconds..."
                    )
                    time.sleep(backoff_time)
                    retries += 1
                    backoff_time *= 2  # Increasing backoff time for each retry
                else:
                    print(f"\nAn error occurred: {error}")
                    # Handle other errors as needed, perhaps by skipping to the next contact
                    break
        backoff_time = backoff
    print("Deletion completed")


def init():
    parser = argparse.ArgumentParser(description="Supply a label to filter contacts.")
    parser.add_argument("--label", type=str, help="Label to filter contacts")

    args = parser.parse_args()
    label = args.label

    if label:
        return label
    else:
        print("No label provided")


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


def execute(service, label):
    label_id = get_label_id(service, label)
    contacts = fetch_contacts(service, label_id)
    skipped_contacts, to_delete_contacts = filter_contacts(contacts)
    save_to_files(skipped_contacts, to_delete_contacts)
    delete_contacts(service, to_delete_contacts)


def main():
    label = init()
    service = auth()
    execute(service, label)
    print("done")


if __name__ == "__main__":
    main()
