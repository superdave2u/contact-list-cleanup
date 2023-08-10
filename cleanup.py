"""
Clean up contacts based on labels
"""
import argparse

from filters import record_filters
from utils import filter_contacts, save_to_files
from google_api import auth, get_label_id, fetch_contacts, delete_contacts


def init():
    parser = argparse.ArgumentParser(description="Supply a label to filter contacts.")
    parser.add_argument("--label", type=str, help="Label to filter contacts")
    args = parser.parse_args()
    label = args.label
    if label:
        return label
    else:
        quit("No label provided")


def execute(service, label):
    label_id = get_label_id(service, label)
    contacts = fetch_contacts(service, label_id)
    skipped_contacts, to_delete_contacts = filter_contacts(record_filters, contacts)
    save_to_files(record_filters, skipped_contacts, to_delete_contacts)
    delete_contacts(service, to_delete_contacts)


def main():
    label = init()
    service = auth()
    execute(service, label)
    print("done")


if __name__ == "__main__":
    main()
