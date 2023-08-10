import time
import csv

from functools import wraps


def rate_limited_calls_per_min(max_per_minute):
    min_interval = 60.0 / float(max_per_minute)

    def decorate(func):
        last_time_called = [0.0]

        @wraps(func)
        def rate_limited_function(*args, **kwargs):
            elapsed = time.perf_counter() - last_time_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            ret = func(*args, **kwargs)
            last_time_called[0] = time.perf_counter()
            return ret

        return rate_limited_function

    return decorate


def filter_contacts(record_filters, contacts):
    print(f"Processing {len(contacts)} contacts", end="")
    skipped_contacts = []
    to_delete_contacts = []
    for contact in contacts:
        result = record_filters().handle(contact)
        if result:
            skipped_contacts.append(contact)
        else:
            to_delete_contacts.append(contact)
        print(".", end="", flush=True)
    print(".")
    print(f"{len(skipped_contacts)} keep, {len(to_delete_contacts)} delete")
    return skipped_contacts, to_delete_contacts


def save_to_files(record_filters, skipped_contacts, to_delete_contacts):
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
