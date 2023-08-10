# Handler Interface
class Handler:
    def set_next(self, handler):
        pass

    def handle(self, contact):
        pass


# Base Handler
class AbstractHandler(Handler):
    _next_handler = None

    def set_next(self, handler):
        self._next_handler = handler
        return handler

    def handle(self, contact):
        if self._next_handler:
            return self._next_handler.handle(contact)


# Condition 1: Any phone number of the record has a label
class PhoneNumberWithLabelHandler(AbstractHandler):
    def handle(self, contact):
        phone_numbers = contact.get("phoneNumbers", [])
        if any("contactGroupMembership" in phone for phone in phone_numbers):
            return ("Skipped", "Phone number has a label")
        return super().handle(contact)


# Condition 2: There is more than one phone number in the record
class MultiplePhoneNumbersHandler(AbstractHandler):
    def handle(self, contact):
        if len(contact.get("phoneNumbers", [])) > 1:
            return ("Skipped", "More than one phone number")
        return super().handle(contact)


# Condition 3: There is more than one label in the record
class MultipleLabelsHandler(AbstractHandler):
    def handle(self, contact):
        if len(contact.get("memberships", [])) > 1:
            return ("Skipped", "More than one label")
        return super().handle(contact)


# Chain Creation
phone_number_with_label_handler = PhoneNumberWithLabelHandler()
multiple_phone_numbers_handler = MultiplePhoneNumbersHandler()
multiple_labels_handler = MultipleLabelsHandler()


def record_filters():
    return phone_number_with_label_handler.set_next(
        multiple_phone_numbers_handler
    ).set_next(multiple_labels_handler)
