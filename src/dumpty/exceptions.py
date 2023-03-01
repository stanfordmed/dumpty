"""Dumpty exception classes.
"""


class ValidationException(Exception):
    """Errors in validation"""


class ExtractException(Exception):
    def __init__(self, extract, message: str = None):
        self.extract = extract
        if message is None:
            self.message = f"Exception extracting {extract.name}"
        else:
            self.message = message
        super().__init__(self.message)
