"""Dumpty exception classes.
"""


class ValidationException(Exception):
    """Errors in validation"""


class ExtractException(Exception):
    def __init__(self, extract):
        self.extract = extract
        self.message = f"Exception extracting {extract.name}"
        super().__init__(self.message)
