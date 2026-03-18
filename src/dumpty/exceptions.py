"""Dumpty exception classes."""



class ValidationError(Exception):
    """Errors in validation"""


class ExtractError(Exception):
    def __init__(self, extract, message: str | None = None):
        self.extract = extract
        if message is None:
            self.message = f"Exception extracting {extract.name}"
        else:
            self.message = message
        super().__init__(self.message)
