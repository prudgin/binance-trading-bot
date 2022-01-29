class SQLError(Exception):
    def __init__(self, err, message):
        self.err = err
        self.message = message


class APIError(Exception):
    def __init__(self, err, message):
        self.err = err
        self.message = message
