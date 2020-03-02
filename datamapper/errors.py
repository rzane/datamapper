class Error(Exception):
    pass


class NoResultsError(Error):
    pass


class MultipleResultsError(Error):
    pass


class NotLoadedError(Error):
    pass
