class Error(Exception):
    pass


class InvalidColumnError(Error):
    pass


class InvalidAssociationError(Error):
    pass


class NoResultsError(Error):
    pass


class MultipleResultsError(Error):
    pass


class NotLoadedError(Error):
    pass
