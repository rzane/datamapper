class Error(Exception):
    pass


class InvalidColumnError(Error):
    def __init__(self, model: str, name: str):
        super().__init__("column '{name}' does not exist for model '{model}'")


class InvalidAssociationError(Error):
    def __init__(self, model: str, name: str):
        super().__init__("association '{name}' does not exist for model '{model}'")


class NoResultsError(Error):
    def __init__(self) -> None:
        super().__init__("expected at least one result but got none")


class MultipleResultsError(Error):
    def __init__(self, count: int) -> None:
        super().__init__(f"expected at most one result but got {count}")


class NotLoadedError(Error):
    def __init__(self, model: str, name: str):
        super().__init__(f"association '{name}' is not loaded for model '{model}'")


class MissingJoinError(Error):
    def __init__(self, parent: str, child: str):
        super().__init__(f"can't join '{child}' without joining '{parent}'")
