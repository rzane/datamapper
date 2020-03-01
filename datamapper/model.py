import typing


class Model:
    def __init__(self, **attributes: typing.Any):
        self._attributes = attributes

    def __getattr__(self, key: str) -> typing.Any:
        if key in self._attributes:
            return self._attributes[key]
        else:
            model_name = self.__class__.__name__
            message = f"'{model_name}' object has no attribute '{key}'"
            raise AttributeError(message)
