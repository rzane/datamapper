class Model:
    def __init__(self, **attributes):
        self._attributes = attributes

    def __getattr__(self, key):
        if key in self._attributes:
            return self._attributes[key]
        else:
            model_name = self.__class__.__name__
            message = f"'{model_name}' object has no attribute '{key}'"
            raise AttributeError(message)
