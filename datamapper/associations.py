from typing import Type, Union
from datamapper.model import Model
from importlib import import_module


class Association:
    def __init__(
        self, model: Union[str, Type[Model]], foreign_key: str, primary_key: str = "id"
    ):
        self._model = model
        self.foreign_key = foreign_key
        self.primary_key = primary_key

    @property
    def model(self) -> Type[Model]:
        if isinstance(self._model, str):
            mod, name = self._model.rsplit(".", 1)
            cls = getattr(import_module(mod), name)
            self._model = cls
        return self._model  # type: ignore
