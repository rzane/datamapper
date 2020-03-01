import typing
import sqlalchemy
from sqlalchemy import Table, Column


class ModelMeta(type):
    def __new__(cls, name, bases, attrs):
        model = super().__new__(cls, name, bases, attrs)

        if attrs.get("__abstract__"):
            return model

        assert "__tablename__" in attrs, "__tablename__ is required"
        assert "__metadata__" in attrs, "__metadata__ is required"

        columns = []
        for name, attr in attrs.items():
            if isinstance(attr, Column):
                if attr.name is None:
                    attr.name = name
                columns.append(attr)

        model.__table__ = Table(model.__tablename__, model.__metadata__, *columns)

        return model


class Model(metaclass=ModelMeta):
    __abstract__ = True

    def __init__(self, **attributes: typing.Any):
        self._attributes = attributes

    def __getattr__(self, key: str) -> typing.Any:
        if key in self._attributes:
            return self._attributes[key]
        else:
            model_name = self.__class__.__name__
            message = f"'{model_name}' object has no attribute '{key}'"
            raise AttributeError(message)
