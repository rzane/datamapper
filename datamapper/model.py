import typing
import sqlalchemy
from sqlalchemy import Table, Column, MetaData


class ModelMeta(type):
    def __new__(cls, name, bases, attrs):
        if attrs.get("__abstract__"):
            return super().__new__(cls, name, bases, attrs)

        assert "__tablename__" in attrs, "__tablename__ is required"
        assert "__metadata__" in attrs, "__metadata__ is required"

        columns = []
        for name, attr in list(attrs.items()):
            if isinstance(attr, Column):
                if attr.name is None:
                    attr.name = name
                columns.append(attr)
                attrs.pop(name)

        model = super().__new__(cls, name, bases, attrs)
        model.__table__ = Table(model.__tablename__, model.__metadata__, *columns)

        return model


class Model(metaclass=ModelMeta):
    __abstract__ = True
    __table__: Table
    __metadata__: MetaData

    def __init__(self, **attributes: typing.Any):
        self._attributes = attributes

    def __getattr__(self, key: str) -> typing.Any:
        if hasattr(self.__table__.columns, key):
            return self._attributes.get(key)
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{key}'"
            )
