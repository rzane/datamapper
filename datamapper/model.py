from abc import ABCMeta
from typing import Any, Sequence, Mapping
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.sql.expression import ClauseElement


class ModelMeta(ABCMeta):
    def __new__(
        cls: type, name: str, bases: Sequence[type], attrs: dict
    ) -> "ModelMeta":
        if attrs.get("__abstract__"):
            return super(ModelMeta, cls).__new__(cls, name, bases, attrs)  # type: ignore

        assert "__tablename__" in attrs, "__tablename__ is required"
        assert "__metadata__" in attrs, "__metadata__ is required"

        columns = []
        attributes = {}
        for name, attr in list(attrs.items()):
            if isinstance(attr, Column):
                if attr.name is None:
                    attr.name = name

                attributes[name] = attr
                columns.append(attr)
                attrs.pop(name)

        model = super(ModelMeta, cls).__new__(cls, name, bases, attrs)  # type: ignore
        model.__table__ = Table(model.__tablename__, model.__metadata__, *columns)
        model.__attributes__ = attributes

        return model


class Model(metaclass=ModelMeta):
    __abstract__ = True
    __table__: Table
    __metadata__: MetaData
    __attributes__: Mapping[str, Column]

    def __init__(self, **attributes: dict):
        self._attributes = attributes

    def __getattr__(self, key: str) -> Any:
        if key in self.__attributes__:
            return self._attributes.get(key)
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{key}'"
            )

    @classmethod
    def to_query(cls) -> ClauseElement:
        return cls.__table__.select()

    @classmethod
    def deserialize_row(cls, row: Mapping) -> "Model":
        attributes = {}
        for name, column in cls.__attributes__.items():
            attributes[name] = row[column.name]
        return cls(**attributes)
