from abc import ABCMeta
from importlib import import_module
from typing import Any, Sequence, Mapping, Union, Type
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.sql.expression import ClauseElement
from datamapper.errors import NotLoadedError


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
        associations = {}
        for name, attr in list(attrs.items()):
            if isinstance(attr, Column):
                if attr.name is None:
                    attr.name = name

                attributes[name] = attr
                columns.append(attr)
                attrs.pop(name)

            if isinstance(attr, Association):
                associations[name] = attr
                attrs.pop(name)

        model = super(ModelMeta, cls).__new__(cls, name, bases, attrs)  # type: ignore
        model.__table__ = Table(model.__tablename__, model.__metadata__, *columns)
        model.__attributes__ = attributes
        model.__associations__ = associations

        return model


class Model(metaclass=ModelMeta):
    __abstract__ = True
    __table__: Table
    __metadata__: MetaData
    __attributes__: Mapping[str, Column]
    __associations__: Mapping[str, "Association"]

    def __init__(self, **attributes: Any):
        self._attributes: dict = attributes
        self._associations: dict = {}

    def __getattr__(self, key: str) -> Any:
        model_name = self.__class__.__name__

        if key in self.__attributes__:
            return self._attributes.get(key)
        elif key in self.__associations__:
            if key in self._associations:
                return self._associations[key]
            else:
                raise NotLoadedError(f"'{model_name}.{key}' is not loaded")
        else:
            raise AttributeError(f"'{model_name}' object has no attribute '{key}'")


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


class BelongsTo(Association):
    pass


class HasOne(Association):
    pass


class HasMany(Association):
    pass
