from __future__ import annotations
import datamapper.query as query
import datamapper.errors as errors
from enum import Enum
from abc import ABCMeta
from importlib import import_module
from typing import Any, Sequence, Mapping, Union, Type, cast
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.ext.hybrid import hybrid_method


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
        for key, attr in list(attrs.items()):
            if isinstance(attr, Column):
                if attr.key is None:
                    attr.key = key
                if attr.name is None:
                    attr.name = key

                attributes[key] = attr
                columns.append(attr)
                attrs.pop(key)

            if isinstance(attr, Association):
                associations[key] = attr
                attrs.pop(key)

        model = super(ModelMeta, cls).__new__(cls, name, bases, attrs)  # type: ignore
        model.__table__ = Table(model.__tablename__, model.__metadata__, *columns)
        model.__attributes__ = attributes
        model.__associations__ = associations

        for key, assoc in model.__associations__.items():
            assoc.name = key
            assoc.owner = model

        return model


class Model(metaclass=ModelMeta):
    __abstract__ = True
    __table__: Table
    __metadata__: MetaData
    __attributes__: Mapping[str, Column]
    __associations__: Mapping[str, "Association"]

    @classmethod
    def deserialize(cls, row: Mapping) -> Model:
        return cls(**{name: row[col.name] for name, col in cls.__attributes__.items()})

    @classmethod
    def column(cls, name: str) -> Column:
        try:
            return cls.__attributes__[name]
        except KeyError:
            raise errors.UnknownColumnError(cls.__name__, name)

    @classmethod
    def association(cls, name: str) -> Association:
        try:
            return cls.__associations__[name]
        except KeyError:
            raise errors.UnknownAssociationError(cls.__name__, name)

    @hybrid_method
    def to_query(binding: Union[Type[Model], Model]) -> query.Query:
        if isinstance(binding, type):
            return query.Query(binding)
        else:
            return query.Query(binding.__class__).where(id=binding.id)

    def __init__(self, **attributes: Any):
        self.attributes: dict = {}
        self.__loaded_associations: dict = {}

        for key, value in attributes.items():
            if key in self.__attributes__ or key in self.__associations__:
                setattr(self, key, value)
            else:
                self.__raise_invalid_attribute(key)

    def __getattr__(self, key: str) -> Any:
        if key in self.__attributes__:
            return self.attributes.get(key)
        elif key in self.__associations__:
            if key not in self.__loaded_associations:
                raise errors.NotLoadedError(self.__class__.__name__, key)
            return self.__loaded_associations[key]
        else:
            self.__raise_invalid_attribute(key)

    def __setattr__(self, key: str, value: Any) -> None:
        if key in self.__attributes__:
            self.attributes[key] = value
        elif key in self.__associations__:
            self.__loaded_associations[key] = value

            assoc = self.__associations__[key]
            if isinstance(assoc, BelongsTo):
                related_key = getattr(value, assoc.related_key)
                self.attributes[assoc.owner_key] = related_key
        else:
            super().__setattr__(key, value)

    def __raise_invalid_attribute(self, key: str) -> None:
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{key}'"
        )


class Cardinality(Enum):
    ONE = "one"
    MANY = "many"


class Association:
    name: str
    owner: Type[Model]
    cardinality: Cardinality = Cardinality.ONE

    def __init__(
        self,
        related: Union[str, Type[Model]],
        foreign_key: str,
        primary_key: str = "id",
    ):
        self._related = related
        self._foreign_key = foreign_key
        self._primary_key = primary_key

    @property
    def related(self) -> Type[Model]:
        if isinstance(self._related, str):
            mod, name = self._related.rsplit(".", 1)
            self._related = getattr(import_module(mod), name)
        return cast(Type[Model], self._related)

    @property
    def owner_key(self) -> str:
        return self._primary_key

    @property
    def related_key(self) -> str:
        return self._foreign_key

    def values(self, value: Model) -> dict:
        return {}


class BelongsTo(Association):
    @property
    def owner_key(self) -> str:
        return self._foreign_key

    @property
    def related_key(self) -> str:
        return self._primary_key

    def values(self, value: Model) -> dict:
        return {self.owner_key: getattr(value, self.related_key)}


class HasOne(Association):
    pass


class HasMany(Association):
    cardinality: Cardinality = Cardinality.MANY
