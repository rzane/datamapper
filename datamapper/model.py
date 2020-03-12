from __future__ import annotations

from abc import ABCMeta
from enum import Enum
from importlib import import_module
from typing import Any, Mapping, Sequence, Type, Union, cast

from sqlalchemy import Column, MetaData, Table
from sqlalchemy.ext.hybrid import hybrid_method

import datamapper.query as query
from datamapper.errors import NotLoadedError, UnknownAssociationError


class ModelMeta(ABCMeta):
    def __new__(
        cls: type, name: str, bases: Sequence[type], attrs: dict
    ) -> "ModelMeta":
        if attrs.get("__abstract__"):
            return super(ModelMeta, cls).__new__(cls, name, bases, attrs)  # type: ignore

        assert "__tablename__" in attrs, "__tablename__ is required"
        assert "__metadata__" in attrs, "__metadata__ is required"

        columns = []
        associations = {}
        for key, attr in list(attrs.items()):
            if isinstance(attr, Column):
                if attr.key is None:
                    attr.key = key
                if attr.name is None:
                    attr.name = key

                columns.append(attr)
                attrs.pop(key)

            if isinstance(attr, Association):
                associations[key] = attr
                attrs.pop(key)

        model = super(ModelMeta, cls).__new__(cls, name, bases, attrs)  # type: ignore
        model.__table__ = Table(model.__tablename__, model.__metadata__, *columns)
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
        names = cls.__table__.columns.keys()
        values = {name: row.get(name) for name in names}
        return cls(**values)

    @classmethod
    def association(cls, name: str) -> Association:
        try:
            return cls.__associations__[name]
        except KeyError:
            raise UnknownAssociationError(cls.__name__, name)

    @hybrid_method
    def to_query(binding: Union[Type[Model], Model]) -> query.Query:
        if isinstance(binding, type):
            return query.Query(binding)
        else:
            return query.Query(binding.__class__).where(id=binding.id)

    def __init__(self, **attributes: Any):
        columns = self.__class__.__table__.columns
        associations = self.__class__.__associations__

        self.attributes: dict = {}
        self.__loaded_associations: dict = {}

        for key, value in attributes.items():
            if key in columns or key in associations:
                setattr(self, key, value)
            else:
                self.__raise_invalid_attribute(key)

    def __getattr__(self, key: str) -> Any:
        columns = self.__class__.__table__.columns
        associations = self.__class__.__associations__
        loaded = self.__loaded_associations

        if key in columns:
            return self.attributes.get(key)
        elif key in associations:
            if key not in loaded:
                raise NotLoadedError(self.__class__.__name__, key)
            return loaded[key]
        else:
            self.__raise_invalid_attribute(key)

    def __setattr__(self, key: str, value: Any) -> None:
        columns = self.__class__.__table__.columns
        associations = self.__class__.__associations__

        if key in columns:
            self.attributes[key] = value
        elif key in associations:
            self.__loaded_associations[key] = value

            assoc = associations[key]
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
