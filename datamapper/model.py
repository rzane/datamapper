from __future__ import annotations
import datamapper.query as query
import datamapper.errors as errors
from abc import ABCMeta
from importlib import import_module
from typing import Any, Sequence, Mapping, Dict, Union, Type, List, cast
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
        assert (
            name in cls.__attributes__
        ), f"Attribute '{name}' does not exist for model '{cls.__name__}'"
        return cls.__attributes__[name]

    @classmethod
    def association(cls, name: str) -> Association:
        assert (
            name in cls.__associations__
        ), f"Association '{name}' does not exist for model '{cls.__name__}'"
        return cls.__associations__[name]

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
                self.__raise_not_loaded(key)
            return self.__loaded_associations[key]
        else:
            self.__raise_invalid_attribute(key)

    def __setattr__(self, key: str, value: Any) -> None:
        if key in self.__attributes__:
            self.attributes[key] = value
        elif key in self.__associations__:
            self.__loaded_associations[key] = value

            for k, v in self.__associations__[key].values(value).items():
                self.attributes[k] = v
        else:
            super().__setattr__(key, value)

    def __raise_not_loaded(self, key: str) -> None:
        raise errors.NotLoadedError(
            f"Association '{key}' is not loaded for model '{self.__class__.__name__}'"
        )

    def __raise_invalid_attribute(self, key: str) -> None:
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{key}'"
        )


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
            self._model = getattr(import_module(mod), name)
        return cast(Type[Model], self._model)

    def values(self, record: Model) -> dict:
        return {}

    def to_query(self, parents: List[Model]) -> query.Query:
        raise NotImplementedError()  # pragma: no cover

    def populate(self, parents: List[Model], children: List[Model], name: str) -> None:
        raise NotImplementedError()  # pragma: no cover


class BelongsTo(Association):
    def values(self, record: Model) -> dict:
        return {self.foreign_key: getattr(record, self.primary_key)}

    def to_query(self, parents: List[Model]) -> query.Query:
        values = [getattr(r, self.foreign_key) for r in parents]
        where = {self.primary_key: values}
        return query.Query(self.model).where(**where)

    def populate(self, parents: List[Model], children: List[Model], name: str) -> None:
        lookup: Dict[Any, Model] = {}
        for child in children:
            lookup[getattr(child, self.primary_key)] = child
        for parent in parents:
            key = getattr(parent, self.foreign_key)
            setattr(parent, name, lookup.get(key))


class HasOne(Association):
    def to_query(self, parents: List[Model]) -> query.Query:
        values = [getattr(r, self.primary_key) for r in parents]
        where = {self.foreign_key: values}
        return self.model.to_query().where(**where)

    def populate(self, parents: List[Model], children: List[Model], name: str) -> None:
        lookup: Dict[Any, Model] = {}
        for child in children:
            lookup[getattr(child, self.foreign_key)] = child
        for parent in parents:
            key = getattr(parent, self.primary_key)
            setattr(parent, name, lookup.get(key))


class HasMany(Association):
    def to_query(self, parents: List[Model]) -> query.Query:
        values = [getattr(r, self.primary_key) for r in parents]
        where = {self.foreign_key: values}
        return self.model.to_query().where(**where)

    def populate(self, parents: List[Model], children: List[Model], name: str) -> None:
        lookup: Dict[Any, List[Model]] = {}
        for child in children:
            key = getattr(child, self.foreign_key)
            if key in lookup:
                lookup[key].append(child)
            else:
                lookup[key] = [child]
        for parent in parents:
            key = getattr(parent, self.primary_key)
            setattr(parent, name, lookup.get(key, []))
