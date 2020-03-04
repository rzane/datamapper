from typing import List, Dict, Any
from datamapper.query import Query
from datamapper.model import Model, Association


class BelongsTo(Association):
    def query(self, parents: List[Model]) -> Query:
        values = [getattr(r, self.foreign_key) for r in parents]
        where = {self.primary_key: values}
        return Query(self.model).where(**where)

    def populate(self, parents: List[Model], children: List[Model], name: str) -> None:
        lookup: Dict[Any, Model] = {}
        for child in children:
            lookup[getattr(child, self.primary_key)] = child
        for parent in parents:
            key = getattr(parent, self.foreign_key)
            setattr(parent, name, lookup.get(key))


class HasOne(Association):
    def query(self, parents: List[Model]) -> Query:
        values = [getattr(r, self.primary_key) for r in parents]
        where = {self.foreign_key: values}
        return Query(self.model).where(**where)

    def populate(self, parents: List[Model], children: List[Model], name: str) -> None:
        lookup: Dict[Any, Model] = {}
        for child in children:
            lookup[getattr(child, self.foreign_key)] = child
        for parent in parents:
            key = getattr(parent, self.primary_key)
            setattr(parent, name, lookup.get(key))


class HasMany(Association):
    def query(self, parents: List[Model]) -> Query:
        values = [getattr(r, self.primary_key) for r in parents]
        where = {self.foreign_key: values}
        return Query(self.model).where(**where)

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
