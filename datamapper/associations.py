from typing import List, Dict, Any
from datamapper.model import Model, Association


class BelongsTo(Association):
    def where_clause(self, parents: List[Model]) -> dict:
        return {self.primary_key: [getattr(r, self.foreign_key) for r in parents]}

    def populate(self, parents: List[Model], children: List[Model], name: str) -> None:
        lookup: Dict[Any, Model] = {}
        for child in children:
            lookup[getattr(child, self.primary_key)] = child
        for parent in parents:
            key = getattr(parent, self.foreign_key)
            parent._associations[name] = lookup.get(key)


class HasOne(Association):
    def where_clause(self, parents: List[Model]) -> dict:
        return {self.foreign_key: [getattr(r, self.primary_key) for r in parents]}

    def populate(self, parents: List[Model], children: List[Model], name: str) -> None:
        lookup: Dict[Any, Model] = {}
        for child in children:
            lookup[getattr(child, self.foreign_key)] = child
        for parent in parents:
            key = getattr(parent, self.primary_key)
            parent._associations[name] = lookup.get(key)


class HasMany(Association):
    def where_clause(self, parents: List[Model]) -> dict:
        return {self.foreign_key: [getattr(r, self.primary_key) for r in parents]}

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
            parent._associations[name] = lookup.get(key, [])
