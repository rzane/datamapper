from __future__ import annotations

from typing import Any, Dict, Generic, Optional, Tuple, Type

from sqlalchemy.types import TypeEngine

from datamapper.changeset.types import Data
from datamapper.changeset.utils import dict_merge
from datamapper.model import Association, Model


class ChangesetDataWrapper(Generic[Data]):
    data: Data

    def get(self, field: str, default: Any = None) -> Optional[Any]:
        return self.attributes.get(field, default)

    def apply_changes(self, changes: dict) -> Data:
        raise NotImplementedError()  # pragma: no cover

    def association(self, field: str) -> Association:
        raise NotImplementedError()  # pragma: no cover

    @property
    def attributes(self) -> dict:
        raise NotImplementedError()  # pragma: no cover

    @property
    def types(self) -> Dict[str, Type]:
        raise NotImplementedError()  # pragma: no cover

    def __repr__(self) -> str:
        return self.data.__repr__()  # pragma: no cover


class DictChangesetDataWrapper(ChangesetDataWrapper[dict]):
    def __init__(self, data: Tuple[dict, dict]):
        self.data: dict = data[0]
        self._types: dict = data[1]

    def apply_changes(self, changes: dict) -> dict:
        return dict_merge(self.data, changes)

    def association(self, field: str) -> Association:
        raise ValueError("Data of type dict has no associations")

    @property
    def attributes(self) -> dict:
        return self.data  # pragma: no cover

    @property
    def types(self) -> Dict[str, Type]:
        return self._types


class ModelChangesetDataWrapper(ChangesetDataWrapper[Model]):
    def __init__(self, data: Model):
        self.data: Model = data

    def apply_changes(self, changes: dict) -> Model:
        attrs = {**self.data.attributes, **changes}
        return type(self.data)(**attrs)

    def association(self, field: str) -> Association:
        return self.data.association(field)

    @property
    def attributes(self) -> dict:
        return self.data.attributes  # pragma: no cover

    @property
    def types(self) -> dict:
        columns: dict = self.data.__table__.columns
        return {k: self._to_python_type(v.type) for (k, v) in columns.items()}

    @classmethod
    def _to_python_type(cls, type_: TypeEngine) -> Type:
        return type_.python_type
