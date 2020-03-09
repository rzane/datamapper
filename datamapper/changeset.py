from __future__ import annotations
from typing import Any, List, Mapping, Type, Optional, Tuple
from marshmallow import Schema, fields
import sqlalchemy

from datamapper.model import Model


class MarshmallowValidator:
    def __init__(self, changeset: Changeset):
        self.changeset = changeset

    @classmethod
    def _sqla_to_marshmallow(cls, type_: Type) -> Type:
        if isinstance(type_, sqlalchemy.String):
            return fields.String
        if isinstance(type_, sqlalchemy.BigInteger):
            return fields.Integer
        else:
            return fields.Raw

    def validate(self) -> Tuple[bool, dict]:
        """
        Programatically build a Marshmallow schema and validate against it.
        """
        if not self.changeset.model:
            raise TypeError("TODO FIXME (model not initialized)")

        permitted_params = self.changeset.permitted_params()
        SchemaType = self._build_schema()
        errors = SchemaType().validate(permitted_params)
        if errors:
            return (False, errors)

        changes = SchemaType().dump(permitted_params)
        return (True, changes)

    def _build_schema(self) -> Type[Schema]:
        """
        Programatically build a Marshmallow schema.
        """
        schema_attributes = {
            field: self._build_field(field) for field in self.changeset.permitted
        }
        return type("schema", (Schema,), schema_attributes)

    def _build_field(self, name: str) -> Type[fields.Field]:
        """
        Programatically build a Marshmallow field.

        Attempts to map the sqlalchemy column type to the relevant field type.
        """
        column: Optional[Type] = self.changeset.model.__attributes__.get(name)
        field_type = (
            self._sqla_to_marshmallow(column.type) if column is not None else fields.Raw
        )

        type_args = {"required": name in self.changeset.required}
        return field_type(**type_args)


class Changeset:
    VALIDATOR_CLASS = MarshmallowValidator

    def __init__(self, model: Type[Model]) -> None:
        self.model = model

        self._changes: dict = {}
        self._errors: dict = {}
        self.params: dict = {}
        self.permitted: list = []
        self.required: set = set()
        self._evaluated = False

    def _update(self, **kwargs) -> Changeset:
        # Can replace this with an immutable collection library.
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        return self

    def cast(self, params: Mapping[str, Any], permitted: List[str]) -> Changeset:
        return self._update(params=params, permitted=permitted)

    def _evaluate(self) -> None:
        """
        Build the validation class and validate the params, finally setting
        the `changes` and `errors` attributes.
        """
        if self._evaluated:
            return

        changes = {}
        errors = {}

        (is_valid, result) = self.VALIDATOR_CLASS(self).validate()
        if is_valid:
            changes = result
        else:
            errors = result

        self._update(_changes=changes, _errors=errors, _evaluated=True)

    def validate_required(self, fields: List) -> Changeset:
        for f in fields:
            self.required.add(f)
        return self

    def permitted_params(self) -> dict:
        return {k: v for (k, v) in self.params.items() if k in set(self.permitted)}

    @property
    def has_error(self) -> bool:
        self._evaluate()
        return bool(self.errors)

    @property
    def is_valid(self) -> bool:
        self._evaluate()
        return not self.has_error

    @property
    def changes(self) -> dict:
        self._evaluate()
        return self._changes

    @property
    def errors(self) -> dict:
        self._evaluate()
        return self._errors

    def __repr__(self) -> str:
        m = f"is_valid={self.is_valid}" if self._evaluated else "unevaluated"
        return f"<Changeset model={self.model.__name__} {m}>"
