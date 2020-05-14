from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type

import sqlalchemy
from marshmallow import Schema, ValidationError, fields

from datamapper.changeset.types import FieldValidator


class MarshmallowValidator:
    def __init__(
        self,
        params: Dict[str, Any],
        types: Dict[str, Any],
        permitted: Set[str],
        required: Set[str],
        field_validators: Dict[str, List[FieldValidator]],
    ):
        self.params = params
        self.types = types
        self.permitted = permitted
        self.required = required
        self.field_validators = field_validators

    @classmethod
    def _to_marshmallow_field_type(cls, type_: Type) -> Type:
        """
        Maps types to marshmallow field types.
        """
        if isinstance(type_, sqlalchemy.String):
            return fields.String
        if isinstance(type_, sqlalchemy.BigInteger):
            return fields.Integer
        elif type_ in Schema.TYPE_MAPPING:
            return Schema.TYPE_MAPPING[type_]
        else:
            return fields.Raw

    def validate(self) -> Tuple[bool, dict]:
        """
        Programatically build a Marshmallow schema and validate against it.

        Returns (True, changes) | (False, errors)
        """
        SchemaType = self._build_schema()
        errors = SchemaType().validate(self.params)
        if errors:
            return (False, errors)

        changes = SchemaType().dump(self.params)
        return (True, changes)

    def _build_schema(self) -> Type[Schema]:
        """
        Programatically build a Marshmallow Schema object.
        """
        all_fields = set(self.permitted).union(self.required)
        field_definitions: Dict[str, fields.Field] = {
            field: self._build_field(field) for field in all_fields
        }
        return Schema.from_dict(field_definitions)  # type: ignore

    def _build_field(self, name: str) -> fields.Field:
        """
        Programatically build a Marshmallow Field object.

        Attempts to map the sqlalchemy column type to the relevant field type.
        """

        def wrap_validator(validator: FieldValidator) -> Callable:
            """
            Turn a generic validation function into a marshmallow-flavored one that
            raises a ValidationError with the message in question.
            """

            def _validate(val: Any) -> None:
                result = validator(val)
                if isinstance(result, str):
                    raise ValidationError(result)

            return _validate

        field_type: Optional[Type] = self.types.get(name)
        field_type = (
            self._to_marshmallow_field_type(field_type)
            if field_type is not None
            else fields.Raw
        )

        type_args: Dict[str, Any] = {
            "allow_none": True,
            "required": name in self.required,
        }

        field_validators = self.field_validators.get(name)
        if field_validators:
            type_args["validate"] = [wrap_validator(v) for v in field_validators]

        return field_type(**type_args)
