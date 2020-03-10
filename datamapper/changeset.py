from __future__ import annotations
from typing import Any, List, Mapping, Type, Optional, Tuple, Callable, Union, Dict
from marshmallow import Schema, fields, ValidationError
import sqlalchemy

from datamapper.model import Model

FieldValidator = Callable[[Any], Union[bool, str]]


class MarshmallowValidator:
    def __init__(self, changeset: Changeset):
        self.changeset = changeset

    @classmethod
    def _sqla_to_marshmallow(cls, type_: Type) -> Type:
        """
        Maps sqlalchemy column types to marshmallow field types.
        """
        if isinstance(type_, sqlalchemy.String):
            return fields.String
        if isinstance(type_, sqlalchemy.BigInteger):
            return fields.Integer
        else:
            return fields.Raw

    def validate(self) -> Tuple[bool, dict]:
        """
        Programatically build a Marshmallow schema and validate against it.

        Returns (True, changes) | (False, errors)
        """
        permitted_params = self.changeset.permitted_params()
        SchemaType = self._build_schema()
        errors = SchemaType().validate(permitted_params)
        if errors:
            return (False, errors)

        changes = SchemaType().dump(permitted_params)
        return (True, changes)

    def _build_schema(self) -> Type[Schema]:
        """
        Programatically build a Marshmallow Schema object.
        """
        all_fields = set(self.changeset.permitted).union(self.changeset.required)
        schema_attributes = {field: self._build_field(field) for field in all_fields}
        return type("schema", (Schema,), schema_attributes)

    def _build_field(self, name: str) -> Type[fields.Field]:
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

        column: Optional[Type] = self.changeset.model.__attributes__.get(name)
        field_type = (
            self._sqla_to_marshmallow(column.type) if column is not None else fields.Raw
        )

        type_args: Dict[str, Any] = {"required": name in self.changeset.required}

        field_validators = self.changeset._field_validators.get(name)
        if field_validators:
            type_args["validate"] = [wrap_validator(v) for v in field_validators]

        return field_type(**type_args)


class Changeset:
    VALIDATOR_CLASS = MarshmallowValidator

    def __init__(self, model: Union[Model, Type[Model]]) -> None:
        self.model = model

        self._changes: dict = {}
        self._errors: dict = {}
        self.params: dict = {}
        self.permitted: list = []
        self.required: set = set()
        self._evaluated = False
        self._field_validators: Dict[str, List[FieldValidator]] = {}
        self._schema_validators: List = []

    def cast(self, params: Mapping[str, Any], permitted: List[str]) -> Changeset:
        return self._update(params=params, permitted=permitted)

    def validate_required(self, fields: List) -> Changeset:
        for f in fields:
            self.required.add(f)
        return self

    def validate_change(self, field: str, validator: FieldValidator) -> Changeset:
        existing_validators = self._field_validators.get(field, [])
        self._field_validators[field] = existing_validators + [validator]
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
        return f"<Changeset model={self.model} {m}>"

    def _update(self, **kwargs: Any) -> Changeset:
        # Can replace this with an immutable collection library.
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        return self

    def _model_class(self) -> Type[Model]:
        if isinstance(self.model, Model):
            return self.model.__class__
        else:
            return self.model

    def _model_instance(self) -> Model:
        if not isinstance(self.model, Model):
            raise ValueError("Must be a Model instance, not a class.")
        return self.model

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
