from __future__ import annotations
from typing import (
    Any,
    List,
    Mapping,
    Type,
    Optional,
    Tuple,
    Callable,
    Union,
    Dict,
    TypeVar,
    Generic,
)
from marshmallow import Schema, fields, ValidationError
import sqlalchemy
from copy import copy
import collections

from datamapper.model import Model, BelongsTo, Association

FieldValidator = Callable[[Any], Union[bool, str]]
Data = Union[dict, Model]

T = TypeVar("T", dict, Model)


class ChangesetDataWrapper(Generic[T]):
    def __init__(self, data: T):
        self.data: T = data

    def apply_changes(self, changes: dict) -> T:
        if isinstance(self.data, dict):
            return dict_merge(self.data, changes)
        elif isinstance(self.data, Model):
            inst = copy(self.data)
            for k, v in changes.items():
                setattr(inst, k, v)
            return inst
        else:
            return self.data(**changes)

    def field_type(self, field: str) -> Type:
        if isinstance(self.data, Model):
            column = self.data.__attributes__.get(field)
            return column.type if column is not None else None
        else:
            return type(self.data.get(field))

    def association(self, field: str) -> Association:
        if not isinstance(self.data, Model):
            raise ValueError("Data of type dict has no associations")

        return self.data.association(field)

    @property
    def type(self) -> Type:
        return type(self.data)


def dict_merge(dct: dict, merge_dct: dict) -> dict:
    """
    Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    dct_ = dict(dct)
    for k, v in merge_dct.items():
        if (
            k in dct_
            and isinstance(dct_[k], dict)
            and isinstance(merge_dct[k], collections.Mapping)
        ):
            dict_merge(dct_[k], merge_dct[k])
        else:
            dct_[k] = merge_dct[k]
    return dct_


class MarshmallowValidator:
    def __init__(self, changeset: Changeset):
        self.changeset = changeset

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
        errors = SchemaType().validate(self.changeset.params)
        if errors:
            return (False, errors)

        changes = SchemaType().dump(self.changeset.params)
        return (True, changes)

    def _build_schema(self) -> Type[Schema]:
        """
        Programatically build a Marshmallow Schema object.
        """
        all_fields = set(self.changeset.permitted).union(self.changeset.required)
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

        field_type: Optional[Type] = self.changeset._field_type(name)
        field_type = (
            self._to_marshmallow_field_type(field_type)
            if field_type is not None
            else fields.Raw
        )

        type_args: Dict[str, Any] = {"required": name in self.changeset.required}

        field_validators = self.changeset._field_validators.get(name)
        if field_validators:
            type_args["validate"] = [wrap_validator(v) for v in field_validators]

        return field_type(**type_args)


class Changeset(Generic[T]):
    VALIDATOR_CLASS = MarshmallowValidator

    def __init__(self, data: T) -> None:
        self._wrapped_data: ChangesetDataWrapper[T] = ChangesetDataWrapper(data)

        self._changes: dict = {}
        self._errors: dict = {}
        self.params: dict = {}
        self.permitted: list = []
        self.required: set = set()
        self._field_validators: Dict[str, List[FieldValidator]] = {}
        self._schema_validators: List = []

    def cast(self, params: Mapping[str, Any], permitted: List[str]) -> Changeset:
        permitted_params = {k: v for (k, v) in params.items() if k in permitted}
        return self._update(params=permitted_params, permitted=permitted)

    def put_assoc(self, name: str, value: Any) -> Changeset:
        assoc = self._wrapped_data.association(name)
        if isinstance(assoc, BelongsTo):
            return self.change(assoc.values(value))
        else:
            raise NotImplementedError()

    def validate_required(self, fields: List) -> Changeset:
        for f in fields:
            self.required.add(f)
        return self

    def validate_change(self, field: str, validator: FieldValidator) -> Changeset:
        existing_validators = self._field_validators.get(field, [])
        self._field_validators[field] = existing_validators + [validator]
        return self

    def change(self, changes: dict) -> Changeset:
        self._changes = dict_merge(self._changes, changes)
        return self

    def apply_changes(self) -> T:
        return self._wrapped_data.apply_changes(self.changes)

    def _field_type(self, field: str) -> Type:
        return self._wrapped_data.field_type(field)

    @property
    def has_error(self) -> bool:
        return bool(self.errors)

    @property
    def is_valid(self) -> bool:
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
        return f"<Changeset data={self._wrapped_data.type}>"

    def _update(self, **kwargs: Any) -> Changeset:
        # Can replace this with an immutable collection library.
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        return self

    @property
    def data(self) -> T:
        return self._wrapped_data.data

    def _evaluate(self) -> None:
        """
        Build the validation class and validate the params, finally setting
        the `changes` and `errors` attributes.
        """
        changes: dict = {}
        errors = {}

        (is_valid, result) = self.VALIDATOR_CLASS(self).validate()
        if is_valid:
            changes = dict_merge(self._changes, result)
        else:
            errors = result

        self._update(_changes=changes, _errors=errors, _evaluated=True)
