from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import sqlalchemy
from marshmallow import Schema, ValidationError, fields

from datamapper.model import Association, BelongsTo, Model

FieldValidator = Callable[[Any], Union[bool, str]]

Data = TypeVar("Data", Model, dict)


class ChangesetDataWrapper(Generic[Data]):
    data: Data

    def get(self, field: str, default: Any = None) -> Optional[Any]:
        return self.attributes.get(field, default)

    def apply_changes(self, changes: dict) -> Data:
        raise NotImplementedError()  # pragma: no cover

    def field_type(self, field: str) -> Type:
        raise NotImplementedError()  # pragma: no cover

    def association(self, field: str) -> Association:
        raise NotImplementedError()  # pragma: no cover

    @property
    def attributes(self) -> dict:
        raise NotImplementedError()  # pragma: no cover

    def __repr__(self) -> str:
        return self.data.__repr__()  # pragma: no cover


class DictChangesetDataWrapper(ChangesetDataWrapper[dict]):
    def __init__(self, data: Tuple[dict, dict]):
        self.data: dict = data[0]
        self._types: dict = data[1]

    def apply_changes(self, changes: dict) -> dict:
        return dict_merge(self.data, changes)

    def field_type(self, field: str) -> type:
        return self._types.get(field, type(None))

    def association(self, field: str) -> Association:
        raise ValueError("Data of type dict has no associations")

    @property
    def attributes(self) -> dict:
        return self.data  # pragma: no cover


class ModelChangesetDataWrapper(ChangesetDataWrapper[Model]):
    def __init__(self, data: Model):
        self.data: Model = data

    def apply_changes(self, changes: dict) -> Model:
        attrs = {**self.data.attributes, **changes}
        return type(self.data)(**attrs)

    def field_type(self, field: str) -> Type:
        column = self.data.__table__.columns.get(field)
        return column.type if column is not None else None

    def association(self, field: str) -> Association:
        return self.data.association(field)

    @property
    def attributes(self) -> dict:
        return self.data.attributes  # pragma: no cover


def dict_merge(dct: dict, merge_dct: dict) -> dict:
    dct_ = dict(dct)
    for k, v in merge_dct.items():
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

        type_args: Dict[str, Any] = {
            "allow_none": True,
            "required": name in self.changeset.required,
        }

        field_validators = self.changeset._field_validators.get(name)
        if field_validators:
            type_args["validate"] = [wrap_validator(v) for v in field_validators]

        return field_type(**type_args)


class Changeset(Generic[Data]):
    VALIDATOR_CLASS = MarshmallowValidator

    _wrapped_data: ChangesetDataWrapper[Data]

    def __init__(self, data: Union[Data, Tuple[Data, dict]]) -> None:
        self.params: dict = {}
        self.permitted: set = set()
        self.required: set = set()

        self._forced_changes: dict = {}
        self._field_validators: Dict[str, List[FieldValidator]] = {}
        self._schema_validators: List = []

        if isinstance(data, tuple) and isinstance(data[0], dict):
            self._wrapped_data = DictChangesetDataWrapper(data)
        elif isinstance(data, Model):
            self._wrapped_data = ModelChangesetDataWrapper(data)
        else:
            raise AttributeError("Changeset data must be a Model or (dict, dict).")

    def cast(self, params: Mapping[str, Any], permitted: List[str]) -> Changeset:
        """
        Applies `params` as changes to the changeset, provided that their keys are in `permitted` and their types are correct.
        """
        permitted_params = {k: v for (k, v) in params.items() if k in permitted}
        return self._update(
            params={**self.params, **permitted_params},
            permitted=self.permitted.union(permitted),
        )

    def put_assoc(self, name: str, value: Any) -> Changeset:
        """
        Put an association as a change on the changeset.
        """
        assoc = self._wrapped_data.association(name)
        if isinstance(assoc, BelongsTo):
            return self.change(assoc.values(value))
        else:
            raise NotImplementedError()

    def has_change(self, field: str) -> bool:
        return field in self.changes

    def get_change(self, field: str, default: Any = None) -> Optional[Any]:
        """
        Gets a change or returns a default value.
        """
        return self.changes.get(field, default)

    def get_field(self, field: str, default: Any = None) -> Optional[Any]:
        """
        Fetches a field from `changes` if it exists, falling back to `data`.
        """
        return self.changes.get(field, default) or self._wrapped_data.get(
            field, default
        )

    def validate_required(self, fields: List) -> Changeset:
        """
        Validate that `fields` are present in the changeset, either as changes or as data.
        """
        for f in fields:
            self.required.add(f)
        return self

    def validate_change(self, field: str, validator: FieldValidator) -> Changeset:
        """
        Validate the value of the given `field` using `valiator` as a predicate.

        Skips the validation if `field` has no changes.
        """
        self._add_field_validator(field, validator)
        return self

    def validate_inclusion(
        self, field: str, vals: Any, msg: str = "is invalid"
    ) -> Changeset:
        def _validate_inclusion(value: Any) -> Union[bool, str]:
            return True if value in vals else msg

        self._add_field_validator(field, _validate_inclusion)
        return self

    def validate_exclusion(
        self, field: str, vals: Any, msg: str = "is invalid"
    ) -> Changeset:
        def _validate_exclusion(value: Any) -> Union[bool, str]:
            return True if value not in vals else msg

        self._add_field_validator(field, _validate_exclusion)
        return self

    def validate_length(
        self,
        field: str,
        length: Optional[int] = None,
        minimum: Optional[int] = None,
        maximum: Optional[int] = None,
        message: Optional[str] = None,
    ) -> Changeset:
        if length is not None:

            def _validate_exact_length(val: Any) -> Union[bool, str]:
                message_ = message or f"should be {length} characters"
                return True if len(val) == length else message_

            self._add_field_validator(field, _validate_exact_length)

        if minimum is not None:

            def _validate_min_length(val: Any) -> Union[bool, str]:
                message_ = message or f"should be at least {minimum} characters"
                return True if len(val) >= cast(int, minimum) else message_

            self._add_field_validator(field, _validate_min_length)

        if maximum is not None:

            def _validate_max_length(val: Any) -> Union[bool, str]:
                message_ = message or f"should be at most {maximum} characters"
                return True if len(val) <= cast(int, maximum) else message_

            self._add_field_validator(field, _validate_max_length)

        return self

    def change(self, changes: dict) -> Changeset:
        """
        Apply the given changes without validation.
        """
        self._forced_changes = dict_merge(self._forced_changes, changes)
        return self

    def put_change(self, field: str, value: Any) -> Changeset:
        """
        Puts a change on the given `field` with `value`.

        The value is stored without validation.
        """
        self._forced_changes[field] = value
        return self

    def on_changed(
        self, field: str, f: Callable[[Changeset, Any], Changeset]
    ) -> Changeset:
        """
        If `field` has been changed, call callback function `f` with the value,
        returning a new Changeset.
        """
        if self.has_change(field):
            return f(self, self.get_change(field))
        else:
            return self

    def pipe(self, f: Callable[[Changeset], Changeset]) -> Changeset:
        """
        Pipe the Changeset into the function f, returning a new Changeset.
        """
        return f(self)

    def apply_changes(self) -> Data:
        """
        Apply the changeset's changes to the data.
        """
        return self._wrapped_data.apply_changes(self.changes)

    def _field_type(self, field: str) -> Type:
        return self._wrapped_data.field_type(field)

    def _add_field_validator(self, field: str, validator: FieldValidator) -> None:
        existing_validators = self._field_validators.get(field, [])
        self._field_validators[field] = existing_validators + [validator]

    @property
    def has_error(self) -> bool:
        return bool(self.errors)

    @property
    def is_valid(self) -> bool:
        return not self.has_error

    @property
    def changes(self) -> dict:
        (is_valid, result) = self.VALIDATOR_CLASS(self).validate()
        if is_valid:
            return dict_merge(result, self._forced_changes)
        else:
            return {}

    @property
    def errors(self) -> dict:
        (is_valid, result) = self.VALIDATOR_CLASS(self).validate()
        if is_valid:
            return {}
        else:
            return result

    def __repr__(self) -> str:
        return (
            f"<Changeset\n"
            f" is_valid={self.is_valid}\n"
            f" data={self._wrapped_data}\n"
            f" params={self.params}\n"
            f" changes={self.changes}\n"
            f" errors={self.errors}"
            ">"
        )  # pragma: no cover

    def _update(self, **kwargs: Any) -> Changeset:
        # Can replace this with an immutable collection library.
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        return self

    @property
    def data(self) -> Data:
        return self._wrapped_data.data
