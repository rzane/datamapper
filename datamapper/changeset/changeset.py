from __future__ import annotations

from typing import Any, Callable, Generic, List, Mapping, Optional, Tuple, Union

from datamapper.changeset import validators
from datamapper.changeset.data_wrapper import (
    ChangesetDataWrapper,
    DictChangesetDataWrapper,
    ModelChangesetDataWrapper,
)
from datamapper.changeset.types import Data, FieldValidator
from datamapper.changeset.utils import dict_merge
from datamapper.model import BelongsTo, Model


class Changeset(Generic[Data]):
    _wrapped_data: ChangesetDataWrapper[Data]

    def __init__(self, data: Union[Data, Tuple[Data, dict]]) -> None:
        self.params: dict = {}
        self.changes: dict = {}
        self.errors: dict = {}

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
        self.params = {k: v for (k, v) in params.items() if k in permitted}
        (changes, errors) = validators.validate_types(
            params=self.params,
            types=self._wrapped_data.types,
            data=self._wrapped_data.attributes,
        )
        self.changes = changes
        self.errors = errors
        return self

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
        """
        Returns `True` if the field is present in the changes.
        """
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

    def has_field(self, field: str) -> bool:
        """
        Returns `True` if the field is present in the changes or data.
        """
        return field in self.changes or field in self._wrapped_data.attributes

    def validate_required(
        self, fields: List, message: str = "Missing data for required field."
    ) -> Changeset:
        """
        Validate that `fields` are present in the changeset, either as changes or as data.
        """
        for field in fields:
            if not self.has_field(field):
                self.add_error(field, message)

        return self

    def validate_change(self, field: str, validator: FieldValidator) -> Changeset:
        """
        Validate the value of the given `field` using `valiator` as a predicate.

        Skips the validation if `field` has no changes.
        """
        return self._validate_field(field, validator)

    def validate_inclusion(
        self, field: str, vals: Any, msg: str = "is invalid"
    ) -> Changeset:
        return self._validate_field(field, validators.validate_inclusion(vals, msg))

    def validate_exclusion(
        self, field: str, vals: Any, msg: str = "is invalid"
    ) -> Changeset:
        return self._validate_field(field, validators.validate_exclusion(vals, msg))

    def validate_length(
        self,
        field: str,
        length: Optional[int] = None,
        minimum: Optional[int] = None,
        maximum: Optional[int] = None,
        message: Optional[str] = None,
    ) -> Changeset:
        if length is not None:
            return self._validate_field(
                field, validators.validate_exact_length(length, message)
            )
        if minimum is not None:
            return self._validate_field(
                field, validators.validate_min_length(minimum, message)
            )
        if maximum is not None:
            return self._validate_field(
                field, validators.validate_max_length(maximum, message)
            )

        return self

    def change(self, changes: dict) -> Changeset:
        """
        Apply the given changes without validation.
        """
        self.changes = dict_merge(self.changes, changes)
        return self

    def put_change(self, field: str, value: Any) -> Changeset:
        """
        Puts a change on the given `field` with `value`.

        The value is stored without validation.
        """
        self.changes[field] = value
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

    def add_error(self, field: str, error: str) -> Changeset:
        existing_errors: List[str] = self.errors.get(field, [])
        self.errors[field] = [*existing_errors, error]
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

    def _validate_field(self, field: str, validator: FieldValidator) -> Changeset:
        change_value = self.changes.get(field)
        if change_value is None:
            return self

        success_or_error_message = validator(change_value)
        if isinstance(success_or_error_message, str):
            return self.add_error(field, success_or_error_message)
        else:
            return self

    @property
    def has_error(self) -> bool:
        return bool(self.errors)

    @property
    def is_valid(self) -> bool:
        return not self.has_error

    @property
    def data(self) -> Data:
        return self._wrapped_data.data

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
