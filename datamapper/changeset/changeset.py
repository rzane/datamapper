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
    Union,
    cast,
)

from datamapper.changeset.data_wrapper import (
    ChangesetDataWrapper,
    DictChangesetDataWrapper,
    ModelChangesetDataWrapper,
)
from datamapper.changeset.types import Data, FieldValidator
from datamapper.changeset.utils import dict_merge
from datamapper.changeset.validator import MarshmallowValidator
from datamapper.model import BelongsTo, Model


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
        (is_valid, result) = self._validate()
        if is_valid:
            return dict_merge(result, self._forced_changes)
        else:
            return {}

    @property
    def errors(self) -> dict:
        (is_valid, result) = self._validate()
        if is_valid:
            return {}
        else:
            return result

    @property
    def data(self) -> Data:
        return self._wrapped_data.data

    def _validate(self) -> Tuple[bool, dict]:
        return self.VALIDATOR_CLASS(
            params=self.params,
            types=self._wrapped_data.types,
            permitted=self.permitted,
            required=self.required,
            field_validators=self._field_validators,
        ).validate()

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
