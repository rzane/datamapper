from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type

from datamapper.changeset.types import FieldValidator

TypeValidator = Callable[[Any], bool]
Errors = Dict[str, List[str]]


def human_type_name(type_name: str) -> str:
    return {"int": "integer", "str": "string"}.get(type_name, type_name)


class Validator:
    def validate(self) -> Tuple[bool, dict]:
        raise NotImplementedError()  # pragma: no cover


class BasicValidator(Validator):
    # TODO: Could take this approach instead of using sqlalchemy's .python_type
    # TYPE_VALIDATORS: List[Tuple[Set[Type], str, TypeValidator]] = [
    #     (
    #         {sa.String, sa.VARCHAR, str},
    #         "Not a valid string.",
    #         lambda v: isinstance(v, str),
    #     ),
    #     (
    #         {sa.Integer, sa.BigInteger, int},
    #         "Not a valid integer.",
    #         lambda v: isinstance(v, int),
    #     ),
    #     ({sa.Date, date}, "Not a valid date.", lambda v: isinstance(v, date)),
    # ]

    def __init__(
        self,
        params: Dict[str, Any],
        types: Dict[str, Any],
        data: Dict[str, Any],
        permitted: Set[str],
        required: Set[str],
        field_validators: Dict[str, List[FieldValidator]],
    ):
        self.params = params
        self.types = types
        self.data = data
        self.permitted = permitted
        self.required = required
        self.field_validators = field_validators

    def validate(self) -> Tuple[bool, dict]:
        (changes, errors) = self._validate_types(self.params, {})
        (changes, errors) = self._validate_required(changes, errors)
        (changes, errors) = self._validate_fields(changes, errors)

        valid = len(errors) == 0
        return (valid, changes) if valid else (valid, errors)

    def _add_error(self, errors: Errors, key: str, msg: str) -> Errors:
        errors_ = dict(errors)
        existing_errors = errors.get(key, [])
        errors_[key] = [*existing_errors, msg]
        return errors_

    def _validate_required(self, changes: dict, errors: dict) -> Tuple[dict, dict]:
        errors_ = dict(errors)

        for key in self.required:
            if key not in changes and key not in self.data:
                errors_ = self._add_error(
                    errors_, key, "Missing data for required field."
                )

        return (changes, errors_)

    def _validate_types(self, params: dict, errors: dict) -> Tuple[dict, dict]:
        errors_ = dict(errors)
        changes = dict(params)

        for (key, val) in self.params.items():
            schema_type = self.types.get(key)
            if schema_type is None:
                changes.pop(key)
                continue

            error = self._validate_type(schema_type, val)
            if error is not None:
                errors_ = self._add_error(errors_, key, error)

        return (changes, errors_)

    def _validate_type(self, schema_type: Type, val: Any) -> Optional[str]:
        if val is None:
            return None

        (error_msg, type_validator) = self._get_type_validator(schema_type)
        return error_msg if not type_validator(val) else None

    def _get_type_validator(self, schema_type: Type) -> Tuple[str, TypeValidator]:
        python_type = self._to_python_type(schema_type)
        error_msg = f"Not a valid {human_type_name(python_type.__name__)}."

        def _validator(v: Any) -> bool:
            return isinstance(v, python_type)

        return (error_msg, _validator)

    def _validate_fields(self, changes: dict, errors: dict) -> Tuple[dict, dict]:
        errors_ = dict(errors)
        for (key, val) in changes.items():
            field_validators = self.field_validators.get(key, [])
            for field_validator in field_validators:
                success_or_err_msg = field_validator(val)
                if isinstance(success_or_err_msg, str):
                    errors_ = self._add_error(errors_, key, success_or_err_msg)

        return (changes, errors_)

    def _to_python_type(self, type_: Any) -> Type:
        try:
            # For sqlalchemy types
            return type_.python_type
        except AttributeError:
            return type_
