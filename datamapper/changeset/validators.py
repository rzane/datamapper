from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

from datamapper.changeset.types import FieldValidator

TypeValidator = Callable[[Any], bool]
Errors = Dict[str, List[str]]


def human_type_name(type_name: str) -> str:
    return {"int": "integer", "str": "string"}.get(type_name, type_name)


def _add_error(errors: Errors, key: str, msg: str) -> Errors:
    errors_ = dict(errors)
    existing_errors = errors.get(key, [])
    errors_[key] = [*existing_errors, msg]
    return errors_


def validate_types(params: dict, types: dict, data: dict) -> Tuple[dict, dict]:
    errors: dict = {}
    changes = dict(params)

    for (key, val) in params.items():
        schema_type = types.get(key)
        if schema_type is None:
            changes.pop(key)
            continue

        error = _validate_type(schema_type, val)
        if error is not None:
            errors = _add_error(errors, key, error)
            changes.pop(key)

    return (changes, errors)


def _validate_type(schema_type: Type, val: Any) -> Optional[str]:
    if val is None:
        return None

    (error_msg, type_validator) = _get_type_validator(schema_type)
    return error_msg if not type_validator(val) else None


def _get_type_validator(schema_type: Type) -> Tuple[str, TypeValidator]:
    error_msg = f"Not a valid {human_type_name(schema_type.__name__)}."

    def _validator(v: Any) -> bool:
        return isinstance(v, schema_type)

    return (error_msg, _validator)


def validate_exact_length(length: int, message: Optional[str] = None) -> FieldValidator:
    def _validate_exact_length(val: Any) -> Union[bool, str]:
        message_ = message or f"should be {length} characters"
        return True if len(val) == length else message_

    return _validate_exact_length


def validate_min_length(minimum: int, message: Optional[str] = None) -> FieldValidator:
    def _validate_min_length(val: Any) -> Union[bool, str]:
        message_ = message or f"should be at least {minimum} characters"
        return True if len(val) >= minimum else message_

    return _validate_min_length


def validate_max_length(maximum: int, message: Optional[str] = None) -> FieldValidator:
    def _validate_max_length(val: Any) -> Union[bool, str]:
        message_ = message or f"should be at most {maximum} characters"
        return True if len(val) <= maximum else message_

    return _validate_max_length


def validate_inclusion(vals: List[Any], message: str) -> FieldValidator:
    def _validate_inclusion(value: Any) -> Union[bool, str]:
        return True if value in vals else message

    return _validate_inclusion


def validate_exclusion(vals: List[Any], message: str) -> FieldValidator:
    def _validate_exclusion(value: Any) -> Union[bool, str]:
        return True if value not in vals else message

    return _validate_exclusion
