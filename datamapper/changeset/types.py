from typing import Any, Callable, TypeVar, Union

from datamapper.model import Model

FieldValidator = Callable[[Any], Union[bool, str]]
Data = TypeVar("Data", Model, dict)
