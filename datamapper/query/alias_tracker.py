from collections import defaultdict
from sqlalchemy import Table
from sqlalchemy.sql.expression import Alias
from typing import Dict, DefaultDict, Optional
from datamapper.errors import ConflictingAliasError, UnknownAliasError


class AliasTracker:
    _aliases: Dict[str, Alias]
    _counter: DefaultDict[str, int]

    def __init__(self) -> None:
        self._aliases = {}
        self._counter = defaultdict(int)

    def fetch(self, alias_name: str) -> Alias:
        try:
            return self._aliases[alias_name]
        except KeyError:
            raise UnknownAliasError(alias_name)

    def put(self, table: Table, alias_name: Optional[str] = None) -> Alias:
        alias_name = alias_name or self.__generate(table.name[0])

        if alias_name in self._aliases:
            raise ConflictingAliasError(alias_name)

        alias = table.alias(alias_name)
        self._aliases[alias_name] = alias
        return alias

    def __generate(self, name: str) -> str:
        count = self._counter[name]
        self._counter[name] += 1
        return f"{name}{count}"
