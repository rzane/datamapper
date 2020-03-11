from collections import defaultdict
from sqlalchemy import Table
from sqlalchemy.sql.expression import Alias
from typing import Dict, DefaultDict, Optional


class AliasTracker:
    _aliases: Dict[str, Alias]
    _counter: DefaultDict[str, int]

    def __init__(self) -> None:
        self._aliases = {}
        self._counter = defaultdict(int)

    def generate(self, name: str) -> str:
        count = self._counter[name]
        self._counter[name] += 1
        return f"{name}{count}"

    def get(self, alias_name: str) -> Alias:
        return self._aliases[alias_name]

    def alias(self, table: Table, alias_name: Optional[str] = None) -> Alias:
        alias_name = alias_name or self.generate(table.name[0])
        alias = table.alias(alias_name)
        self._aliases[alias_name] = alias
        return alias
