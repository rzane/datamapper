from collections import defaultdict
from sqlalchemy import Table
from typing import Dict


class AliasTracker:
    _counter: Dict[str, int]

    def __init__(self) -> None:
        self._counter = defaultdict(int)

    def alias(self, table: Table) -> Table:
        name = table.name[0]
        count = self._counter[name]
        alias = f"{name}{count}"
        self._counter[name] += 1
        return table.alias(alias)
