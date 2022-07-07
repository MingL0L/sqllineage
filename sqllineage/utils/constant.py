from enum import Enum, unique


class NodeTag:
    READ = "read"
    WRITE = "write"
    CTE = "cte"
    DROP = "drop"
    SOURCE_ONLY = "source_only"
    TARGET_ONLY = "target_only"
    SELFLOOP = "selfloop"


@unique
class EdgeType(Enum):
    LINEAGE = 1
    RENAME = 2
    HAS_COLUMN = 3
    HAS_ALIAS = 4
    FORMULA = 5
    WHERE = 6
    GROUP_BY = 7


class LineageLevel:
    TABLE = "table"
    COLUMN = "column"
    FORMULA = "formula"
