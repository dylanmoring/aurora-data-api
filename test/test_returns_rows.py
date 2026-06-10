"""Unit tests for ``_statement_returns_rows`` — the heuristic the driver uses
to decide whether a SQL statement is expected to yield a result set.

Pure-Python tests, no DB fixtures."""
import pytest

from aurora_data_api import _statement_returns_rows


@pytest.mark.parametrize("sql, expected", [
    # Basic.
    ("SELECT 1", True),
    ("  select 1  ", True),
    ("VALUES (1), (2)", True),
    ("SHOW search_path", True),
    ("EXPLAIN SELECT 1", True),
    ("TABLE t", True),
    # DML without/with RETURNING.
    ("INSERT INTO t VALUES (1)", False),
    ("INSERT INTO t VALUES (1) RETURNING id", True),
    ("UPDATE t SET x = 1", False),
    ("UPDATE t SET x = 1 RETURNING x", True),
    ("DELETE FROM t WHERE x = 1", False),
    ("DELETE FROM t WHERE x = 1 RETURNING x", True),
    # Leading parens (compound SELECTs).
    ("(SELECT 1) UNION (SELECT 2)", True),
    ("((SELECT 1)) UNION ALL (SELECT 2)", True),
    # Plain CTE.
    ("WITH a AS (SELECT 1) SELECT * FROM a", True),
    # The bug — recursive CTE whose body has nested parens.
    (
        "WITH RECURSIVE some_cte(n) AS (SELECT 1 UNION ALL "
        "SELECT (n + 1) FROM some_cte WHERE n < 10) SELECT * FROM some_cte",
        True,
    ),
    # CTE followed by INSERT (no RETURNING) — not a row-returning stmt.
    ("WITH a AS (SELECT 1) INSERT INTO t SELECT * FROM a", False),
    # CTE followed by INSERT ... RETURNING.
    ("WITH a AS (SELECT 1) INSERT INTO t SELECT * FROM a RETURNING id", True),
    # CTE followed by UPDATE ... RETURNING.
    (
        "WITH a AS (SELECT 1) UPDATE t SET x = 1 FROM a RETURNING x",
        True,
    ),
    # Multi-CTE.
    (
        "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b",
        True,
    ),
    # String literal with parens — must not confuse paren-depth tracking.
    ("SELECT 'a)b('", True),
    ("SELECT 'don''t' FROM t", True),
    # Empty / whitespace.
    ("", False),
    ("   ", False),
    ("\n\t  \n", False),
    # Comments before the keyword.
    ("-- a comment\nSELECT 1", True),
    ("/* hi */ SELECT 1", True),
    # MATERIALIZED hint on CTE body.
    ("WITH a AS MATERIALIZED (SELECT 1) SELECT * FROM a", True),
    ("WITH a AS NOT MATERIALIZED (SELECT 1) SELECT * FROM a", True),
    # CTE body containing a string with the word INSERT — body is opaque.
    (
        "WITH a AS (SELECT 'INSERT INTO t') SELECT * FROM a",
        True,
    ),
])
def test_statement_returns_rows(sql, expected):
    assert _statement_returns_rows(sql) is expected
