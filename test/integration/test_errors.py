"""
Error class mapping — Data API ``ClientError`` / ``DatabaseError``
must surface as the right SQLAlchemy exception subclass so
``try/except IntegrityError`` and friends work in application code.

If these mappings drift, callers that catch ``IntegrityError`` to
handle conflict-as-not-error fall through to a generic exception
handler and behave incorrectly (often silently).
"""
import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    exc,
    text,
)


async def test_unique_violation_raises_integrity_error(engine):
    """Inserting a duplicate into a UNIQUE column → ``IntegrityError``."""
    metadata = MetaData()
    table = Table(
        "_int_err_unique",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("email", String(128)),
        UniqueConstraint("email", name="uq_email"),
    )
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS _int_err_unique"))
        await conn.run_sync(metadata.create_all)

    try:
        async with engine.begin() as conn:
            await conn.execute(
                table.insert(), {"id": 1, "email": "dup@example.com"}
            )

        with pytest.raises(exc.IntegrityError):
            async with engine.begin() as conn:
                await conn.execute(
                    table.insert(), {"id": 2, "email": "dup@example.com"}
                )
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS _int_err_unique"))


async def test_missing_table_raises_programming_error(engine):
    """SELECT from a non-existent table → ``ProgrammingError``.

    Catches the case where Data API's ``ER_INVALID_TABLE`` (or
    equivalent) maps cleanly through the driver to SQLAlchemy's
    ``ProgrammingError``. Application code reasonably treats this
    differently from a connection failure.
    """
    with pytest.raises(exc.ProgrammingError):
        async with engine.connect() as conn:
            await conn.execute(text("SELECT * FROM _no_such_table_xyzzy"))


async def test_syntax_error_raises_programming_error(engine):
    """Malformed SQL → ``ProgrammingError`` (not ``OperationalError``).

    Misclassifying syntax errors as transient/operational leads
    callers to retry forever.
    """
    with pytest.raises(exc.ProgrammingError):
        async with engine.connect() as conn:
            await conn.execute(text("THIS IS NOT VALID SQL"))


async def test_check_constraint_violation_raises_integrity_error(engine):
    """A failed CHECK constraint → ``IntegrityError``."""
    metadata = MetaData()
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS _int_err_check"))
        await conn.execute(
            text(
                "CREATE TABLE _int_err_check (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))"
            )
        )

    try:
        with pytest.raises(exc.IntegrityError):
            async with engine.begin() as conn:
                await conn.execute(
                    text("INSERT INTO _int_err_check (id, n) VALUES (1, -5)")
                )
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS _int_err_check"))


async def test_pool_recovers_after_error(engine):
    """An error inside a transaction must not poison the pool.

    Reuse a connection after a deliberate failure and verify it's
    healthy.
    """
    with pytest.raises(exc.ProgrammingError):
        async with engine.connect() as conn:
            await conn.execute(text("SELECT * FROM _no_such_table_xyzzy"))

    # Subsequent checkout still works.
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT 1"))).scalar() == 1
