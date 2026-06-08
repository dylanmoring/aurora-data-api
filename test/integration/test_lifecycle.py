"""
Connection lifecycle and cancellation propagation.

Bug class this targets:
* ``CancelledError`` raised by ``await_only`` before reaching driver-level
  ``rollback()`` — historically left sessions in an inconsistent state
  and produced rollback log noise on every cancel.
* ``async with engine.begin()`` / ``async with engine.connect()`` not
  cleanly releasing the underlying DBAPI connection on cancel.
* Pool not handing back a usable connection after a cancelled task.

These are the integration shapes ``survey_db`` actually uses — Lambda
handlers that get SIGTERM'd partway through a transaction, or callers
that race ``asyncio.wait_for`` against a long query.
"""
import asyncio

import pytest
from sqlalchemy import text


async def test_engine_begin_basic_round_trip(engine):
    """``async with engine.begin()`` works for the simplest case."""
    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT 1 AS x"))
        row = result.one()
        assert row.x == 1


async def test_engine_connect_basic_round_trip(engine):
    """``async with engine.connect()`` works without an outer begin."""
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT 42 AS x"))
        assert result.scalar() == 42


async def test_pool_releases_after_normal_use(engine):
    """Successive uses re-acquire a healthy connection from the pool."""
    for _ in range(3):
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT 1"))).scalar() == 1


async def test_cancelled_query_does_not_break_pool(engine):
    """Cancelling a task mid-query must leave the pool usable.

    Historical bug shape: ``await_only`` raised ``CancelledError`` before
    the driver's ``rollback()`` ran, so the pool entry was returned in
    a half-open state and the next checkout produced ``MissingGreenlet``
    noise on every subsequent query.
    """

    async def slow_query():
        async with engine.connect() as conn:
            # pg_sleep is the cheapest reliable "long-running" probe.
            await conn.execute(text("SELECT pg_sleep(5)"))

    task = asyncio.create_task(slow_query())
    await asyncio.sleep(0.2)  # let it get into pg_sleep
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass  # expected

    # Pool must still hand out a usable connection.
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT 1"))).scalar() == 1


async def test_transaction_rollback_on_cancel(engine, scratch_table):
    """A cancel inside ``async with engine.begin()`` rolls back the tx.

    Without proper cancellation handling, a partial INSERT would stay
    visible to the next session — silent data corruption.
    """
    async def doomed_insert():
        async with engine.begin() as conn:
            await conn.execute(
                scratch_table.insert(), {"id": 1, "name": "before-cancel"}
            )
            await asyncio.sleep(5)  # cancel point
            await conn.execute(
                scratch_table.insert(), {"id": 2, "name": "after-cancel"}
            )

    task = asyncio.create_task(doomed_insert())
    await asyncio.sleep(0.5)  # let the first INSERT land
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Both rows must be absent — transaction rolled back.
    async with engine.connect() as conn:
        count = (
            await conn.execute(text("SELECT count(*) FROM _int_scratch"))
        ).scalar()
        assert count == 0, f"transaction did not roll back on cancel; saw {count} rows"


async def test_explicit_rollback_inside_begin(engine, scratch_table):
    """Explicit ``trans.rollback()`` discards writes."""
    async with engine.connect() as conn:
        trans = await conn.begin()
        await conn.execute(
            scratch_table.insert(), {"id": 99, "name": "rolled-back"}
        )
        await trans.rollback()

    async with engine.connect() as conn:
        count = (
            await conn.execute(text("SELECT count(*) FROM _int_scratch"))
        ).scalar()
        assert count == 0
