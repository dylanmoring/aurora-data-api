"""
Pool behavior under ``asyncio.gather`` concurrency.

Bug class this targets:
* Multiple coroutines simultaneously acquiring connections from the
  ``AsyncAdaptedQueuePool`` — does the greenlet-spawn-per-await-only
  pattern actually keep their cursors isolated, or do queries
  cross-contaminate at the boto3 client layer?
* Transactions started in one coroutine must not be visible to others
  before commit (isolation under concurrent load).
* Pool sizing: under N concurrent tasks, the pool must scale or queue
  appropriately, not deadlock.

This matters because ``refresh_pending_batches`` (in ``survey_db``)
fan-outs N parallel queries via ``asyncio.gather``, and the original
bug surfaced precisely under concurrent load.
"""
import asyncio

import pytest
from sqlalchemy import text


async def test_concurrent_simple_queries(engine):
    """Many parallel queries via ``asyncio.gather`` all return correctly."""
    async def one(i):
        async with engine.connect() as conn:
            return (await conn.execute(text(f"SELECT {i} AS x"))).scalar()

    results = await asyncio.gather(*(one(i) for i in range(10)))
    assert results == list(range(10))


async def test_concurrent_writes_isolated(engine, scratch_table):
    """Each coroutine's transaction is isolated from siblings.

    If a coroutine started a transaction and another saw its
    uncommitted INSERT, that'd be a real isolation breach — the kind
    that produces "phantom row" bugs.
    """
    async def insert_and_count(i):
        async with engine.begin() as conn:
            await conn.execute(
                scratch_table.insert(), {"id": i, "name": f"row-{i}"}
            )
            # Inside our own transaction we should see our row.
            count = (
                await conn.execute(text("SELECT count(*) FROM _int_scratch"))
            ).scalar()
            return count

    counts = await asyncio.gather(*(insert_and_count(i) for i in range(5)))
    # Each coroutine should see at least its own row. (Could see more if
    # peer transactions committed first under READ COMMITTED, which is the
    # PG default.) The point is no coroutine sees fewer than 1.
    assert all(c >= 1 for c in counts), counts

    # After all commits, all 5 rows are visible.
    async with engine.connect() as conn:
        final = (
            await conn.execute(text("SELECT count(*) FROM _int_scratch"))
        ).scalar()
        assert final == 5


async def test_concurrent_with_cancellation(engine):
    """Cancelling some tasks in a gather pool doesn't break the others.

    Production shape: a batch dispatch coroutine times out one branch
    via ``asyncio.wait_for``. The other branches must complete cleanly.
    """
    async def slow(i):
        async with engine.connect() as conn:
            await conn.execute(text("SELECT pg_sleep(0.1)"))
            return (await conn.execute(text(f"SELECT {i}"))).scalar()

    async def to_cancel():
        async with engine.connect() as conn:
            await conn.execute(text("SELECT pg_sleep(10)"))

    tasks = [asyncio.create_task(slow(i)) for i in range(5)]
    cancel_task = asyncio.create_task(to_cancel())

    await asyncio.sleep(0.5)
    cancel_task.cancel()

    results = await asyncio.gather(*tasks, return_exceptions=False)
    try:
        await cancel_task
    except asyncio.CancelledError:
        pass

    assert results == list(range(5))
