"""
Conftest for the integration test suite.

This is the *async-surface* harness — focused tests for behaviors the
compliance suite (which drives the sync engine API) doesn't cover:

* ``async with engine.begin()`` / ``async with engine.connect()``
  lifecycle and transaction semantics
* ``asyncio.CancelledError`` propagation through ``await_only``
* Pool behavior under ``asyncio.gather`` concurrency
* Aurora-Data-API-specific edge cases (HTTPS round-trip, cold-start
  wake-up, error mapping, RETURNING drain)

We do NOT re-import the compliance plugin here. This is a plain
pytest-asyncio suite using vanilla SQLAlchemy async API — exactly how
production code uses it. That's the point: stress the integration
between SQLAlchemy's async surface and our specific driver / Aurora
Data API, in shapes the application actually uses.
"""
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from sqlalchemy.dialects import registry

registry.register(
    "postgresql.auroradataapiasync",
    "sqlalchemy_aurora_data_api",
    "AuroraPostgresDataAPIAsyncDialect",
)


from test import load_dotenv

load_dotenv()


# pytest-asyncio config (asyncio_default_*_loop_scope = session in setup.cfg)
# pins all async fixtures + tests to a single event loop for the whole
# session. Without this, ``aioboto3``'s aiohttp session attaches to test
# #1's loop, then test #2 starts on a fresh loop and the half-bound HTTP
# tasks die mid-flight ("Task was destroyed but it is pending!").


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def engine():
    """One AsyncEngine per session. Tests share the pool; the
    auto-paused cluster wakes once and stays warm for the run.
    """
    eng = create_async_engine(
        "postgresql+auroradataapiasync://:@/ada_test",
        pool_pre_ping=True,
    )
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture(loop_scope="session")
async def conn(engine):
    """Per-test connection inside its own transaction. Rolls back on
    exit so tests don't leak schema/data. Use this for query tests.
    """
    async with engine.connect() as connection:
        async with connection.begin():
            yield connection
            # transaction context exits → automatic rollback


@pytest_asyncio.fixture(loop_scope="session")
async def scratch_table(engine):
    """A throwaway table for tests that need to write/read. Created
    fresh per test and dropped at teardown to avoid coupling between
    tests.
    """
    from sqlalchemy import Column, Integer, MetaData, String, Table, text

    metadata = MetaData()
    table = Table(
        "_int_scratch",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(64)),
        Column("payload", String(1024), nullable=True),
    )
    async with engine.begin() as c:
        # idempotent setup: drop+create. Don't reflect (Data API reflection
        # is broken anyway — see compliance findings).
        await c.execute(text("DROP TABLE IF EXISTS _int_scratch"))
        await c.run_sync(metadata.create_all)
    try:
        yield table
    finally:
        async with engine.begin() as c:
            await c.execute(text("DROP TABLE IF EXISTS _int_scratch"))
