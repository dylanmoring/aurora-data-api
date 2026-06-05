"""
Type coercion and RETURNING behavior under the async path.

Bug class this targets:
* Tz-aware ``datetime`` round-trips losing ``tzinfo`` — the bug behind
  ``09a05e7`` "Fix tz-naive created_at compare in recover_stuck_batch".
* ``INSERT ... ON CONFLICT DO NOTHING RETURNING ...`` failing to drain
  results when the conflict path returns 0 rows — the cursor-close
  bug we hand-debugged repeatedly.
* Bulk INSERT with RETURNING — the ``survey_db.bulk_validation`` hot
  path.
* Numeric precision (the ``supports_native_decimal`` fix we just
  shipped — ``Numeric(asdecimal=False)`` should return ``float``).
* Datetime microsecond preservation (the ``_ADA_DATETIME_MIXIN.ms``
  fix).
"""
import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert


async def test_tz_aware_datetime_round_trip(engine):
    """A tz-aware datetime must survive write→read without losing tzinfo.

    The ``recover_stuck_batch`` bug was: we wrote a tz-aware ``utcnow()``
    and read it back as tz-naive, then compared the two with ``>``,
    which raised ``TypeError`` under Python's strict comparison rules.
    """
    metadata = MetaData()
    table = Table(
        "_int_tz",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("ts", DateTime(timezone=True)),
    )
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS _int_tz"))
        await conn.run_sync(metadata.create_all)

    try:
        original = dt.datetime(2026, 6, 5, 12, 34, 56, 789012, tzinfo=dt.timezone.utc)
        async with engine.begin() as conn:
            await conn.execute(table.insert(), {"id": 1, "ts": original})

        async with engine.connect() as conn:
            ts = (
                await conn.execute(text("SELECT ts FROM _int_tz WHERE id = 1"))
            ).scalar()

        assert ts.tzinfo is not None, f"tzinfo was lost on round-trip: {ts!r}"
        assert ts == original, f"datetime mismatch: {ts!r} != {original!r}"
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS _int_tz"))


async def test_datetime_microsecond_precision_preserved(engine):
    """Six-digit microseconds round-trip exactly.

    The old ``_ADA_DATETIME_MIXIN.ms()`` truncated to milliseconds via
    ``[:-3]``, losing the bottom three digits.
    """
    metadata = MetaData()
    table = Table(
        "_int_micro",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("ts", DateTime()),
    )
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS _int_micro"))
        await conn.run_sync(metadata.create_all)

    try:
        original = dt.datetime(2026, 6, 5, 12, 34, 56, 123456)
        async with engine.begin() as conn:
            await conn.execute(table.insert(), {"id": 1, "ts": original})

        async with engine.connect() as conn:
            ts = (
                await conn.execute(text("SELECT ts FROM _int_micro WHERE id = 1"))
            ).scalar()

        assert ts.microsecond == 123456, (
            f"microseconds truncated: {ts.microsecond} (wanted 123456)"
        )
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS _int_micro"))


async def test_numeric_asdecimal_false_returns_float(engine):
    """``Numeric(asdecimal=False)`` must return ``float``, not ``Decimal``.

    The fix was setting ``supports_native_decimal = True`` on the
    PG dialect. Without it, SQLAlchemy doesn't apply the
    Decimal→float conversion at result-processor time.
    """
    metadata = MetaData()
    table = Table(
        "_int_num",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("v_decimal", Numeric(precision=8, scale=4, asdecimal=True)),
        Column("v_float", Numeric(precision=8, scale=4, asdecimal=False)),
    )
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS _int_num"))
        await conn.run_sync(metadata.create_all)

    try:
        async with engine.begin() as conn:
            await conn.execute(
                table.insert(),
                {"id": 1, "v_decimal": Decimal("12.3456"), "v_float": 12.3456},
            )

        async with engine.connect() as conn:
            row = (
                await conn.execute(
                    table.select().where(table.c.id == 1)
                )
            ).one()

        assert isinstance(row.v_decimal, Decimal), type(row.v_decimal)
        assert isinstance(row.v_float, float), type(row.v_float)
        assert row.v_float == pytest.approx(12.3456)
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS _int_num"))


async def test_insert_on_conflict_returning_drains_when_zero_rows(engine):
    """``INSERT ... ON CONFLICT DO NOTHING RETURNING ...`` returning 0
    rows must drain its cursor without leaving the connection wedged.

    Targets the cursor-close bug class we hand-debugged repeatedly:
    when the conflict path matched, the RETURNING clause produced no
    rows, but the cursor was left mid-stream and the next operation
    on the same connection would explode with ``MissingGreenlet`` or
    similar pool-teardown noise.
    """
    metadata = MetaData()
    table = Table(
        "_int_conflict",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(64)),
    )
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS _int_conflict"))
        await conn.run_sync(metadata.create_all)
        await conn.execute(table.insert(), {"id": 1, "name": "existing"})

    try:
        async with engine.connect() as conn:
            trans = await conn.begin()
            stmt = (
                pg_insert(table)
                .values(id=1, name="duplicate")
                .on_conflict_do_nothing(index_elements=["id"])
                .returning(table.c.id)
            )
            result = await conn.execute(stmt)
            rows = result.fetchall()
            assert rows == []  # conflict path took it; no row returned

            # The cursor must be fully drained. Next query on the same
            # connection must succeed cleanly.
            sanity = (
                await conn.execute(
                    text("SELECT count(*) FROM _int_conflict")
                )
            ).scalar()
            assert sanity == 1
            await trans.rollback()

        # Connection back in pool — next checkout still healthy.
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT 1"))).scalar() == 1
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS _int_conflict"))


async def test_bulk_insert_returning(engine):
    """Bulk INSERT with RETURNING — the ``survey_db.bulk_validation``
    hot path. Many rows, RETURNING the primary key of each.
    """
    metadata = MetaData()
    table = Table(
        "_int_bulk",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("email", String(128)),
    )
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS _int_bulk"))
        await conn.run_sync(metadata.create_all)

    try:
        rows = [{"id": i, "email": f"u{i}@example.com"} for i in range(50)]
        async with engine.begin() as conn:
            stmt = pg_insert(table).returning(table.c.id, table.c.email)
            result = await conn.execute(stmt, rows)
            returned = result.fetchall()

        assert len(returned) == 50
        assert {r.id for r in returned} == set(range(50))
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS _int_bulk"))
