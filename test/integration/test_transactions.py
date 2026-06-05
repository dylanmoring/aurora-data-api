"""
Transaction semantics: commit, rollback, read-only flow, savepoints.

Bug class this targets — from the recent commit log on ``main``:
* "Commit read-only sessions to silence Data API rollback noise"
  (``2d22e69``): sessions that only did SELECTs were implicitly
  rolled back at scope exit, producing ERROR-level log lines.
* "Replace dead session-level idle-tx SET with a transaction-scoped
  one" (``61559bb``): ``SET LOCAL`` semantics matter for our usage.
* "Wrap stray session.refresh in run_store_validation in its own tx"
  (``708f959``): refresh outside a tx broke transaction lifetime
  invariants.

These are the same shapes the integration suite should pin down.
"""
import pytest
from sqlalchemy import text


async def test_explicit_commit_persists(engine, scratch_table):
    """A commit makes the write visible to a later connection."""
    async with engine.connect() as conn:
        trans = await conn.begin()
        await conn.execute(
            scratch_table.insert(), {"id": 1, "name": "committed"}
        )
        await trans.commit()

    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT name FROM _int_scratch WHERE id = 1")
            )
        ).one()
        assert row.name == "committed"


async def test_implicit_commit_at_begin_exit(engine, scratch_table):
    """``async with engine.begin()`` commits on clean exit."""
    async with engine.begin() as conn:
        await conn.execute(
            scratch_table.insert(), {"id": 2, "name": "implicit-commit"}
        )

    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT name FROM _int_scratch WHERE id = 2")
            )
        ).one()
        assert row.name == "implicit-commit"


async def test_rollback_on_exception(engine, scratch_table):
    """An exception inside ``async with engine.begin()`` rolls back."""
    class BoomError(Exception):
        pass

    with pytest.raises(BoomError):
        async with engine.begin() as conn:
            await conn.execute(
                scratch_table.insert(), {"id": 3, "name": "doomed"}
            )
            raise BoomError("simulated failure")

    async with engine.connect() as conn:
        count = (
            await conn.execute(
                text("SELECT count(*) FROM _int_scratch WHERE id = 3")
            )
        ).scalar()
        assert count == 0


async def test_read_only_session_does_not_log_rollback_noise(engine, caplog):
    """Read-only sessions should NOT produce rollback-level log noise.

    Targets the bug fixed in ``2d22e69`` — sessions that only ran
    SELECTs were implicitly rolled back at scope exit, producing
    Data-API ``ROLLBACK`` log lines visible at WARNING/ERROR. The
    driver-level fix commits read-only sessions so the noise stops.

    ``caplog`` collects every log record at the active level; we assert
    nothing at WARNING+ mentions rollback.
    """
    import logging

    with caplog.at_level(logging.WARNING):
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
            await conn.execute(text("SELECT 2"))
            await conn.execute(text("SELECT 3"))

    rollback_records = [
        r for r in caplog.records if "rollback" in r.getMessage().lower()
    ]
    assert not rollback_records, (
        f"read-only session produced {len(rollback_records)} rollback log "
        f"records: {[r.getMessage() for r in rollback_records[:3]]}"
    )


async def test_transaction_scoped_set_local(engine):
    """``SET LOCAL`` ties a setting to the current transaction.

    Targets ``61559bb`` — we shifted from a session-level SET (which
    didn't work since Data API has no persistent session) to a
    transaction-scoped one. Verify the setting is visible inside the
    same txn and gone afterwards.
    """
    async with engine.begin() as conn:
        await conn.execute(text("SET LOCAL idle_in_transaction_session_timeout = 30000"))
        value = (
            await conn.execute(text("SHOW idle_in_transaction_session_timeout"))
        ).scalar()
        # PG normalizes to "30s" — accept any string starting with "30".
        assert value.startswith("30"), f"SET LOCAL didn't stick: {value!r}"

    # New transaction — setting should be back to default.
    async with engine.begin() as conn:
        value = (
            await conn.execute(text("SHOW idle_in_transaction_session_timeout"))
        ).scalar()
        assert not value.startswith("30") or value == "0", (
            f"SET LOCAL leaked across transactions: {value!r}"
        )
