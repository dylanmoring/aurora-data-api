"""
SQLAlchemy third-party dialect compliance suite collector.

The single ``from sqlalchemy.testing.suite import *`` brings every test
case in SQLAlchemy's ``sqlalchemy.testing.suite`` package into this
module's namespace, which pytest then collects and runs against the
dialect we registered in ``conftest.py``.

The suite is the canonical way for a third-party dialect to assert
behavioral compliance with what SQLAlchemy expects of a DBAPI/dialect
pairing — cursor lifecycle, RETURNING, isolation levels, type coercion,
pool teardown, async iteration, the lot.
"""
from sqlalchemy.testing.suite import *  # noqa: F401, F403
