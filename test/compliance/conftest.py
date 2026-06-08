"""
SQLAlchemy compliance suite conftest for the aurora-data-api fork.

Loads ``test/.env`` (produced by ``scripts/provision_test_cluster.py``),
registers the async Postgres Data API dialect under the SQLAlchemy
dialect registry, then star-imports SQLAlchemy's pytest plugin so the
compliance suite is discoverable from this directory.

Idiom notes for future-you:
* The star import of ``sqlalchemy.testing.plugin.pytestplugin`` is
  canonical (see SQLAlchemy ``README.dialects.rst``). It pulls the
  plugin's ``pytest_configure`` / ``pytest_sessionstart`` etc. into
  this module's namespace so pytest treats them as conftest hooks.
* ``pytest_plugins = [...]`` is NOT a drop-in replacement — it defers
  plugin loading until after conftest body runs, which lets earlier
  imports of ``sqlalchemy.testing.fixtures.base`` decorate
  ``TestBase.connection`` under the no-op ``_NullFixtureFunctions``
  and silently drop the fixture (manifests as ``fixture 'connection'
  not found`` at test-setup time).
* Our chained ``pytest_configure`` / ``pytest_sessionstart`` capture
  the plugin's functions BEFORE redefinition and call them first.

The driver picks up ``AURORA_CLUSTER_ARN`` / ``AURORA_SECRET_ARN`` from
``os.environ`` when those connect_args aren't passed explicitly, so the
URL itself is just the empty-credentials form
``postgresql+auroradataapiasync://:@/<dbname>``.
"""
import os
from pathlib import Path

import pytest


def _load_dotenv() -> None:
    # ``.env`` lives at ``test/.env`` (one level up) and is shared by
    # the compliance + integration suites.
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())


_load_dotenv()


def _patch_testing_engine_for_async() -> None:
    """``setup_config`` calls ``engines.testing_engine(url, db_opts)`` without
    ``asyncio=True`` even when the dialect is ``is_async=True``. Without this
    patch our URL routes through sync ``create_engine`` instead of
    ``create_async_engine``. Built-in async dialects sidestep this because
    SQLAlchemy ships them; third-party async dialects need a conftest patch.
    """
    from sqlalchemy.testing import engines

    _original = engines.testing_engine

    def _patched(url=None, options=None, **kwargs):
        url_str = str(url) if url else ""
        if "auroradataapiasync" in url_str and "asyncio" not in kwargs:
            kwargs["asyncio"] = True
        return _original(url, options, **kwargs)

    engines.testing_engine = _patched


_patch_testing_engine_for_async()


def _patch_pg_post_configure_for_async() -> None:
    """Base postgres provisioning calls sync ``engine.connect()`` to install
    citext/hstore extensions. ``AsyncEngine.connect()`` returns an
    ``AsyncConnection`` that only supports ``async with``. The dispatch in
    ``provision.register.__call__`` keys only on backend ("postgresql"), so
    we wrap the registered ``postgresql`` hook to swap in ``sync_engine``
    when handed an ``AsyncEngine``.

    Also ensure the ``test_schema`` / ``test_schema_2`` schemas exist —
    the compliance suite hard-codes those names (``config.py:329``) and
    assumes they're pre-provisioned. Without them ~800 tests error with
    ``schema "test_schema" does not exist``.

    Force-import the postgres provision module first so its ``@for_db``
    registrations land before we wrap them.
    """
    import sqlalchemy.dialects.postgresql.provision  # noqa: F401
    from sqlalchemy import text
    from sqlalchemy.testing.provision import post_configure_engine

    original = post_configure_engine.fns.get("postgresql")
    if original is None:
        return

    def patched(url, engine, follower_ident):
        sync_engine = getattr(engine, "sync_engine", engine)
        original(url, sync_engine, follower_ident)
        with sync_engine.connect() as conn:
            for schema in ("test_schema", "test_schema_2"):
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            conn.commit()

    post_configure_engine.fns["postgresql"] = patched


_patch_pg_post_configure_for_async()


from sqlalchemy.dialects import registry  # noqa: E402

registry.register(
    "postgresql.auroradataapiasync",
    "sqlalchemy_aurora_data_api",
    "AuroraPostgresDataAPIAsyncDialect",
)


pytest.register_assert_rewrite("sqlalchemy.testing.assertions")
from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: E402, F401, F403


_plugin_pytest_configure = pytest_configure  # noqa: F405
_plugin_pytest_sessionstart = pytest_sessionstart  # noqa: F405


def pytest_configure(config):
    _plugin_pytest_configure(config)


def pytest_sessionstart(session):
    _plugin_pytest_sessionstart(session)

    # AsyncEngine doesn't expose ``_run_ddl_visitor``; the compliance suite's
    # ``TablesTest._setup_once_tables`` calls ``metadata.create_all(cls.bind)``
    # which dispatches via that sync-only method. Hand back the underlying
    # sync engine — it drives the AsyncAdapt DBAPI the same way, and the
    # whole fixture runs inside a greenlet (via ``_maybe_async``), so
    # ``await_only`` still bridges to the running event loop.
    from sqlalchemy.testing.fixtures import sql as sql_fixtures
    from sqlalchemy.testing.fixtures import base as base_fixtures
    from sqlalchemy.testing import config as sa_config

    def _sync_db():
        db = sa_config.db
        return getattr(db, "sync_engine", db)

    def setup_bind(cls):
        return _sync_db()

    sql_fixtures.TablesTest.setup_bind = classmethod(setup_bind)

    # ``TestBase.connection`` / ``connection_no_trans`` / ``future_connection``
    # / ``trans_ctx_manager_fixture`` etc. all do ``eng = getattr(self, "bind",
    # None) or config.db`` then ``eng.connect()`` synchronously. When ``bind``
    # isn't set (test classes that aren't ``TablesTest`` subclasses), that
    # falls through to the AsyncEngine and ``conn.begin()`` later raises
    # ``AsyncContextNotStarted``. Replace these with versions that route
    # through ``sync_engine`` — same greenlet bridging argument as above.
    from sqlalchemy.testing import config as _cfg
    import functools as _functools

    def _connection_no_trans(self):
        eng = getattr(self, "bind", None) or _sync_db()
        eng = getattr(eng, "sync_engine", eng)
        with eng.connect() as conn:
            yield conn

    def _connection(self):
        eng = getattr(self, "bind", None) or _sync_db()
        eng = getattr(eng, "sync_engine", eng)
        conn = eng.connect()
        trans = conn.begin()
        base_fixtures._connection_fixture_connection = conn
        yield conn
        base_fixtures._connection_fixture_connection = None
        if trans.is_active:
            trans.rollback()
        conn.close()

    base_fixtures.TestBase.connection_no_trans = _cfg.fixture()(_connection_no_trans)
    base_fixtures.TestBase.connection = _cfg.fixture()(_connection)

    # ``drop_all_tables_from_metadata(metadata, engine)`` does
    # ``with engine.begin()`` which returns ``_AsyncGeneratorContextManager``
    # for AsyncEngine — same async/sync ctx-manager mismatch. Wrap the
    # function so its engine argument is always the sync proxy.
    from sqlalchemy.testing import util as testing_util

    _orig_drop_all = testing_util.drop_all_tables_from_metadata

    def _patched_drop_all(metadata, engine_or_connection):
        if engine_or_connection is not None and hasattr(
            engine_or_connection, "sync_engine"
        ):
            engine_or_connection = engine_or_connection.sync_engine
        return _orig_drop_all(metadata, engine_or_connection)

    testing_util.drop_all_tables_from_metadata = _patched_drop_all

    # ``base_fixtures.drop_all_tables_from_metadata`` is the same name
    # re-bound at import time — patch it too.
    base_fixtures.drop_all_tables_from_metadata = _patched_drop_all
