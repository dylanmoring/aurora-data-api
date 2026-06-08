"""
SQLAlchemy compliance suite conftest for the *sync* dialect.

This is the dialect-isolation harness — drives ``postgresql+auroradataapi://``
(NOT the async variant). No AsyncEngine, no async fixture routing patches,
no ``_run_ddl_visitor`` / ``_AsyncGeneratorContextManager`` errors. What's
left is genuine dialect / driver behavior: SQL generation, type coercion,
DBAPI surface, and whatever Aurora Data API itself can or can't do.

The conftest is much smaller than the async variant — only two patches
needed, both for genuine Aurora Data API plumbing (`test_schema`
provisioning + the canonical conftest idiom). All async-engine plumbing
from the sister conftest is unnecessary here.
"""
import os
from pathlib import Path

import pytest


def _load_dotenv() -> None:
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


def _patch_pg_post_configure_to_create_schemas() -> None:
    """Compliance suite hard-codes ``test_schema`` / ``test_schema_2``
    (``config.py:329``). We have to pre-create them — the sister async
    conftest does this AND has to wrap the hook in a sync_engine
    unwrapper; the sync side just needs the CREATE SCHEMA part.
    """
    import sqlalchemy.dialects.postgresql.provision  # noqa: F401
    from sqlalchemy import text
    from sqlalchemy.testing.provision import post_configure_engine

    original = post_configure_engine.fns.get("postgresql")
    if original is None:
        return

    def patched(url, engine, follower_ident):
        original(url, engine, follower_ident)
        with engine.connect() as conn:
            for schema in ("test_schema", "test_schema_2"):
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            conn.commit()

    post_configure_engine.fns["postgresql"] = patched


_patch_pg_post_configure_to_create_schemas()


from sqlalchemy.dialects import registry  # noqa: E402

# Sync dialect — the URL scheme is ``postgresql+auroradataapi`` (no
# ``async`` suffix).
registry.register(
    "postgresql.auroradataapi",
    "sqlalchemy_aurora_data_api",
    "AuroraPostgresDataAPIDialect",
)


# Canonical third-party-dialect idiom (per README.dialects.rst).
pytest.register_assert_rewrite("sqlalchemy.testing.assertions")
from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: E402, F401, F403


# Capture the plugin's hooks before we shadow them via local redefinition.
_plugin_pytest_configure = pytest_configure  # noqa: F405
_plugin_pytest_sessionstart = pytest_sessionstart  # noqa: F405


def pytest_configure(config):
    _plugin_pytest_configure(config)


def pytest_sessionstart(session):
    _plugin_pytest_sessionstart(session)
