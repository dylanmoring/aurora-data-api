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
import pytest

from test import load_dotenv

load_dotenv()


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


# Canonical third-party-dialect idiom (per README.dialects.rst). The star
# import binds the plugin's ``pytest_configure`` / ``pytest_sessionstart``
# hooks into this conftest's namespace, where pytest discovers them directly.
pytest.register_assert_rewrite("sqlalchemy.testing.assertions")
from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: E402, F401, F403


# Data API rejects named-params with chars outside [A-Za-z0-9_]; deselect
# the specific DifficultParametersTest combinations that can't pass.
_DATA_API_REJECTED_PARAM_CHARS = ("/slashes/", "more/slashes", "q?marks")
# Tests the Data API service contract fundamentally can't satisfy.
_DATA_API_INCOMPATIBLE_TESTS = ("test_round_trip_custom_json",)


def pytest_collection_modifyitems(config, items):
    keep, deselected = [], []
    for item in items:
        if any(f"[{p}]" in item.name for p in _DATA_API_REJECTED_PARAM_CHARS):
            deselected.append(item)
        elif any(t in item.name for t in _DATA_API_INCOMPATIBLE_TESTS):
            deselected.append(item)
        else:
            keep.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = keep


# setup.cfg's ``[db] default`` is the async URL (shared with the async
# compliance suite). SA's ``_engine_uri`` reads
# ``file_config["db"]["default"]`` at ``post_begin`` time; we have to
# remap default -> the ``sync`` entry so this directory actually drives
# ``postgresql+auroradataapi://`` (the sync dialect/driver). Without
# this, ``pytest test/compliance_sync`` silently runs against the async
# variant with a bare conftest -- not the dialect isolation it's meant
# to provide.
_plugin_pytest_configure = pytest_configure  # noqa: F405


def pytest_configure(config):
    _plugin_pytest_configure(config)
    from sqlalchemy.testing.plugin import plugin_base
    sync_url = plugin_base.file_config.get("db", "sync")
    plugin_base.file_config.set("db", "default", sync_url)
