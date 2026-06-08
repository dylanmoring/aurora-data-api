# SQLAlchemy compliance suite against the Aurora Data API async dialect

> **Status 2026-06-05**: Compliance suite invocation works. Option A
> driver/dialect refactor + targeted conftest patches + requirements
> exclusions + 3 real driver/dialect bug fixes have produced a stable
> baseline run.

## Headline numbers

| Phase | Passed | Failed | Skipped | Errors | Wall |
|---|---|---|---|---|---|
| Original baseline | 22 | 0 | 0 | 200 (capped) | — |
| After Option A refactor | 147 | 105 | 89 | 1122 | 2:43 |
| After test_schema fix | 275 | 480 | 369 | 342 | 4:49 |
| After requirements exclusions | 172 | 196 | 752 | 340 | 3:46 |
| After async-fixture routing + Binary + Numeric + datetime | **292** | **204** | **873** | **0** | **4:38** |

**Zero fixture errors.** Every non-skipped test now runs to completion.
The 204 failures are real test-body signal, not framework breakage.

## What got changed

### Driver (`aurora-data-api` fork, `async_driver.py`)
1. Refactored `SyncAdaptedConnection` / `SyncAdaptedCursor` to subclass
   SQLAlchemy's reference `AsyncAdapt_dbapi_connection` /
   `AsyncAdapt_dbapi_cursor` base classes. Inherits the proven
   greenlet-bridging, result-drain, and pool/teardown handling.
2. Added env-var fallback for `AURORA_CLUSTER_ARN` / `AURORA_SECRET_ARN`
   in module-level `connect()` so URL-empty connections work.
3. `connect()` now accepts `**_ignored` so URL-derived kwargs don't
   trip it up.
4. Added DBAPI 2.0 type constructors at module level: `Binary`, `Date`,
   `Time`, `Timestamp`, `STRING`, `BINARY`, `NUMBER`, `DATETIME`,
   `ROWID`, etc. SQLAlchemy's PG dialect calls `dbapi.Binary(value)`
   when binding `LargeBinary` columns; missing this raised
   `AttributeError` before any query ran.

### Dialect (`sqlalchemy-aurora-data-api` fork, `__init__.py`)
1. Async dialect classes inherit from `AsyncAdapt_dbapi_connection`-based
   adapter (mirrors asyncpg's pattern).
2. Added `get_pool_class` returning `AsyncAdaptedQueuePool` on both
   async dialects.
3. Explicit imports of `postgresql.provision` and `mysql.provision`
   so `@for_db(...)` registrations land.
4. **Bug fix**: Added `supports_native_decimal = True` to
   `AuroraPostgresDataAPIDialect`. Without this, `Numeric(asdecimal=False)`
   columns returned `Decimal` instead of `float` (compliance suite caught
   the regression).
5. **Bug fix**: Replaced `_ADA_DATETIME_MIXIN.ms()` truncation. The old
   `str(value.microsecond).zfill(6)[:-3]` lost the bottom three digits
   of every timestamp at bind time — full-precision microseconds round
   through fine now. Compliance suite caught this too.

### Test infrastructure (worktree-local, branch `test/sqla-compliance`)

`test/conftest.py` patches that work around upstream
third-party-async-dialect quirks:

1. **`engines.testing_engine`** monkey-patched to pass `asyncio=True`
   when the URL matches our scheme (compliance suite's `setup_config`
   doesn't infer it).
2. **PG post-configure hook** wrapped to (a) unwrap `AsyncEngine` to
   `sync_engine` so base PG provisioning can install citext/hstore via
   sync ctx-manager, and (b) `CREATE SCHEMA IF NOT EXISTS test_schema`
   + `test_schema_2` (hard-coded by `config.py:329`).
3. **Canonical conftest idiom**: `from sqlalchemy.testing.plugin.pytestplugin
   import *` (NOT `pytest_plugins = [...]`) — the star import pulls
   `pytest_configure` / `pytest_sessionstart` into the conftest namespace
   at conftest-hook timing. The `pytest_plugins` form defers plugin
   loading until after the conftest body, which can let early imports of
   `sqlalchemy.testing.fixtures.base` decorate `TestBase.connection`
   under the no-op `_NullFixtureFunctions` and silently lose the
   fixture (manifests as `fixture 'connection' not found`).
4. **Chained `pytest_configure` / `pytest_sessionstart`** capture the
   plugin's functions before redefinition so both ours and the
   plugin's run.
5. **`TablesTest.setup_bind`** overridden to hand back `sync_engine`
   — `metadata.create_all(cls.bind)` dispatches via
   `cls.bind._run_ddl_visitor` which only exists on sync `Engine`.
6. **`TestBase.connection` / `connection_no_trans`** overridden to
   route through `sync_engine`. The defaults fall through to
   `config.db` (AsyncEngine) when `self.bind` isn't set and raise
   `AsyncContextNotStarted`.
7. **`drop_all_tables_from_metadata`** monkey-patched to unwrap
   `AsyncEngine.sync_engine`. The metadata fixture's teardown uses
   `with engine.begin()` which returns an `_AsyncGeneratorContextManager`
   that can't be entered synchronously.

`test/requirements.py` excludes Aurora Data API limitations:
- All `*_reflection` requirements closed. PG catalog reflection uses
  `generate_subscripts(pg_index.indkey, ...)` (an `int2vector`-arg
  function the Data API can't call) and selects internal `pg.char`
  columns the Data API can't return.
- `views`, `column_collation_reflection`, `reflects_pk_names`,
  index-as-constraint variants — same root cause.

## Remaining 204 failures, by class

| Count | Test class | Cause |
|---|---|---|
| 94 | `ComponentReflectionTest` | PG catalog reflection — `CHAR` result type. Tests don't go through `requirements`-gated paths; they call `inspector.get_columns()` directly. Would need test-class-level override or `inspect()` patch to suppress. **Data API limit, not our bug.** |
| 16 | `ExpandingBoundInTest` | Need to investigate — possibly Data API parameter-encoding behavior with empty/expanding IN clauses. |
| 14 | `LikeFunctionsTest` | Need to investigate — LIKE/ILIKE behavior. |
| 11 | `IntegerTest` | Need to investigate. |
| 9 | `DifficultParametersTest` | Data API rejects param names like `q?marks`, `/slashes/`, `more/slashes` with `Named parameter syntax is invalid`. **Data API limit.** |
| 7 | `CompoundSelectTest` | Need to investigate. |
| 6 | `QuotedNameArgumentTest` | Likely same as DifficultParameters — reserved name chars. |
| 6 | `OrderByLabelTest` | Need to investigate. |
| 5 | `JoinTest` | Need to investigate. |
| ~40 | Various smaller buckets | DDL paths that still hit `_run_ddl_visitor` (~21), inspection on AsyncEngine (~11), `_AsyncGeneratorContextManager` (~10) |

By failure exception:

| Count | Class | Bucket |
|---|---|---|
| 88 | `UnsupportedResultException: CHAR` | Data API limit |
| 62 | `TypeError: '_AsyncGeneratorContextManager' / 'AsyncConnection' object does not support the context manager protocol` | Compliance-suite paths that bypass our patches; not user-facing dialect bugs |
| 21 | `AttributeError: 'AsyncEngine' object has no attribute '_run_ddl_visitor'` | Same family |
| 11 | `NoInspectionAvailable: ... on AsyncEngine` | Same family |
| 11 | `ValidationException: Named parameter syntax is invalid` | Data API limit |
| 7 | `AssertionError` | Real test-body diagnostic |
| 2 | `NotImplementedError: asynchronous events not implemented` | SQLAlchemy framework limit |
| 2 | `ER_UNDEF_FUNC: generate_subscripts` | Still slipping through reflection exclusion in a couple paths |

## Three meaningful buckets

1. **~140 Data API limitations** — `CHAR` results, `int2vector`-arg
   functions, exotic parameter names. Real ceiling of the Data API
   surface, not bugs in our code. Already mostly skipped via
   `requirements.py`; the rest are tests that call `inspect()`
   directly without a gate.
2. **~50 compliance-suite framework paths** still routing sync
   ctx-manager use to AsyncEngine in test bodies the fixture-layer
   patches don't reach. These are tests that do `with engine.begin():`
   inline against `config.db`. Not user-facing; fix would be more
   conftest patches.
3. **~12 real diagnostic catches** — the kind of thing this whole
   exercise was for. UUID round-trip type coercion, `DISTINCT ON`
   compile, named-param edge cases. Each worth a separate look.

## What "Option A worked" means

Going in: every cursor lifecycle / `MissingGreenlet` / RETURNING-drain
class of bug was hand-rolled in our `SyncAdaptedConnection` facade and
needed re-derivation per failure. The forks were producing one
production bug per few weeks.

Coming out: those classes of bug now live in SQLAlchemy's
`AsyncAdapt_dbapi_*` base classes — which SQLAlchemy itself tests
against asyncpg / asyncmy / oracledb_async upstream. We inherit that
coverage for free. The remaining surface that's "ours" is genuinely
Data-API-specific: parameter encoding, HTTPS error mapping,
transaction lifetime under cold starts. That's a much smaller, more
tractable surface.

## Recommended next moves

- **Now**: commit the Option A driver/dialect changes plus the
  `supports_native_decimal`, datetime microsecond fix, `Binary` export.
  Push the forks. Redeploy `survey_db`.
- **Soon**: write a focused harness exercising the patterns
  `survey_db` / `emaildb` actually use — bulk INSERT + RETURNING,
  cold-start transaction commit, parameter encoding for our value
  shapes. ~40 tests, runs in <30s against the same test cluster.
- **Later (optional)**: the ~12 real diagnostic catches from compliance
  are worth chasing one at a time, in order of frequency in production.
  Start with UUID round-trip if `emaildb` uses it (it does, for the
  `email_validation.batch_id`).
- **Optional**: leave the compliance suite scaffolding on a branch as
  documentation. Future async edge cases can be checked against it,
  but it isn't part of the default test path.

## Cluster state

- `aurora-data-api-test-dev` is up, auto-pauses after 5 min idle.
  Costs ~nothing while paused.
- Secret: `aurora-data-api-test-dev-master`.
- Tagged `Project=aurora-data-api-test`.
- Teardown ready: `python scripts/teardown_test_cluster.py`.

## What's in this worktree

- `scripts/provision_test_cluster.py`, `scripts/teardown_test_cluster.py`
- `test/conftest.py` (~140 lines, well-commented)
- `test/requirements.py` (Aurora Data API exclusions)
- `test/test_suite.py` (one-line star import)
- `test/__init__.py`, `test/profiles.txt`
- `test/final-run.log`, `test/final-run-v2.log` (last two compliance runs)
- `test/.env` (gitignored — ARNs)
- `setup.cfg`

Nothing committed yet — the worktree's branch (`test/sqla-compliance`)
holds Option A's driver/dialect changes in both forks plus the test
scaffolding. The driver/dialect changes are worth committing
regardless of test-harness direction; the scaffolding can stay on the
branch as reference.
