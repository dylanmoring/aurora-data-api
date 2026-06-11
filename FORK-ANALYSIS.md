# Fork Analysis — `aurora-data-api` (driver layer)

> Comprehensive analysis of this fork vs. its upstream baseline.
> Generated 2026-06-10. Companion document: `FORK-ANALYSIS.md` in the
> **sqlalchemy-aurora-data-api** repo (the dialect that depends on this driver).

## Baseline & how to reproduce

- **Upstream:** `https://github.com/chanzuckerberg/aurora-data-api` (added as the
  `upstream` git remote during this analysis).
- **Merge-base:** `ef6208e30e3b9823d8540342ba0e43cd8d24e64c`
- **Divergence:** 43 custom commits, **+3980 / −345 lines**; fork is **2 commits
  behind** upstream `main`.
- **Diff everything we changed:** `git diff ef6208e..HEAD`

### ⚠️ Upstream independently added async + DBAPI conformance after our fork point

Upstream `main` now contains two commits we do **not** have:

- `2d60d0c Async Version (#51)`
- `6f81da3 Enhance DB-API 2.0 (PEP 249) Conformance (#52)`

Our async support is a **parallel implementation**, not built on theirs. This is
the root cause of the "missing upstream bugfixes" section below. Convergence onto
upstream's async was scoped *out* of this analysis (flag-only).

---

## 🔴 Action items (confirmed regressions / defects, ranked)

| # | Severity | Location | Issue |
|---|----------|----------|-------|
| 1 | **High** | `type_conversion.py:169` | **Nested-array decode crashes.** `arr` is already `value_dict["arrayValue"]`, so `arr["arrayValue"]["arrayValues"]` double-unwraps → `KeyError`. Fix: `arr["arrayValues"]`. Verified by direct repro. Likely masked today because the Data API rejects multi-dim array *results* (see `LIMITATIONS.md`), but it's a latent crash. |
| 2 | **High** | `__init__.py:259-263` | **Sync `cursor()` no longer auto-begins a transaction.** Upstream (merge-base `__init__.py:94-102`) called `begin_transaction` when none was active. Now nothing on the raw sync path calls `start_transaction()` — only the SQLAlchemy dialect does (`do_begin`). Raw DBAPI users doing `cur.execute(); conn.commit()` now get per-statement autocommit; `commit()`/`rollback()` become no-ops. Multi-statement atomicity and rollback-on-error are lost for non-SA callers. |
| 3 | **High** | `async_driver.py:300-308` | **Async oversize-retry silently swallows non-matching errors.** Inner `except UnsupportedResultException as e:` logs "Retrying…" then only re-paginates inside `if "...size limit..." in str(e):` with **no `else: raise`**. A differing oversize message (or any other `UnsupportedResultException`) is caught and dropped — `execute()` returns empty with no error. The sync path (`__init__.py`) correctly re-raises. |
| 4 | **Med** | `__init__.py` (pagination trigger) | **Pagination trigger exception/message changed.** Upstream paginated on `BadRequestException`/`DatabaseErrorException` with `"Please paginate your query"` / `"...allowed response size limit"`. Now it only paginates on `UnsupportedResultException` + `"The result exceeds the size limit"`, and routes `BadRequestException` straight to `translate_database_error` → raise. If the service still emits the old type/message for some engine/region, large SELECTs that used to auto-paginate will now error. **Verify against a >1 MiB result on the target cluster.** |
| 5 | **Med** | `type_conversion.py:25,27` | **`NUMBER` / `ROWID` DBAPI type-codes changed.** `NUMBER` is now the tuple `(int, float)` (was `float`); `ROWID` is `int` (was `str`). Breaks the PEP-249 idiom `desc.type_code == NUMBER` (a tuple never compares equal). Upstream #52 keeps the conventional values. |
| 6 | **Med** | `setup.py` | **Python 3.8/3.9 advertised but broken.** Classifiers list 3.8–3.12, but evaluated return annotations use PEP-604 unions, e.g. `-> tuple | None` (`__init__.py:457`, `async_driver.py:349`). These evaluate at def-time → `TypeError` on 3.8/3.9. Import fails on the advertised floors. |
| 7 | **Med** | `setup.py` | **`aioboto3` is a hard base dependency.** `install_requires` includes `aioboto3 >= 15.0.0`, so sync-only users pull the full async stack (aiobotocore version-conflict risk). Upstream #51 gated async behind an extra (`[async]`). |
| 8 | **Low** | `retry.py:105` | **`_should_retry` does substring matching on exception type names** with fragile `and`/`or` precedence; the `any(...)` arm runs even when `exc_names` is empty. Safe only because the single configured name (`"DatabaseResumingException"`) never collides. |
| 9 | **Low** | `exceptions/translate.py:43`, `__init__.py:288` | **Broad `except Exception → DatabaseError`** downgrades specific DBAPI subclasses (defeats `except IntegrityError`) and masks credential/endpoint failures during `start_transaction`. |

---

## ✨ Features added

- **Async driver** — new `async_driver.py`: `AsyncAuroraDataAPIClient` /
  `AsyncAuroraDataAPICursor` + module-level `async def connect()`, built on
  aioboto3. Does not inherit from the sync driver; shares only pure helpers from
  `type_conversion` / `exceptions`. *(commits `791239f`, `e9c825b`, `dac06ac`)*
- **SQLAlchemy async-dialect adapters** — `AuroraDataAPIAsyncAdaptCursor` /
  `AuroraDataAPIAsyncAdaptConnection` (`async_driver.py:524-569`) subclass SA's
  `AsyncAdapt_dbapi_*` to greenlet-bridge sync DBAPI calls; adds
  `start_transaction` for the dialect's `do_begin`. *(`0738f54`)*
- **RETURNING on the async path** — async `execute` gates `description` on
  `_statement_returns_rows`, so `INSERT/UPDATE/DELETE … RETURNING` produce a
  result set. *(`075da4d`)*
- **Retry module** — `retry.py`: `retry_exceptions(4, 2, 2, 4, exceptions="DatabaseResumingException")`
  decorator (sync + async aware) wrapping `start_transaction`, `execute`,
  `executemany` for Aurora resume/cold-start. *(`5297920`, `9c3eaa6`)*
- **`type_conversion.py`** — centralized `build_description`, `format_parameters`,
  `convert_value`, PG-type→Python map, Data-API field/typeHint maps, DBAPI type
  constructors. *(`24bf065`)*
- **Region-from-ARN derivation** — `_region_from_arn` builds the boto3 client in
  the cluster's region when none is injected, fixing "Invalid region in ARN".
  New `region_name` kwarg on both `connect()`. *(`e1ef2d5`, `0738f54`)*
- **Automatic pagination on size-limit** — declares a server-side
  `SCROLL CURSOR`, buffers pages, and on repeated oversize **rewinds and halves
  `records_per_page`** down to 1. *(`3324ce2`, `8a413f5`, `5e3d42b`)*
- **Exception package with SQLSTATE→DBAPI mapping** — `exceptions/`:
  `_PG_SQLSTATE_CLASS_TO_BASE` maps the 2-char SQLSTATE class to the right DBAPI
  base (`23`→IntegrityError, `42`→ProgrammingError, `22`→DataError,
  `08/40/57`→OperationalError). `translate_database_error` makes `Position:`
  optional so integrity/FK/unique errors translate. *(`bdc38ad`, `5b8088b`)*
- **tz-aware datetimes** for `timestamptz` / `timetz` columns via
  `_PG_TZ_AWARE_TYPES` + a new `pg_type_name` field on `ColumnDescription`.
  *(`59303f2`)*
- **Python `list` → `arrayValue` serialization** — `_list_to_array_value`
  recursively maps lists to typed `*Values`, nested lists to `arrayValues`,
  mixed/None to stringified fallback. *(`109d91b`)*
- **DBAPI 2.0 module-level conformance** — `Binary`, `Date`, `Time`, `Timestamp`,
  `*FromTicks`, `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID` defined once and
  re-exported; `TimeFromTicks` implemented (was a TODO); `uuid.UUID` + PG-internal
  id types (`oid/regproc/regclass/xid/cid`→int) coercion. *(`0738f54`)*

## 🐛 Bug fixes

- **Returns-rows detection** — upstream set `description` whenever
  `columnMetadata` was present, making `CursorResult.returns_rows` lie for plain
  DML. Now gated by `_statement_returns_rows`, a paren-aware hand-rolled scanner
  (`_leading_keyword`) handling leading `(`, `WITH [RECURSIVE] … AS (…)` chains,
  `MATERIALIZED`, quoting, and `--` / `/* */` comments. *(`d13e3bc`, `d3ca0d4`)*
- **Stale description leak across statements** — `execute`/`executemany` reset
  `self.description = None` up front so a prior SELECT's description doesn't flip a
  following INSERT to returns-rows (SA reuses cursors for `nextval`+INSERT).
  *(`d13e3bc`)*
- **`fetchone` on empty results** — rewritten to a buffer-pop returning `None`
  cleanly instead of relying on `next(self._iterator)`. *(`a016f0d`)*
- **`executemany`** — batches via `_page_input` with explicit `page_size`, shared
  `format_parameters`, translated `BadRequestException`. *(`76c7246`, `b48dbc1`)*
- **PG result strings coerced to native types** — oid/regproc/etc. and UUID columns
  returned as `stringValue` are coerced, fixing `operator does not exist: oid = text`
  on reflection round-trips. *(`4fbcf59`)*
- **`uuid` type-code** corrected from the *function* `uuid.uuid4` to the *type*
  `uuid.UUID`. *(`4fbcf59`)*
- **Async rollback shielded against cancellation** — runs the rollback request as a
  detached task under `asyncio.shield` so a Lambda-timeout cancel doesn't abort an
  in-flight rollback. *(`fbeca37`)* — *(best-effort; see Risk notes)*
- **Quoted/scrollable cursor name & `FETCH FORWARD`** for paginated cursors.

## ♻️ Refactors

| Refactor | Verdict |
|----------|---------|
| Split type machinery → `type_conversion.py` (`24bf065`) | **Worthwhile** — precondition for sharing helpers across sync/async. |
| Split exceptions → `exceptions/` package + new `translate.py` (`bdc38ad`) | **Worthwhile behavior** (SQLSTATE mapping), but `exceptions/__init__.py` does `from .exceptions import *` with no `__all__`; `translate_database_error` is reachable on the top-level package but **not** on the `exceptions` subpackage in isolation — a fragile import-order artifact. |
| Connection/Cursor responsibility split — `AuroraDataAPICursor(connection, ...)`, `self._client`→`self.client` (`4f2ce3d`) | **Worthwhile** — clearer ownership. |
| Table-driven coercion / DBAPI dedup (`b48dbc1`, `b0c0693`) | **Worthwhile.** |
| Buffer-based iteration shared by `__iter__`/`fetch*` (`5e3d42b`, `8a413f5`) | Mostly clarity, **but the buffer/paging helpers are copy-pasted between sync and async files, not shared** (see Duplication). |

### Dead code / churn

- **`async_driver.py:39-54`** — the 11-name DBAPI-type re-import block
  (`Binary`, `Date`, `Time`, `STRING`, …) is **unused in that module** (grep
  confirms zero references). The contract only requires them on the top-level
  package. Vestigial.
- **`retry.py:1-51`** — the module docstring documents a decorator
  `retry_resuming(3, 6, 12, …)` that **does not exist**; the real name is
  `retry_exceptions`. Stale GPT-generated doc that survived the rename.
- **`retry.py:67-99`** — `_normalize_exceptions` is a fully generic
  class|iterable|str|mixed normalizer (~40 lines) for a decorator called exactly
  one way at all 6 sites. Over-engineered for a single string literal.
- **`async_driver.py:209,538-542`** — the async cursor factory is `async def`
  solely to support `await cur.execute("SET character_set_client…")` for MySQL,
  but the comment admits that path is unused for Postgres and there's no async
  MySQL test path. Speculative generality.

### Duplication (sync ↔ async copy-paste)

- `_fetch_next_page_into_buffer`, `_has_buffered_row`, `_pop_buffered_row`,
  `_page_input`, `_render_response`, `scroll`, `fetchone/many/all`, `rowcount`,
  `lastrowid`, and the `start_transaction` body are near-verbatim between
  `__init__.py` and `async_driver.py` (~120 lines), differing only by `await`.
  The pure-logic helpers could live in a shared mixin.
- **Gratuitous drift:** `__init__.py` emits `FETCH FORWARD {n}` while
  `async_driver.py` emits `FETCH {n}` — equivalent SQL, but divergent copy-paste.
- The datetime strptime fallback ladder in `convert_value` is duplicated **again**
  in the dialect's `_ADA_DATETIME_MIXIN.result_processor` (cross-repo).

## 📉 Missing upstream bugfixes (flag-only, from #51 / #52)

- **`DECIMAL` not exported** at top level (`hasattr(aurora_data_api, 'DECIMAL')`
  → False); upstream #52 exposes it. DBAPI consumers probing `dbapi.DECIMAL` get
  `AttributeError`.
- **`NUMBER` / `ROWID`** diverge from PEP-249-conventional values (see action #5).
- **`TimeFromTicks`** implementation differs from upstream's `localtime`-based
  construction (both functional).
- **No PEP-249 conformance test** (upstream #52 added `test/base.py` +
  `test_compat.py`). Our compliance suites don't assert the module-level DBAPI
  contract, which is why the `NUMBER`/`DECIMAL`/`ROWID` divergences went uncaught.

## ⚠️ Risk notes

- **Retry wraps `execute`/`executemany`, not just begin** — a mid-statement
  `DatabaseResumingException` can re-run non-idempotent DML up to 4× (and blocks
  the thread with `time.sleep` totaling ~12s on the sync path). Combined with
  action #2 (no auto-begin), a raw multi-batch `executemany` has no atomicity.
- **`rowcount` for paginated results is wrong** — returns
  `len(self._current_response["records"])`, which is unset/empty under pagination
  (the comment admits "non-paginated only").
- **Async rollback shield** (`fbeca37`) is best-effort: the shielded task is never
  awaited after `CancelledError` propagates, so under immediate loop teardown the
  rollback may not land server-side. Verify under real imminent-timeout cancel.
- **`print()` in import path** — the dialect's `register_dialects()` prints to
  stdout; noted here because it's triggered by importing this driver's dialect.

---

## Test & infrastructure additions (summary)

New surfaces, none of which existed at the merge-base:

- **`test/integration/`** — real-cluster `pytest-asyncio` suite using vanilla SA
  async API: lifecycle, transactions (incl. read-only no-rollback-noise,
  transaction-scoped `SET LOCAL`), concurrency (`asyncio.gather`, cancel-mid-query
  pool poisoning), error mapping, types (tz-aware round-trip, microsecond
  precision, `Numeric(asdecimal=False)`→float).
- **`test/compliance/` (async) + `test/compliance_sync/`** — SQLAlchemy
  third-party-dialect compliance harness; shared `requirements.py` (433 lines).
  Async conftest carries heavy monkeypatches to route the async dialect through
  the plugin's sync expectations.
- **`test/test_param_serialization.py`, `test/test_returns_rows.py`** — pure-Python
  unit tests for `_prepare_param`/`_list_to_array_value` and the
  `_statement_returns_rows` heuristic (30+ cases incl. recursive CTEs).
- **`scripts/provision_test_cluster.py` / `teardown_test_cluster.py`** — provision
  Aurora PG Serverless v2 16.6, Secrets Manager secret, write `test/.env`.
- **Docs:** `LIMITATIONS.md` (response-size pagination behavior, raw-driver
  outside-transaction pagination gap), `test/BASELINE-FINDINGS.md`,
  `test/SKIPS.md` (252 categorized compliance skips).

> **Doc drift:** `BASELINE-FINDINGS.md` (dated 2026-06-05) still describes
> reflection as broken, but `requirements.py` (current) says the four root causes
> are patched. Reconcile.
