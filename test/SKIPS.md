# Compliance-suite skip inventory

Derived from `pytest -rs` output, 2026-06-10. Applies identically to all
four suites (this repo's `compliance` / `compliance_sync` and the
sqlalchemy-aurora-data-api mirrors). Current state: **252 skips in every
suite** (sync and async are now identical; the former 28-skip async
delta was the `supports_sane_rowcount = False` flag, since fixed).

To regenerate: run a suite with `-rs`, aggregate the `SKIPPED [n]` lines.
Class-level skips name the gating requirement via `__requires__`;
method-level skips report a generic "marked as skip" — find the
`@testing.requires.<x>` decorator in `sqlalchemy/testing/suite/`.

## 1. Features PG doesn't have — correct skips, permanent (165)

| Count | Requirement | What it is |
|---|---|---|
| 85 | `reflect_table_options` | MySQL-style `ENGINE=...` table options |
| 69 | `legacy_unconditional_json_extract` | MySQL/SQLite legacy JSON-extract semantics |
| 4+1+1 | `fetch_percent`, `fetch_expression`, `fetch_no_order_by` | `FETCH FIRST n PERCENT` etc. |
| 2 | `denormalized_names` | Oracle-style uppercase name normalization |
| 2 | `nvarchar_types` | NVARCHAR/NCHAR |
| 1 | `dbapi_lastrowid` | `cursor.lastrowid` — PG DBAPIs never have it |

## 2. Data API service walls — can't fix driver-side (39)

| Count | What | Why |
|---|---|---|
| 15 | server-side cursors | stateless HTTPS, no session to hold a cursor |
| 11 | temp tables | fresh PG session per call |
| 4+3 | `unicode_ddl`, `percent_schema_names` | named params limited to `[A-Za-z0-9_]` |
| 3 | `array_type` | service raises UnsupportedResultException on multi-dim array results (binds work) |
| 2 | `supports_bitwise_shift` | every int marshalled as bigint; `int4 << int8` undefined in PG |
| 1 | `infinity_floats` | JSON wire format can't carry IEEE Inf |

## 3. Dialect features not implemented — fixable with work (25)

| Count | What | Notes |
|---|---|---|
| 11+6 | `autocommit`, `isolation_level` | needs isolation-level plumbing over Data API transactions |
| 8 | `default_schema_name_switch` | doubtful: `SET search_path` won't survive stateless calls |

## 4. Remaining tail (23)

| Count | Requirement | Status |
|---|---|---|
| 10 | `datetime_interval` | needs driver PG-interval → timedelta parsing (real work) |
| 8 | `literal_float_coercion` combos (test_insert_w_floats) | requirement opened but combos still skip — gated by an additional per-combo exclusion, uninvestigated |
| 3 | FK option reflection combos | gating requirements already open; combo-level cause uninvestigated |
| 2 | collation reflection | deliberately closed — Aurora collation naming mismatch |

## Resolved 2026-06-10 (were 66 skips, now passing)

`time_timezone` (6 — needed both the tz bind-cast fix and a
`column_expression` text-cast workaround: the Data API rejects TIMETZ
*result columns* outright, so `_ADA_TIME` casts tz-aware time columns to
text in SELECT lists and parses back, wrapped in `type_coerce` so the
type's result processor still runs), `datetime_historic` (6),
`date_historic` (6), `ctes_with_values` (8), `update_from` /
`delete_from` (2), `reflect_tables_no_columns` (5),
`table_value_constructor` (1), `expression_server_defaults` (4), and
async `sane_rowcount` (28 — the async cursor reports rowcount fine; the
`supports_sane_rowcount = False` flag was defensive and wrong).
