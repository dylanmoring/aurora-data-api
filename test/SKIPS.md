# Compliance-suite skip inventory

Derived from `pytest -rs` output, 2026-06-10. Applies identically to all
four suites (this repo's `compliance` / `compliance_sync` and the
sqlalchemy-aurora-data-api mirrors). At time of writing: **290 skips
(sync) / 318 (async)**; the async delta is exactly the `sane_rowcount`
block below.

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

## 3. Dialect features not implemented — fixable with work

| Count | What | Notes |
|---|---|---|
| 11+6 | `autocommit`, `isolation_level` | needs isolation-level plumbing over Data API transactions |
| 8 | `default_schema_name_switch` | doubtful: `SET search_path` won't survive stateless calls |
| 28 (async only) | `sane_rowcount` (RowCountTest, SimpleUpdateDeleteTest) | async mixin declared `supports_sane_rowcount = False`; cursor does report rowcount — under test |

## 4. Open-and-test candidates / known-deliberate closures

| Count | Requirement | Status |
|---|---|---|
| 6 | `time_timezone` | opening — tz bind-cast fix should cover timetz |
| 6+6 | `datetime_historic`, `date_historic` | opening — PG handles pre-1900 |
| 8 | `ctes_with_values` | opening — PG supports VALUES in CTE |
| 2 | `update_from` / `delete_from` | opening — PG supports `UPDATE ... FROM` |
| 5 | `reflect_tables_no_columns` | opening — PG allows zero-column tables |
| 1 | `table_value_constructor` | opening — PG supports standalone `VALUES` |
| 4 | `expression_server_defaults` | opening |
| 8 | `literal_float_coercion` | opening — doubleValue is IEEE double, exact round-trip expected |
| 3 | FK option reflection combos | gating requirements already open; combo-level cause uninvestigated |
| 10 | `datetime_interval` | needs driver PG-interval → timedelta parsing (real work) |
| 2 | collation reflection | deliberately closed — Aurora collation naming mismatch |
