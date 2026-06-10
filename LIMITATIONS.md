# Known limitations and behaviors

## Data API response size limit (~1 MiB per statement)

The RDS Data API caps the response of a single `ExecuteStatement` call.
Verified behavior against Aurora PG 16 (2026-06-10):

**The adaptation.** Both drivers (`aurora_data_api` and
`aurora_data_api.async_driver`) catch the service error
(`The result exceeds the size limit` — note: the older message
`Database response exceeded size limit` is no longer what the service
emits) and transparently recover by re-executing the query as
`DECLARE <name> SCROLL CURSOR FOR <sql>`, then paging with
`FETCH <n>`. If an individual page itself exceeds the limit, the driver
`MOVE`s back and halves the page size.

**Inside a transaction this works end-to-end** (verified live: a ~2 MiB
result paginated to completion and the transaction remained usable
afterward). The upstream `test_pagination_backoff` skip-reason claiming
the API "terminates and deletes the transaction" on size-limit
violations is stale — the current service leaves the transaction alive.

Since the SQLAlchemy dialects always run inside a transaction
(`do_begin` on connect + SQLAlchemy autobegin), **all
sqlalchemy-aurora-data-api usage is covered**.

**The hole: raw driver usage outside a transaction.** PG rejects
`DECLARE CURSOR can only be used in transaction blocks` (SQLState
25P01), so the recovery path fails for autocommit-style calls. A fix
would have `_start_paginated_query` begin its own transaction and commit
it when the cursor is exhausted or closed. Not currently implemented.

**Testing gaps.**

- Nothing automated exercises a >1 MiB response. The compliance suites
  use tiny fixtures; `test/test.py::test_pagination_backoff` is
  `@unittest.skip`-ed with the stale reason above and asserts the
  outdated error message.
- The async driver has no pagination tests, and its size-limit retry
  block logs before checking the exception string (ordering swapped
  vs. the sync driver) — harmless but worth cleaning when touched.

Multiple statements per transaction are unaffected — the limit is
per-statement, and the cursor-paging recovery exploits exactly that.
