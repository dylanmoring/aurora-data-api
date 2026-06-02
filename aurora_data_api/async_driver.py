"""
aurora-data-api-async - Async variant of the AWS Aurora Serverless Data API client.


Design goals (parallel to sync driver):
- No inheritance from the sync driver; share only pure helpers.
- Connection exposes: start_transaction(), commit(), rollback(), _prepare_execute_args(), cursor().
- Cursor keeps ONLY cursor-local state; all I/O goes through the connection.
- No implicit/autotransaction. SQLAlchemy async dialect should call start_transaction() via do_begin().
- aioboto3 lifecycle managed with async context manager.


This module is designed to work with an async SQLAlchemy dialect that adapts
this async client to a sync-looking facade via await_only (see SyncAdaptedConnection below).
"""
from __future__ import annotations


import os
import time
import random
import string
import asyncio
import logging
import itertools
import reprlib
from typing import Optional, Any, List


import aioboto3


from sqlalchemy.util.concurrency import await_only


# Shared helpers from sync package
from .type_conversion import build_description, format_parameters, convert_value
from .exceptions import *
from .retry import retry_exceptions


logger = logging.getLogger(__name__)


apilevel = "2.0"
threadsafety = 0 # DB-API meaning; async implies no cross-thread use of the same connection
paramstyle = "named"

class AsyncAuroraDataAPIClient:
    """
    Async connection façade that mirrors the sync client but:
    - Manages an aioboto3 client context (you must use `async with` or call `await close()`).

    You can inject an existing aioboto3 client via `rds_data_client=` to control lifecycle yourself.
    """

    def __init__(
        self,
        dbname: Optional[str] = None,
        aurora_cluster_arn: Optional[str] = None,
        secret_arn: Optional[str] = None,
        rds_data_client=None,  # allow injection of a pre-created aiobotocore client
        charset: Optional[str] = None,
        continue_after_timeout: Optional[bool] = None,
        *,
        session: Optional[aioboto3.Session] = None,
        region_name: Optional[str] = None,
    ):
        self._session = session
        self._region_name = region_name
        self._client_ctx = None  # context manager from aioboto3
        self.client = rds_data_client  # async boto client (once connected)

        self._dbname = dbname
        self._aurora_cluster_arn = aurora_cluster_arn or os.environ.get("AURORA_CLUSTER_ARN")
        self._secret_arn = secret_arn or os.environ.get("AURORA_SECRET_ARN")
        self._charset = charset
        self._continue_after_timeout = continue_after_timeout
        self._transaction_id: Optional[str] = None

    async def connect(self):
        if self.client is not None:
            return  # external client injected
        session = self._session or aioboto3.Session()
        self._client_ctx = session.client("rds-data", region_name=self._region_name)
        self.client = await self._client_ctx.__aenter__()

    async def close(self):
        if self._client_ctx is not None:
            await self._client_ctx.__aexit__(None, None, None)
            self._client_ctx = None
            self.client = None

    # ----- context management & lifecycle -----
    # This is not used natively by sqlalchemy, but allows using this client
    # in an async context manager, which is useful for manual transaction control.

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, err_type, value, traceback):
        # Mirror sync semantics: rollback on error, else commit
        try:
            if err_type is not None:
                await self.rollback()
            else:
                await self.commit()
        finally:
            await self.close()

    # ----- transaction control -----

    @retry_exceptions(4, 2, 2, 4, exceptions="DatabaseResumingException")
    async def start_transaction(self):
        if self._transaction_id is not None:
            return self._transaction_id
        try:
            res = await self.client.begin_transaction(
                database=self._dbname,
                resourceArn=self._aurora_cluster_arn,
                secretArn=self._secret_arn,
            )
        except (self.client.exceptions.BadRequestException,
                self.client.exceptions.DatabaseErrorException) as e:
            raise translate_database_error(e) from e
        except Exception as e:
            raise DatabaseError(e) from e
        self._transaction_id = res["transactionId"]
        logger.info(f"Started transaction {self._transaction_id}")
        return self._transaction_id

    async def commit(self):
        if self._transaction_id:
            res = await self.client.commit_transaction(
                resourceArn=self._aurora_cluster_arn,
                secretArn=self._secret_arn,
                transactionId=self._transaction_id,
            )
            if res.get("transactionStatus") != "Transaction Committed":
                raise DatabaseError(f"Error while committing transaction: {res}")
            logger.info(f"Committed transaction {self._transaction_id}")
            self._transaction_id = None

    async def rollback(self):
        if not self._transaction_id:
            return
        tx_id = self._transaction_id
        self._transaction_id = None
        # Schedule as a detached task and shield the await: if the parent task
        # is being cancelled (e.g. Lambda imminent-timeout), the outer await
        # raises CancelledError but the inner HTTPS request keeps running on
        # the loop and can still land server-side before the loop tears down.
        rollback_task = asyncio.create_task(
            self.client.rollback_transaction(
                resourceArn=self._aurora_cluster_arn,
                secretArn=self._secret_arn,
                transactionId=tx_id,
            )
        )
        try:
            await asyncio.shield(rollback_task)
            logger.info(f"Rolled back transaction {tx_id}")
        except asyncio.CancelledError:
            logger.warning(
                f"Rollback of {tx_id} interrupted by outer cancellation; "
                f"shielded request continues until event loop teardown"
            )
            raise

    # ----- cursor creation -----

    def _prepare_execute_args(self, operation: str) -> dict:
        args = dict(
            database=self._dbname,
            resourceArn=self._aurora_cluster_arn,
            secretArn=self._secret_arn,
            sql=operation,
        )
        if self._transaction_id:
            args["transactionId"] = self._transaction_id
        return args

    # ---- cursor factory ----
    async def cursor(self) -> "AsyncAuroraDataAPICursor":
        cur = AsyncAuroraDataAPICursor(connection=self)
        if self._charset:
            await cur.execute("SET character_set_client = '{}'".format(self._charset))
        return cur


class AsyncAuroraDataAPICursor:
    def __init__(self, connection: AsyncAuroraDataAPIClient, arraysize: int = 1000):
        # Cursor-local state only
        self.arraysize = arraysize
        self.description = None
        self._current_response = None
        self._paging_state = None
        self._connection = connection
        # Simple buffer for non-paginated results and for page materialization
        self._buffer: Optional[List[Any]] = None
        self._buffer_idx: int = 0

    def _set_description(self, column_metadata):
        self.description = build_description(column_metadata)

    def _render_response(self, response):
        if "records" in response:
            for i, record in enumerate(response["records"]):
                response["records"][i] = tuple(
                    convert_value(value, col_desc=self.description[j] if self.description else None)
                    for j, value in enumerate(record)
                )

        return response

    async def _start_paginated_query(self, execute_statement_args, records_per_page=None):
        pg_cursor_name = '"{}_{}_{}"'.format(
            __name__, int(time.time()), "".join(random.choices(string.ascii_letters + string.digits, k=8))
        )
        cursor_stmt = "DECLARE " + pg_cursor_name + " SCROLL CURSOR FOR "
        execute_statement_args = dict(execute_statement_args)  # copy
        execute_statement_args["sql"] = cursor_stmt + execute_statement_args["sql"]
        logger.debug(f'Starting paginated query with cursor "{pg_cursor_name}"', extra = execute_statement_args)

        await self._connection.client.execute_statement(**execute_statement_args)
        self._paging_state = {
            "execute_statement_args": dict(execute_statement_args),
            "records_per_page": records_per_page or self.arraysize,
            "pg_cursor_name": pg_cursor_name,
        }
        # reset buffer for paged mode
        self._buffer, self._buffer_idx = None, 0
        # Fetch the first page into buffer
        await self._fetch_next_page_into_buffer()

    @retry_exceptions(4, 2, 2, 4, exceptions="DatabaseResumingException")
    async def execute(self, operation, parameters=None):
        # Reset per-exec state
        self._current_response, self._paging_state = None, None
        self._buffer, self._buffer_idx = None, 0

        execute_statement_args: dict = dict(
            self._connection._prepare_execute_args(operation),
            includeResultMetadata=True,
        )
        if self._connection._continue_after_timeout is not None:
            execute_statement_args["continueAfterTimeout"] = self._connection._continue_after_timeout
        if parameters:
            execute_statement_args["parameters"] = format_parameters(parameters)

        logger.debug("execute %s", reprlib.repr(operation.strip()))
        try:
            res = await self._connection.client.execute_statement(**execute_statement_args)
            if "columnMetadata" in res and not self.description:
                self._set_description(res["columnMetadata"])
            self._current_response = self._render_response(res)
            # Preload buffer for non-paginated responses
            self._buffer = list(self._current_response.get("records", []))
            self._buffer_idx = 0
        except (self._connection.client.exceptions.BadRequestException,
                self._connection.client.exceptions.DatabaseErrorException) as e:
            raise translate_database_error(e) from e
        except self._connection.client.exceptions.UnsupportedResultException as e:
            if "The result exceeds the size limit" in str(e):
                logger.info(
                    f'Switching to paginated query for "{operation.strip()[:30]}..."',
                    extra=dict(query=operation.strip()[:2000])
                )
                try:
                    await self._start_paginated_query(execute_statement_args)
                except self._connection.client.exceptions.UnsupportedResultException as e:
                    logger.info(f'Retrying paginated query with smaller pages "{operation.strip()[:30]}..."')
                    if "The result exceeds the size limit" in str(e):
                        await self._start_paginated_query(
                            execute_statement_args, records_per_page=max(1, self.arraysize // 2))
            else:
                raise e

        # For non-paginated case, emulate the sync driver’s iteration contract:
        # fetch* APIs will read from _buffer; async iteration is also supported.

    @retry_exceptions(4, 2, 2, 4, exceptions="DatabaseResumingException")
    async def executemany(self, operation, seq_of_parameters):
        logger.debug("executemany %s", reprlib.repr(operation.strip()))
        for batch in self._page_input(seq_of_parameters, page_size=self.arraysize):
            batch_args = dict(
                self._connection._prepare_execute_args(operation),
                parameterSets=[format_parameters(p) for p in batch],
            )
            try:
                await self._connection.client.batch_execute_statement(**batch_args)
            except self._connection.client.exceptions.BadRequestException as e:
                raise translate_database_error(e) from e

    async def scroll(self, value, mode="relative"):
        if not self._paging_state:
            raise InterfaceError("Cursor scroll attempted but pagination is not active")
        scroll_stmt = "MOVE {mode} {value} FROM {pg_cursor_name}".format(
            mode=mode.upper(), value=value, **self._paging_state
        )
        scroll_args = dict(self._paging_state["execute_statement_args"], sql=scroll_stmt)
        logger.debug("Scrolling cursor %s by %d rows", mode, value)
        await self._connection.client.execute_statement(**scroll_args)
        # Changing position invalidates any buffered rows
        self._buffer, self._buffer_idx = None, 0

    # ----- async iteration & fetch APIs -----

    def __aiter__(self):
        return self

    def _has_buffered_row(self) -> bool:
        return bool(self._buffer) and self._buffer_idx < len(self._buffer)

    def _pop_buffered_row(self) -> tuple[Any, ...] | None:
        if not self._has_buffered_row():
            return None
        row = self._buffer[self._buffer_idx]
        self._buffer_idx += 1
        return row

    async def _fetch_next_page_into_buffer(self) -> None:
        """Fetches the next page into _buffer. Sets description if not set yet.
        On oversize, halves page size and retries."""
        if not self._paging_state:
            raise InterfaceError("Paging state missing while fetching next page")

        while True:
            next_page_args = dict(self._paging_state["execute_statement_args"])
            rpp = self._paging_state["records_per_page"]
            next_page_args["sql"] = f'FETCH {rpp} FROM {self._paging_state["pg_cursor_name"]}'

            try:
                page = await self._connection.client.execute_statement(**next_page_args)
            except self._connection.client.exceptions.UnsupportedResultException as e:
                # 1 MiB response limit. Try smaller pages.
                if "The result exceeds the size limit" in str(e) and rpp > 1:
                    await self.scroll(-rpp, mode="relative")
                    logger.debug("Halving records per page")
                    self._paging_state["records_per_page"] = max(1, rpp // 2)
                    continue
                raise
            except (self._connection.client.exceptions.BadRequestException,
                    self._connection.client.exceptions.DatabaseErrorException) as e:
                raise translate_database_error(e) from e

            if page.get("columnMetadata") and not self.description:
                self._set_description(page["columnMetadata"])

            page = self._render_response(page)
            self._buffer = list(page.get("records", []))  # materialize rows for fetch APIs
            self._buffer_idx = 0
            return


    async def __anext__(self):
        # Non-paginated path uses preloaded buffer
        if not self._paging_state:
            row = self._pop_buffered_row()
            if row is None:
                raise StopAsyncIteration
            return row

        # Paginated path: fetch pages lazily
        row = self._pop_buffered_row()
        if row is not None:
            return row

        # Need a new page
        await self._fetch_next_page_into_buffer()
        row = self._pop_buffered_row()
        if row is None:
            # no rows even after fetch => done
            raise StopAsyncIteration
        return row

    async def fetchone(self):
        try:
            return await self.__anext__()
        except StopAsyncIteration:
            return None

    async def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        out = []
        while size > 0:
            row = await self.fetchone()
            if row is None:
                break
            out.append(row)
            size -= 1
        return out

    async def fetchall(self):
        rows = []
        while True:
            row = await self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    async def close(self):
        self._current_response = None
        self._paging_state = None
        self._buffer, self._buffer_idx = None, 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, err_type, value, traceback):
        await self.close()

    @property
    def rowcount(self):
        if self._current_response:
            if "records" in self._current_response:
                return len(self._current_response["records"])  # non-paginated only
            elif "numberOfRecordsUpdated" in self._current_response:
                return self._current_response["numberOfRecordsUpdated"]
        return -1

    @property
    def lastrowid(self):
        if self._current_response and self._current_response.get("generatedFields"):
            return convert_value(self._current_response["generatedFields"][-1])
        return None

    def _page_input(self, iterable, page_size: int | None = None):
        page_size = page_size or self.arraysize
        iterable = iter(iterable)
        return iter(lambda: list(itertools.islice(iterable, page_size)), [])





def _region_from_arn(arn: str) -> str:
    return arn.split(":")[3]

async def connect(
    *,
    aurora_cluster_arn=None,
    secret_arn=None,
    region_name=None,
    database=None,
    charset=None,
    continue_after_timeout=None,
):
    region_name = region_name or _region_from_arn(aurora_cluster_arn or secret_arn)
    connection = AsyncAuroraDataAPIClient(
        dbname=database,
        aurora_cluster_arn=aurora_cluster_arn,
        secret_arn=secret_arn,
        charset=charset,
        continue_after_timeout=continue_after_timeout,
        region_name=region_name,
    )
    await connection.connect()
    return connection


class SyncAdaptedConnection:
    """Synchronous facade over AsyncAuroraDataAPIClient (for SQLAlchemy core)."""
    def __init__(self, async_conn: AsyncAuroraDataAPIClient):
        self._async = async_conn

    def cursor(self):
        acur = await_only(self._async.cursor())
        return SyncAdaptedCursor(acur)

    # Transaction hooks invoked by SA dialect do_* methods
    def start_transaction(self):
        return await_only(self._async.start_transaction())

    def commit(self):
        await_only(self._async.commit())

    def rollback(self):
        await_only(self._async.rollback())

    def close(self):
        await_only(self._async.close())

    @property
    def connection(self):  # some SA code accesses .connection
        return self

class SyncAdaptedCursor:
    """Synchronous DB-API-ish cursor that forwards to the async cursor."""
    def __init__(self, async_cursor: AsyncAuroraDataAPICursor):
        self._cur = async_cursor
        self.arraysize = async_cursor.arraysize
        self.description = async_cursor.description

    def execute(self, operation, parameters=None):
        await_only(self._cur.execute(operation, parameters))
        self.description = self._cur.description
        return self

    def executemany(self, operation, seq_of_parameters):
        await_only(self._cur.executemany(operation, seq_of_parameters))
        self.description = None
        return self

    def fetchone(self):
        return await_only(self._cur.fetchone())

    def fetchmany(self, size=None):
        return await_only(self._cur.fetchmany(size))

    def fetchall(self):
        return await_only(self._cur.fetchall())

    def scroll(self, value, mode="relative"):
        return await_only(self._cur.scroll(value, mode))

    def close(self):
        await_only(self._cur.close())

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def lastrowid(self):
        return self._cur.lastrowid
