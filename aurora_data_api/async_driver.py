"""
aurora-data-api-async - Async variant of the AWS Aurora Serverless Data API client.

Design:
- Reuse as much logic as possible by subclassing the sync AuroraDataAPICursor.
- Only override methods that touch the network or iteration.
- Manage the aioboto3 client with an async context manager (__aenter__/__aexit__).
"""

from __future__ import annotations

import os
import time
import random
import string
import logging
import reprlib

import itertools
import re
import uuid
import ipaddress
import datetime
from decimal import Decimal
from collections.abc import Mapping
from typing import Optional, List, Any

import aioboto3

from sqlalchemy.util.concurrency import await_only

# Reuse exceptions and utilities from the sync module
from .exceptions import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
    MySQLError,
    PostgreSQLError,
)

# We subclass the sync cursor to reuse: type maps, parameter formatting,
# response rendering, column description, etc.
from aurora_data_api import AuroraDataAPICursor as _SyncCursor, ColumnDescription

logger = logging.getLogger(__name__)
logging.getLogger('aiobotocore.credentials').setLevel(logging.WARNING)

apilevel = "2.0"
threadsafety = 0  # DB-API meaning; async implies no cross-thread use of the same connection
paramstyle = "named"

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = datetime.date.fromtimestamp
TimestampFromTicks = datetime.datetime.fromtimestamp
Binary = bytes
STRING = str
BINARY = bytes
NUMBER = float
DATETIME = datetime.datetime
ROWID = str
DECIMAL = Decimal


class AsyncAuroraDataAPICursor(_SyncCursor):
    """
    Async version of the cursor. Inherits all the "pure" helpers from the sync cursor:
    - prepare_param
    - _set_description
    - _render_value
    - _render_response
    - _format_parameter_set
    - _get_database_error
    And we override only the methods that actually perform I/O or iteration.
    """

    def __init__(
        self,
        client=None,
        dbname=None,
        aurora_cluster_arn=None,
        secret_arn=None,
        transaction_id=None,
        continue_after_timeout=None,
    ):
        super().__init__(
            client=client,
            dbname=dbname,
            aurora_cluster_arn=aurora_cluster_arn,
            secret_arn=secret_arn,
            transaction_id=transaction_id,
            continue_after_timeout=continue_after_timeout,
        )
        # Async iteration state
        self._buffer: List[Any] | None = None
        self._buffer_idx: int = 0

    async def _start_paginated_query(self, execute_statement_args, records_per_page=None):
        # Mirrors sync version but awaits I/O
        pg_cursor_name = "{}_{}_{}".format(
            __name__, int(time.time()), "".join(random.choices(string.ascii_letters + string.digits, k=8))
        )
        cursor_stmt = "DECLARE " + pg_cursor_name + " SCROLL CURSOR FOR "
        execute_statement_args = dict(execute_statement_args)  # copy
        execute_statement_args["sql"] = cursor_stmt + execute_statement_args["sql"]

        await self._client.execute_statement(**execute_statement_args)
        self._paging_state = {
            "execute_statement_args": dict(execute_statement_args),
            "records_per_page": records_per_page or self.arraysize,
            "pg_cursor_name": pg_cursor_name,
        }

    async def execute(self, operation, parameters=None):
        # Reset per-exec state
        self._current_response, self._iterator, self._paging_state = None, None, None
        self._buffer, self._buffer_idx = None, 0

        execute_statement_args = dict(self._prepare_execute_args(operation), includeResultMetadata=True)
        if self._continue_after_timeout is not None:
            execute_statement_args["continueAfterTimeout"] = self._continue_after_timeout
        if parameters:
            execute_statement_args["parameters"] = self._format_parameter_set(parameters)

        logger.debug("execute %s", reprlib.repr(operation.strip()))
        try:
            res = await self._client.execute_statement(**execute_statement_args)
            if "columnMetadata" in res:
                self._set_description(res["columnMetadata"])
            self._current_response = self._render_response(res)
            # Preload buffer for non-paginated responses
            self._buffer = list(self._current_response.get("records", []))
            self._buffer_idx = 0
        except (self._client.exceptions.BadRequestException, self._client.exceptions.DatabaseErrorException) as e:
            msg = str(e)
            if "Please paginate your query" in msg:
                await self._start_paginated_query(execute_statement_args)
            elif "Database returned more than the allowed response size limit" in msg:
                await self._start_paginated_query(
                    execute_statement_args, records_per_page=max(1, self.arraysize // 2)
                )
            else:
                raise self._get_database_error(e) from e

        # For non-paginated case, emulate the sync driver’s iteration contract:
        # fetch* APIs will read from _buffer; async iteration is also supported.

    async def executemany(self, operation, seq_of_parameters):
        logger.debug("executemany %s", reprlib.repr(operation.strip()))
        for batch in self._page_input(seq_of_parameters):
            batch_args = dict(
                self._prepare_execute_args(operation),
                parameterSets=[self._format_parameter_set(p) for p in batch],
            )
            try:
                await self._client.batch_execute_statement(**batch_args)
            except self._client.exceptions.BadRequestException as e:
                raise self._get_database_error(e) from e

    async def scroll(self, value, mode="relative"):
        if not self._paging_state:
            raise InterfaceError("Cursor scroll attempted but pagination is not active")
        scroll_stmt = "MOVE {mode} {value} FROM {pg_cursor_name}".format(
            mode=mode.upper(), value=value, **self._paging_state
        )
        scroll_args = dict(self._paging_state["execute_statement_args"], sql=scroll_stmt)
        logger.debug("Scrolling cursor %s by %d rows", mode, value)
        await self._client.execute_statement(**scroll_args)
        # Invalidate buffer since we changed position
        self._buffer, self._buffer_idx = None, 0

    # ----- async iteration & fetch APIs -----

    def __aiter__(self):
        return self

    async def __anext__(self):
        # Buffered (non-paginated) path
        if not self._paging_state:
            if not self._buffer:
                raise StopAsyncIteration
            if self._buffer_idx >= len(self._buffer):
                raise StopAsyncIteration
            row = self._buffer[self._buffer_idx]
            self._buffer_idx += 1
            return row

        # Paginated path
        while True:
            # Load next page into buffer if needed
            if not self._buffer or self._buffer_idx >= len(self._buffer):
                next_page_args = dict(self._paging_state["execute_statement_args"])
                next_page_args["sql"] = "FETCH {records_per_page} FROM {pg_cursor_name}".format(
                    **self._paging_state
                )
                try:
                    page = await self._client.execute_statement(**next_page_args)
                except self._client.exceptions.BadRequestException as e:
                    cur_rpp = self._paging_state["records_per_page"]
                    msg = str(e)
                    if "Database returned more than the allowed response size limit" in msg and cur_rpp > 1:
                        # Rewind and halve page size, then retry
                        await self.scroll(-self._paging_state["records_per_page"])
                        logger.debug("Halving records per page")
                        self._paging_state["records_per_page"] //= 2
                        continue
                    raise self._get_database_error(e) from e

                if "columnMetadata" in page and not self.description:
                    self._set_description(page["columnMetadata"])

                # No more rows
                if not page.get("records"):
                    raise StopAsyncIteration

                page = self._render_response(page)
                self._buffer = list(page["records"])
                self._buffer_idx = 0

            # Yield next buffered row
            row = self._buffer[self._buffer_idx]
            self._buffer_idx += 1
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
        # Nothing to close at the Data API cursor level; keep parity with sync
        self._iterator = None
        self._current_response = None
        self._buffer, self._buffer_idx = None, 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, err_type, value, traceback):
        await self.close()


class AsyncAuroraDataAPIClient:
    """
    Async connection façade that mirrors the sync client but:
    - Manages an aioboto3 client context (you must use `async with` or call `await close()`).
    - Begins a transaction on first `await cursor()`, like the sync client.
    - Commits on __aexit__ if no exception, otherwise rolls back.

    You can inject an existing aioboto3 client via `rds_data_client=` to control lifecycle yourself.
    """

    def __init__(
        self,
        dbname: Optional[str] = None,
        aurora_cluster_arn: Optional[str] = None,
        secret_arn: Optional[str] = None,
        rds_data_client=None,
        charset: Optional[str] = None,
        continue_after_timeout: Optional[bool] = None,
        *,
        session: Optional[aioboto3.Session] = None,
        region_name: Optional[str] = None,
    ):
        self._session = session
        self._region_name = region_name
        self._client_ctx = None
        self._client = rds_data_client  # if provided, we won't create/close
        self._dbname = dbname
        self._aurora_cluster_arn = aurora_cluster_arn or os.environ.get("AURORA_CLUSTER_ARN")
        self._secret_arn = secret_arn or os.environ.get("AURORA_SECRET_ARN")
        self._charset = charset
        self._transaction_id: Optional[str] = None
        self._continue_after_timeout = continue_after_timeout

    async def connect(self):
        session = self._session or aioboto3.Session()
        # Important: aioboto3 returns an async context manager for the client
        self._client_ctx = session.client("rds-data", region_name=self._region_name)
        self._client = await self._client_ctx.__aenter__()

    async def close(self):
        if self._client_ctx is not None:
            # Make sure the underlying HTTP resources are released
            await self._client_ctx.__aexit__(None, None, None)
            self._client_ctx = None
            self._client = None

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

    async def commit(self):
        if self._transaction_id:
            res = await self._client.commit_transaction(
                resourceArn=self._aurora_cluster_arn,
                secretArn=self._secret_arn,
                transactionId=self._transaction_id,
            )
            self._transaction_id = None
            if res.get("transactionStatus") != "Transaction Committed":
                raise DatabaseError(f"Error while committing transaction: {res}")

    async def rollback(self):
        if self._transaction_id:
            await self._client.rollback_transaction(
                resourceArn=self._aurora_cluster_arn,
                secretArn=self._secret_arn,
                transactionId=self._transaction_id,
            )
            self._transaction_id = None

    # ----- cursor creation -----

    async def cursor(self) -> AsyncAuroraDataAPICursor:
        # Begin an explicit transaction on first cursor(), same as sync client
        if self._transaction_id is None:
            res = await self._client.begin_transaction(
                database=self._dbname,
                resourceArn=self._aurora_cluster_arn,
                # schema="string",  # TODO if needed
                secretArn=self._secret_arn,
            )
            self._transaction_id = res["transactionId"]

        cursor = AsyncAuroraDataAPICursor(
            client=self._client,
            dbname=self._dbname,
            aurora_cluster_arn=self._aurora_cluster_arn,
            secret_arn=self._secret_arn,
            transaction_id=self._transaction_id,
            continue_after_timeout=self._continue_after_timeout,
        )
        if self._charset:
            await cursor.execute("SET character_set_client = '{}'".format(self._charset))
        return cursor


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
        # return a sync-looking cursor
        acur = await_only(self._async.cursor())
        return SyncAdaptedCursor(acur)

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
    def __init__(self, async_cursor):
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

    # pass-through to support your new pagination/scroll feature
    def scroll(self, value, mode="relative"):
        return await_only(self._cur.scroll(value, mode))

    def close(self):
        await_only(self._cur.close())

    @property
    def rowcount(self):
        return self._cur.rowcount
