"""
Async DB-API-ish driver for Aurora Data API + small sync adapters that SQLAlchemy's
async dialect can hand back to the sync core after awaiting.

Expose:
    - async def connect(...)
    - SyncAdaptedConnection / SyncAdaptedCursor (used by the dialect)
    - standard DB-API module globals: apilevel, threadsafety, paramstyle, etc.
"""

from __future__ import annotations

import os
import datetime
import ipaddress
import uuid
import logging
import reprlib
import itertools
import re
from decimal import Decimal
from collections import namedtuple
from collections.abc import Mapping
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import aioboto3
from sqlalchemy.util.concurrency import await_only  # used by the sync adapters

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
from .mysql_error_codes import MySQLErrorCodes  # noqa: F401  (kept for parity with sync impl)
from .postgresql_error_codes import PostgreSQLErrorCodes  # noqa: F401

# ---------------------------------------------------------------------------
# DB-API module attributes
# ---------------------------------------------------------------------------

apilevel = "2.0"
threadsafety = 0
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

ColumnDescription = namedtuple(
    "ColumnDescription",
    "name type_code display_size internal_size precision scale null_ok"
)
ColumnDescription.__new__.__defaults__ = (None,) * len(ColumnDescription._fields)

logger = logging.getLogger(__name__)

__all__ = [
    # dbapi symbols
    "apilevel",
    "threadsafety",
    "paramstyle",
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimestampFromTicks",
    "Binary",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "DECIMAL",
    # main API
    "connect",
    "AuroraDataAPIClientAsync",
    "AuroraDataAPICursorAsync",
    # sync adapters that the dialect will use
    "SyncAdaptedConnection",
    "SyncAdaptedCursor",
]

def _region_from_arn(arn: str) -> str:
    # arn:partition:service:region:account-id:resource
    return arn.split(":")[3]

# ---------------------------------------------------------------------------
# Async driver
# ---------------------------------------------------------------------------


class AuroraDataAPIClientAsync:
    def __init__(
        self,
        *,
        dbname: Optional[str] = None,
        aurora_cluster_arn: Optional[str] = None,
        secret_arn: Optional[str] = None,
        client=None,
        client_ctx=None,
        charset: Optional[str] = None,
        continue_after_timeout: Optional[bool] = None,
    ):
        self._client = client
        self._client_ctx = client_ctx
        self._dbname = dbname
        self._aurora_cluster_arn = (
            aurora_cluster_arn or os.environ.get("AURORA_CLUSTER_ARN")
        )
        self._secret_arn = secret_arn or os.environ.get("AURORA_SECRET_ARN")
        self._charset = charset
        self._transaction_id = None
        self._continue_after_timeout = continue_after_timeout

    @classmethod
    async def connect(
        cls,
        *,
        aurora_cluster_arn: Optional[str] = None,
        secret_arn: Optional[str] = None,
        region_name: Optional[str] = None,
        database: Optional[str] = None,
        charset: Optional[str] = None,
        continue_after_timeout: Optional[bool] = None,
    ) -> "AuroraDataAPIClientAsync":
        """
        Async DB-API `connect()` entry point.
        """
        # Pick / validate region
        arn_region = _region_from_arn(aurora_cluster_arn or secret_arn)
        if region_name is None:
            region_name = arn_region
        elif region_name != arn_region:
            raise ValueError(
                f"region_name ({region_name}) must match ARN region ({arn_region})"
            )
        session = aioboto3.Session()
        client_ctx = session.client("rds-data", region_name=region_name)
        client = await client_ctx.__aenter__()  # <— IMPORTANT: actually get the client
        return cls(
            dbname=database,
            aurora_cluster_arn=aurora_cluster_arn,
            secret_arn=secret_arn,
            client=client,
            client_ctx=client_ctx,
            charset=charset,
            continue_after_timeout=continue_after_timeout,
        )

    async def close(self):
        # Properly exit the aioboto3 client context
        if self._client_ctx is not None:
            await self._client_ctx.__aexit__(None, None, None)
            self._client_ctx = None
        self._client = None

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

    async def cursor(self) -> "AuroraDataAPICursorAsync":
        if self._transaction_id is None:
            res = await self._client.begin_transaction(
                database=self._dbname,
                resourceArn=self._aurora_cluster_arn,
                secretArn=self._secret_arn,
            )
            self._transaction_id = res["transactionId"]
        cursor = AuroraDataAPICursorAsync(
            client=self._client,
            dbname=self._dbname,
            aurora_cluster_arn=self._aurora_cluster_arn,
            secret_arn=self._secret_arn,
            transaction_id=self._transaction_id,
            continue_after_timeout=self._continue_after_timeout,
        )
        if self._charset:
            await cursor.execute(f"SET character_set_client = '{self._charset}'")
        return cursor

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc:
            await self.rollback()
        else:
            await self.commit()


class AuroraDataAPICursorAsync:
    _pg_type_map = {
        "int": int,
        "int2": int,
        "int4": int,
        "int8": int,
        "float4": float,
        "float8": float,
        "serial2": int,
        "serial4": int,
        "serial8": int,
        "bool": bool,
        "varbit": bytes,
        "bytea": bytearray,
        "char": str,
        "varchar": str,
        "cidr": ipaddress.ip_network,
        "date": datetime.date,
        "inet": ipaddress.ip_address,
        "json": dict,
        "jsonb": dict,
        "money": str,
        "text": str,
        "time": datetime.time,
        "timestamp": datetime.datetime,
        "uuid": uuid.UUID,
        "numeric": Decimal,
        "decimal": Decimal,
    }
    _data_api_type_map = {
        bytes: "blobValue",
        bool: "booleanValue",
        float: "doubleValue",
        int: "longValue",
        str: "stringValue",
        Decimal: "stringValue",
    }
    _data_api_type_hint_map = {
        datetime.date: "DATE",
        datetime.time: "TIME",
        datetime.datetime: "TIMESTAMP",
        Decimal: "DECIMAL",
        uuid.UUID: "UUID",
    }

    def __init__(
        self,
        *,
        client,
        dbname: Optional[str],
        aurora_cluster_arn: str,
        secret_arn: str,
        transaction_id: Optional[str],
        continue_after_timeout: Optional[bool],
    ):
        self.arraysize = 1000
        self.description: Optional[List[ColumnDescription]] = None
        self._client = client
        self._dbname = dbname
        self._aurora_cluster_arn = aurora_cluster_arn
        self._secret_arn = secret_arn
        self._transaction_id = transaction_id
        self._continue_after_timeout = continue_after_timeout
        self._current_response = None
        self._records: List[Tuple[Any, ...]] = []

    # ---------------------------
    # execution helpers
    # ---------------------------

    def prepare_param(self, name, value):
        if value is None:
            return {"name": name, "value": {"isNull": True}}
        t = self._data_api_type_map.get(type(value), "stringValue")
        param = {"name": name, "value": {t: value}}
        if t == "stringValue" and not isinstance(value, str):
            param["value"][t] = str(value)
        hint = self._data_api_type_hint_map.get(type(value))
        if hint:
            param["typeHint"] = hint
        return param

    def _set_description(self, meta):
        self.description = []
        for col in meta:
            self.description.append(
                ColumnDescription(
                    name=col["name"],
                    type_code=self._pg_type_map.get(col["typeName"].lower(), str),
                )
            )

    def _prepare(self, sql, parameters):
        args = {
            "database": self._dbname,
            "resourceArn": self._aurora_cluster_arn,
            "secretArn": self._secret_arn,
            "sql": sql,
            "includeResultMetadata": True,
        }
        if self._transaction_id:
            args["transactionId"] = self._transaction_id
        if self._continue_after_timeout is not None:
            args["continueAfterTimeout"] = self._continue_after_timeout
        if parameters:
            if not isinstance(parameters, Mapping):
                raise NotSupportedError("Expected mapping for parameters")
            args["parameters"] = [self.prepare_param(k, v) for k, v in parameters.items()]
        return args

    async def execute(self, operation, parameters=None):
        self._current_response = None
        self._records = []
        args = self._prepare(operation, parameters)
        logger.debug("execute %s", reprlib.repr(operation.strip()))
        try:
            res = await self._client.execute_statement(**args)
            if "columnMetadata" in res:
                self._set_description(res["columnMetadata"])
            rendered = self._render(res)
            self._records = rendered.get("records", [])
            self._current_response = rendered
        except (self._client.exceptions.BadRequestException, self._client.exceptions.DatabaseErrorException) as e:
            msg = str(e)
            if "Please paginate your query" in msg:
                raise NotSupportedError("Cursor pagination is not supported in async driver") from e
            else:
                raise self._get_error(e) from e

    async def executemany(self, operation, seq: Iterable[Mapping[str, Any]]):
        args = self._prepare(operation, None)
        args.pop('includeResultMetadata', None)  # no metadata for batch execute
        for batch in itertools.zip_longest(*[iter(seq)] * self.arraysize, fillvalue=None):
            params_batch = [p for p in batch if p is not None]
            args_batch = args.copy()
            args_batch["parameterSets"] = [[self.prepare_param(k, v) for k, v in p.items()] for p in params_batch]
            try:
                await self._client.batch_execute_statement(**args_batch)
            except self._client.exceptions.BadRequestException as e:
                raise self._get_error(e) from e

    # ---------------------------
    # fetch helpers
    # ---------------------------

    def _render(self, res):
        if "records" in res:
            for i, record in enumerate(res["records"]):
                res["records"][i] = tuple(
                    self._render_val(value, self.description[j] if self.description else None)
                    for j, value in enumerate(record)
                )
        return res

    def _render_val(self, val, desc=None):
        if val.get("isNull"):
            return None
        if "arrayValue" in val:
            arr = val["arrayValue"]
            if "arrayValues" in arr:
                return [self._render_val(x) for x in arr["arrayValues"]]
            return list(arr.values())[0]
        scalar = list(val.values())[0]
        if desc and desc.type_code in self._data_api_type_hint_map:
            if desc.type_code == Decimal:
                return Decimal(scalar)
            try:
                return desc.type_code.fromisoformat(scalar)
            except Exception:
                return desc.type_code(scalar)
        return scalar

    async def fetchone(self):
        return self._records.pop(0) if self._records else None

    async def fetchmany(self, size=None):
        size = size or self.arraysize
        results = []
        for _ in range(size):
            row = await self.fetchone()
            if row is None:
                break
            results.append(row)
        return results

    async def fetchall(self):
        all_rows = self._records
        self._records = []
        return all_rows

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    def _get_error(self, e):
        msg = getattr(e, 'response', {}).get('Error', {}).get('Message', '')
        # TODO: map AWS messages into MySQLError/PostgreSQLError specific exceptions
        return DatabaseError(e)


async def connect(
    *,
    aurora_cluster_arn: Optional[str] = None,
    secret_arn: Optional[str] = None,
    region_name: Optional[str] = None,
    database: Optional[str] = None,
    charset: Optional[str] = None,
    continue_after_timeout: Optional[bool] = None,
) -> AuroraDataAPIClientAsync:
    """
    SQLAlchemy dialect will call this (and await it via await_only())
    in its connect() / do_connect() override.
    """
    return await AuroraDataAPIClientAsync.connect(
        aurora_cluster_arn=aurora_cluster_arn,
        secret_arn=secret_arn,
        region_name=region_name,
        database=database,
        charset=charset,
        continue_after_timeout=continue_after_timeout,
    )

# ---------------------------------------------------------------------------
# Synchronous adapters for SQLAlchemy core (used *by the dialect*)
# ---------------------------------------------------------------------------


class SyncAdaptedConnection:
    """
    Synchronous facade over AuroraDataAPIClientAsync. Meant to be returned by the dialect's
    connect() after it awaited our async connect() with await_only().
    """

    def __init__(self, async_conn: AuroraDataAPIClientAsync):
        self._async_conn = async_conn

    # SQLAlchemy expects these to be synchronous methods

    def cursor(self) -> "SyncAdaptedCursor":
        async def _cursor():
            return await self._async_conn.cursor()
        acur = await_only(_cursor())
        return SyncAdaptedCursor(acur)

    def commit(self):
        return await_only(self._async_conn.commit())

    def rollback(self):
        return await_only(self._async_conn.rollback())

    def close(self):
        return await_only(self._async_conn.close())

    # convenience for SA which sometimes asks for .connection (esp. wrappers)
    @property
    def connection(self):
        return self

    # helpful debug
    def __repr__(self):
        return f"<SyncAdaptedConnection async={self._async_conn!r}>"


class SyncAdaptedCursor:
    def __init__(self, async_cursor: AuroraDataAPICursorAsync):
        self._cursor = async_cursor
        self.arraysize = async_cursor.arraysize
        self.description = async_cursor.description
        self._rowcount = -1
        self._buffer = []
        self._closed = False

    def __iter__(self):
        # allow plain `for row in result:` after execute()
        return iter(self.fetchall())

    def execute(self, operation, parameters=None):
        # run the statement (await inside greenlet)
        await_only(self._cursor.execute(operation, parameters))
        self.description = self._cursor.description

        # **prefetch all rows while still in the greenlet**
        self._buffer = await_only(self._cursor.fetchall())
        self._rowcount = len(self._buffer)
        return self

    def executemany(self, operation, param_sets):
        await_only(self._cursor.executemany(operation, param_sets))
        self.description = None
        self._rowcount = -1
        self._buffer = []
        return self

    def fetchone(self):
        return self._buffer.pop(0) if self._buffer else None

    def fetchmany(self, size=None):
        size = size or self.arraysize
        res, self._buffer = self._buffer[:size], self._buffer[size:]
        return res

    def fetchall(self):
        res, self._buffer = self._buffer, []
        return res

    def close(self):
        # Data API cursors are stateless; nothing to actually close.
        self._closed = True

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def closed(self):
        return self._closed
