"""
aurora-data-api - A Python DB-API 2.0 client for the AWS Aurora Serverless Data API
"""
import os, time, random, string, logging, itertools, reprlib, threading

import boto3

from .type_conversion import build_description, format_parameters, convert_value
from .exceptions import *
from .retry import retry_exceptions

logger = logging.getLogger(__name__)

apilevel = "2.0"

threadsafety = 0

paramstyle = "named"


# DBAPI 2.0 type constructors. SQLAlchemy's PG dialect calls ``dbapi.Binary``
# when binding ``LargeBinary`` values and probes other DBAPI type attrs
# during bind processing — without these at module scope, the bind
# processor raises ``AttributeError`` on the module before any query runs.
# (The async driver imports this same set from here — see async_driver.py.)
Binary = bytes
import datetime as _dt
Date = _dt.date
Time = _dt.time
Timestamp = _dt.datetime
DateFromTicks = _dt.date.fromtimestamp
def TimeFromTicks(ticks):
    return _dt.datetime.fromtimestamp(ticks).time()
TimestampFromTicks = _dt.datetime.fromtimestamp
STRING = str
BINARY = bytes
NUMBER = (int, float)
DATETIME = _dt.datetime
ROWID = int
# NB: keep ``_dt`` bound — ``TimeFromTicks`` references it at call time.


import re as _re
# Leading whitespace, optional ``(`` or ``WITH ... AS (...)`` prefix, then the
# first keyword token. Handles ``(SELECT ...) UNION ...``,
# ``WITH cte AS (...) SELECT ...``, and ordinary ``SELECT ...``.
_LEADING_KEYWORD_RE = _re.compile(
    r"^\s*(?:\(|WITH\b.*?\)\s*)*(\w+)", _re.IGNORECASE | _re.DOTALL
)
_RETURNING_RE = _re.compile(r"\bRETURNING\b", _re.IGNORECASE)
_ROW_RETURNING_KEYWORDS = frozenset({
    "SELECT", "VALUES", "SHOW", "EXPLAIN", "FETCH", "TABLE",
})
_DML_KEYWORDS = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE"})


def _statement_returns_rows(sql: str) -> bool:
    """Return True if ``sql`` is expected to yield a result set.

    Data API echoes ``columnMetadata`` for many INSERT/UPDATE/DELETE
    statements that have no result set — driving ``cursor.description``
    off it makes ``CursorResult.returns_rows`` lie. Use this as a
    structural check: SELECT/VALUES/SHOW/EXPLAIN/FETCH/TABLE always
    return rows; INSERT/UPDATE/DELETE/MERGE return rows only when a
    RETURNING clause is present.
    """
    if not sql:
        return False
    match = _LEADING_KEYWORD_RE.match(sql)
    if not match:
        return False
    head = match.group(1).upper()
    if head in _ROW_RETURNING_KEYWORDS:
        return True
    if head in _DML_KEYWORDS:
        return bool(_RETURNING_RE.search(sql))
    return False


def _region_from_arn(arn: str) -> str:
    """Extract the AWS region segment from a cluster or secret ARN. Mirrors
    the helper on the async driver so both paths derive region the same way."""
    return arn.split(":")[3]


class AuroraDataAPIClient:
    _client_init_lock = threading.Lock()

    def __init__(
        self,
        dbname=None,
        aurora_cluster_arn=None,
        secret_arn=None,
        rds_data_client=None,
        charset=None,
        continue_after_timeout=None,
        region_name=None,
    ):
        # Resolve ARNs first so we can derive region from them if caller
        # didn't pass one explicitly.
        self._aurora_cluster_arn = aurora_cluster_arn or os.environ.get("AURORA_CLUSTER_ARN")
        self._secret_arn = secret_arn or os.environ.get("AURORA_SECRET_ARN")

        # AWS client. If the caller didn't inject one, build it with the
        # region pulled from the cluster (or secret) ARN — boto3's default
        # region resolution can otherwise pick a region that doesn't match
        # the resourceArn we're about to use, producing the "Invalid region
        # in ARN" ValidationException at first ExecuteStatement. The async
        # driver mirrors this trick in its module-level connect().
        self.client = rds_data_client
        if self.client is None:
            with self._client_init_lock:
                arn_for_region = self._aurora_cluster_arn or self._secret_arn
                client_region = region_name or (
                    _region_from_arn(arn_for_region) if arn_for_region else None
                )
                self.client = boto3.client("rds-data", region_name=client_region)

        self._dbname = dbname
        # Session-level options
        self._charset = charset
        self._continue_after_timeout = continue_after_timeout
        self._transaction_id = None

    def close(self):
        pass

    def commit(self):
        if self._transaction_id:
            res = self.client.commit_transaction(
                resourceArn=self._aurora_cluster_arn, secretArn=self._secret_arn, transactionId=self._transaction_id
            )
            self._transaction_id = None
            if res["transactionStatus"] != "Transaction Committed":
                raise DatabaseError("Error while committing transaction: {}".format(res))

    def rollback(self):
        if self._transaction_id:
            self.client.rollback_transaction(
                resourceArn=self._aurora_cluster_arn, secretArn=self._secret_arn, transactionId=self._transaction_id
            )
            self._transaction_id = None

    def cursor(self):
        cursor = AuroraDataAPICursor(connection=self)
        if self._charset:
            cursor.execute("SET character_set_client = '{}'".format(self._charset))
        return cursor

    def __enter__(self):
        return self

    def __exit__(self, err_type, value, traceback):
        if err_type is not None:
            self.rollback()
        else:
            self.commit()

    @retry_exceptions(4, 2, 2, 4, exceptions="DatabaseResumingException")
    def start_transaction(self):
        if self._transaction_id is not None:
            return self._transaction_id
        try:
            res = self.client.begin_transaction(
                database=self._dbname,
                resourceArn=self._aurora_cluster_arn,
                secretArn=self._secret_arn,
            )
        except (self.client.exceptions.BadRequestException,
                self.client.exceptions.DatabaseErrorException) as e:
            # RDS returned an application/db error → map to DB-API subclass
            raise translate_database_error(e) from e
        except Exception as e:
            # Network/credential/endpoint issues, etc.
            raise DatabaseError(e) from e
        self._transaction_id = res["transactionId"]
        return self._transaction_id

    def _prepare_execute_args(self, operation):
        execute_args = dict(
            database=self._dbname, resourceArn=self._aurora_cluster_arn, secretArn=self._secret_arn, sql=operation
        )
        if self._transaction_id:
            execute_args["transactionId"] = self._transaction_id
        return execute_args


class AuroraDataAPICursor:
    def __init__(self, connection, arraysize=1000):
        """Cursor-local state only. All config & client on parent connection."""
        self.arraysize = arraysize
        self.description = None
        self._current_response = None
        self._iterator = None
        self._paging_state = None
        self._connection = connection
        self._buffer: list | None = None
        self._buffer_idx: int = 0

    def _set_description(self, column_metadata):
        self.description = build_description(column_metadata)

    def _start_paginated_query(self, execute_statement_args, records_per_page=None):
        pg_cursor_name = '"{}_{}_{}"'.format(
            __name__, int(time.time()), "".join(random.choices(string.ascii_letters + string.digits, k=8))
        )
        cursor_stmt = "DECLARE " + pg_cursor_name + " SCROLL CURSOR FOR "
        exec_args = dict(execute_statement_args)
        exec_args["sql"] = cursor_stmt + exec_args["sql"]
        logger.debug(f'Starting paginated query with cursor "{pg_cursor_name}"', extra=exec_args)
        self._connection.client.execute_statement(**exec_args)
        self._paging_state = {
            "execute_statement_args": dict(exec_args),
            "records_per_page": records_per_page or self.arraysize,
            "pg_cursor_name": pg_cursor_name,
        }
        # reset buffer for paged mode and immediately fetch first page
        self._buffer, self._buffer_idx = None, 0
        self._fetch_next_page_into_buffer()

    @retry_exceptions(4, 2, 2, 4, exceptions="DatabaseResumingException")
    def execute(self, operation, parameters=None):
        # SA reuses the same cursor across statements (e.g. a sequence
        # ``nextval`` SELECT followed by the INSERT). Stale description
        # from the previous SELECT would otherwise leak into the INSERT
        # and flip ``CursorResult.returns_rows`` to True.
        self.description = None
        self._current_response, self._iterator, self._paging_state = None, None, None
        self._buffer, self._buffer_idx = None, 0
        execute_statement_args: dict = dict(
            self._connection._prepare_execute_args(operation),
            includeResultMetadata=True
        )
        if self._connection._continue_after_timeout is not None:
            execute_statement_args["continueAfterTimeout"] = self._connection._continue_after_timeout
        if parameters:
            execute_statement_args["parameters"] = format_parameters(parameters)
        logger.debug("execute %s", reprlib.repr(operation.strip()))
        try:
            res = self._connection.client.execute_statement(**execute_statement_args)
            # Data API echoes ``columnMetadata`` for many non-SELECT
            # statements (e.g. plain INSERT without RETURNING) — the
            # metadata describes the *table* shape, not a result set.
            # If we set cursor.description from it, SQLAlchemy's
            # ``CursorResult.returns_rows`` flips to True and downstream
            # callers expect a row iterator that doesn't exist. Only
            # populate description when the SQL actually returns rows
            # (SELECT, or any DML with a RETURNING clause).
            if "columnMetadata" in res and _statement_returns_rows(operation):
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
                    self._start_paginated_query(execute_statement_args)
                except self._connection.client.exceptions.UnsupportedResultException as e2:
                    if "The result exceeds the size limit" in str(e2):
                        logger.info(f'Retrying paginated query with smaller pages "{operation.strip()[:30]}..."')
                        self._start_paginated_query(
                            execute_statement_args, records_per_page=max(1, self.arraysize // 2)
                        )
                    else:
                        raise e2
            else:
                raise e
        self._iterator = iter(self)

    @property
    def rowcount(self):
        if self._current_response:
            if "records" in self._current_response:
                return len(self._current_response["records"])
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

    @retry_exceptions(4, 2, 2, 4, exceptions="DatabaseResumingException")
    def executemany(self, operation, seq_of_parameters):
        # No autotransaction here either; batching is auto-committed unless outer tx active
        # See ``execute`` for why we reset description here too: SA may
        # have done a sequence ``nextval`` SELECT on this same cursor
        # before calling executemany and we don't want that description
        # to leak.
        self.description = None
        logger.debug("executemany %s", reprlib.repr(operation.strip()))
        for batch in self._page_input(seq_of_parameters, page_size=self.arraysize):
            batch_execute_statement_args = dict(
                self._connection._prepare_execute_args(operation),
                parameterSets=[format_parameters(p) for p in batch]
            )
            try:
                self._connection.client.batch_execute_statement(**batch_execute_statement_args)
            except self._connection.client.exceptions.BadRequestException as e:
                raise translate_database_error(e) from e

    def _render_response(self, response):
        if "records" in response:
            # convert each record using pure helpers against this cursor's description
            for i, record in enumerate(response["records"]):
                response["records"][i] = tuple(
                    convert_value(value, col_desc=self.description[j] if self.description else None)
                    for j, value in enumerate(record)
                )
        return response

    def scroll(self, value, mode="relative"):
        if not self._paging_state:
            raise InterfaceError("Cursor scroll attempted but pagination is not active")
        scroll_stmt = "MOVE {mode} {value} FROM {pg_cursor_name}".format(
            mode=mode.upper(), value=value, **self._paging_state
        )
        scroll_args = dict(self._paging_state["execute_statement_args"], sql=scroll_stmt)
        logger.debug("Scrolling cursor %s by %d rows", mode, value)
        self._connection.client.execute_statement(**scroll_args)
        self._buffer, self._buffer_idx = None, 0

    # ---------- Shared buffer helpers & page fetcher ----------
    def _has_buffered_row(self) -> bool:
        return bool(self._buffer) and self._buffer_idx < len(self._buffer)

    def _pop_buffered_row(self) -> tuple | None:
        if not self._has_buffered_row():
            return None
        row = self._buffer[self._buffer_idx]
        self._buffer_idx += 1
        return row

    def _fetch_next_page_into_buffer(self) -> None:
        """Fetch next page into _buffer. Sets description if not set yet.
        On oversize, halves page size and retries."""
        if not self._paging_state:
            raise InterfaceError("Paging state missing while fetching next page")

        while True:
            next_page_args = dict(self._paging_state["execute_statement_args"])
            rpp = self._paging_state["records_per_page"]
            next_page_args["sql"] = f'FETCH FORWARD {rpp} FROM {self._paging_state["pg_cursor_name"]}'

            try:
                page = self._connection.client.execute_statement(**next_page_args)
            except self._connection.client.exceptions.UnsupportedResultException as e:
                # 1 MiB response limit. Try smaller pages.
                if "The result exceeds the size limit" in str(e) and rpp > 1:
                    # rewind and backoff
                    self.scroll(-rpp, mode="relative")
                    logger.debug("Halving records per page")
                    self._paging_state["records_per_page"] = max(1, rpp // 2)
                    logger.info("Reduced records per page due to size limit: %d",
                                self._paging_state["records_per_page"])
                    continue
                raise
            except (self._connection.client.exceptions.BadRequestException,
                    self._connection.client.exceptions.DatabaseErrorException) as e:
                raise translate_database_error(e) from e

            if "columnMetadata" in page and page["columnMetadata"] and not self.description:
                self._set_description(page["columnMetadata"])

            page = self._render_response(page)
            self._buffer = list(page.get("records", []))  # materialize rows for fetch APIs
            self._buffer_idx = 0
            return

    def __iter__(self):
        # Yield from buffer; fetch pages on demand if paged
        while True:
            row = self._pop_buffered_row()
            if row is not None:
                yield row
                continue

            # no buffered row available
            if self._paging_state:
                # try to get another page
                self._fetch_next_page_into_buffer()
                # if fetch produced no rows, we're done
                if not self._has_buffered_row():
                    break
                continue
            else:
                # non-paged and buffer exhausted
                break

    def fetchone(self):
        # Consume from buffer; in paged mode, pull pages as needed
        row = self._pop_buffered_row()
        if row is not None:
            return row

        if self._paging_state:
            # try to load a new page
            self._fetch_next_page_into_buffer()
            return self._pop_buffered_row()

        return None

    def fetchmany(self, size: int | None = None):
        if size is None:
            size = self.arraysize
        results: list[tuple] = []
        while size > 0:
            row = self.fetchone()
            if row is None:
                break
            results.append(row)
            size -= 1
        return results

    def fetchall(self):
        rows: list[tuple] = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def close(self):
        self._current_response = None
        self._paging_state = None
        self._buffer, self._buffer_idx = None, 0
        self._iterator = None

    def __enter__(self):
        return self

    def __exit__(self, err_type, value, traceback):
        self.close()


def connect(
    aurora_cluster_arn=None,
    secret_arn=None,
    rds_data_client=None,
    database=None,
    host=None,
    port=None,
    username=None,
    password=None,
    charset=None,
    continue_after_timeout=None,
    region_name=None,
):
    return AuroraDataAPIClient(
        dbname=database,
        aurora_cluster_arn=aurora_cluster_arn,
        secret_arn=secret_arn,
        rds_data_client=rds_data_client,
        charset=charset,
        continue_after_timeout=continue_after_timeout,
        region_name=region_name,
    )
