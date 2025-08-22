"""
aurora-data-api - A Python DB-API 2.0 client for the AWS Aurora Serverless Data API
"""
import os, datetime, ipaddress, uuid, time, random, string, logging, itertools, reprlib, json, re, threading
from decimal import Decimal
from collections import namedtuple
from collections.abc import Mapping

import boto3

from .exceptions import translate_database_error, InterfaceError, NotSupportedError, DatabaseError

apilevel = "2.0"

threadsafety = 0

paramstyle = "named"

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = datetime.date.fromtimestamp
# TimeFromTicks = datetime.time.fromtimestamp TODO
TimestampFromTicks = datetime.datetime.fromtimestamp
Binary = bytes
STRING = str
BINARY = bytes
NUMBER = float
DATETIME = datetime.datetime
ROWID = str
DECIMAL = Decimal

ColumnDescription = namedtuple("ColumnDescription", "name type_code display_size internal_size precision scale null_ok")
ColumnDescription.__new__.__defaults__ = (None,) * len(ColumnDescription._fields)

logger = logging.getLogger(__name__)

# Postgres type name -> Python type for description.type_code
_PG_TYPE_MAP = {
    "int": int, "int2": int, "int4": int, "int8": int,
    "float4": float, "float8": float,
    "serial2": int, "serial4": int, "serial8": int,
    "bool": bool,
    "varbit": bytes, "bytea": bytearray,
    "char": str, "varchar": str, "text": str,
    "cidr": ipaddress.ip_network, "inet": ipaddress.ip_address,
    "date": datetime.date, "time": datetime.time, "timestamp": datetime.datetime,
    "uuid": uuid.UUID,
    "json": dict, "jsonb": dict,
    "money": str,
    "numeric": Decimal, "decimal": Decimal,
}

# Python value type -> RDS Data API field name
_DATA_API_TYPE_MAP = {
    bytes: "blobValue",
    bool: "booleanValue",
    float: "doubleValue",
    int: "longValue",
    str: "stringValue",
    Decimal: "stringValue",
    # list -> "arrayValue" not supported by our DB-API paramstyle mapping here
}

# Python value type -> RDS Data API typeHint
_DATA_API_TYPE_HINT_MAP = {
    datetime.date: "DATE",
    datetime.time: "TIME",
    datetime.datetime: "TIMESTAMP",
    Decimal: "DECIMAL",
}

def build_description(column_metadata):
    """Return DB-API description list from column metadata."""
    desc = []
    for column in column_metadata:
        type_code = _PG_TYPE_MAP.get(column["typeName"].lower(), str)
        col_desc = ColumnDescription(
            name=column["name"],
            type_code=type_code,
        )
        desc.append(col_desc)
    return desc

def _prepare_param(name, value):
    if value is None:
        return dict(name=name, value=dict(isNull=True))
    data_api_field = _DATA_API_TYPE_MAP.get(type(value), "stringValue")
    param = dict(name=name, value={data_api_field: value})
    if data_api_field == "stringValue" and not isinstance(value, str):
        param["value"][data_api_field] = str(value)
    hint = _DATA_API_TYPE_HINT_MAP.get(type(value))
    if hint:
        param["typeHint"] = hint
    return param

def format_parameters(parameters):
    """Mapping[str, Any] -> list of named parameter dicts for Data API."""
    if not isinstance(parameters, Mapping):
        raise NotSupportedError("Expected a mapping of parameters. Array parameters are not supported.")
    return [_prepare_param(k, v) for k, v in parameters.items()]

def convert_value(value_dict, col_desc=None):
    """Convert a single RDS Data API field dict to a Python scalar/array."""
    if value_dict.get("isNull"):
        return None
    if "arrayValue" in value_dict:
        arr = value_dict["arrayValue"]
        if "arrayValues" in arr:
            return [convert_value(nested) for nested in arr["arrayValue"]["arrayValues"]]
        # primitive arrays (e.g., {longValues: [...]}) come through as a dict with exactly one key
        return list(arr.values())[0]

    # scalar
    scalar = list(value_dict.values())[0]
    if not col_desc:
        return scalar

    tc = col_desc.type_code
    # If the column's Python type suggests a string-encoded temporal/decimal, coerce
    if tc in _DATA_API_TYPE_HINT_MAP:
        if tc is Decimal:
            return Decimal(scalar)
        try:
            return tc.fromisoformat(scalar)
        except (AttributeError, ValueError):
            # Fallbacks for older Python / non-ISO strings
            if tc is datetime.date:
                return datetime.datetime.strptime(scalar, "%Y-%m-%d").date()
            if tc is datetime.time:
                return datetime.datetime.strptime(scalar, "%H:%M:%S").time()
            if "." in scalar:
                return datetime.datetime.strptime(scalar, "%Y-%m-%d %H:%M:%S.%f")
            return datetime.datetime.strptime(scalar, "%Y-%m-%d %H:%M:%S")
    return scalar




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
    ):
        # AWS client and connection config
        self.client = rds_data_client
        if self.client is None:
            with self._client_init_lock:
                self.client = boto3.client("rds-data")

        self._dbname = dbname
        self._aurora_cluster_arn = aurora_cluster_arn or os.environ.get("AURORA_CLUSTER_ARN")
        self._secret_arn = secret_arn or os.environ.get("AURORA_SECRET_ARN")
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

    def start_transaction(self):
        """
        Begin a transaction and cache the transaction id.  Returns the
        active transaction_id.
        """
        if self._transaction_id is None:
            res = self.client.begin_transaction(
                database=self._dbname,
                resourceArn=self._aurora_cluster_arn,
                secretArn=self._secret_arn,
            )
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

    def _set_description(self, column_metadata):
        self.description = build_description(column_metadata)

    def _start_paginated_query(self, execute_statement_args, records_per_page=None):
        pg_cursor_name = "{}_{}_{}".format(
            __name__, int(time.time()), "".join(random.choices(string.ascii_letters + string.digits, k=8))
        )
        cursor_stmt = "DECLARE " + pg_cursor_name + " SCROLL CURSOR FOR "
        execute_statement_args["sql"] = cursor_stmt + execute_statement_args["sql"]
        self._connection.client.execute_statement(**execute_statement_args)
        self._paging_state = {
            "execute_statement_args": dict(execute_statement_args),
            "records_per_page": records_per_page or self.arraysize,
            "pg_cursor_name": pg_cursor_name,
        }

    def execute(self, operation, parameters=None):
        self._current_response, self._iterator, self._paging_state = None, None, None
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
            if "columnMetadata" in res:
                self._set_description(res["columnMetadata"])
            self._current_response = self._render_response(res)
        except (self._connection.client.exceptions.BadRequestException,
                self._connection.client.exceptions.DatabaseErrorException) as e:
            if "Please paginate your query" in str(e):
                self._start_paginated_query(execute_statement_args)
            elif "Database returned more than the allowed response size limit" in str(e):
                self._start_paginated_query(execute_statement_args, records_per_page=max(1, self.arraysize // 2))
            else:
                raise translate_database_error(e) from e
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

    def _page_input(self, iterable, page_size=1000):
        iterable = iter(iterable)
        return iter(lambda: list(itertools.islice(iterable, page_size)), [])

    def executemany(self, operation, seq_of_parameters):
        # No autotransaction here either; batching is auto-committed unless outer tx active
        logger.debug("executemany %s", reprlib.repr(operation.strip()))
        for batch in self._page_input(seq_of_parameters):
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

    def __iter__(self):
        if self._paging_state:
            next_page_args = self._paging_state["execute_statement_args"]
            while True:
                logger.debug(
                    "Fetching page of %d records for auto-paginated query", self._paging_state["records_per_page"]
                )
                next_page_args["sql"] = "FETCH {records_per_page} FROM {pg_cursor_name}".format(**self._paging_state)
                try:
                    page = self._connection.client.execute_statement(**next_page_args)
                except self._connection.client.exceptions.BadRequestException as e:
                    cur_rpp = self._paging_state["records_per_page"]
                    if "Database returned more than the allowed response size limit" in str(e) and cur_rpp > 1:
                        self.scroll(-self._paging_state["records_per_page"])  # Rewind the cursor to read the page again
                        logger.debug("Halving records per page")
                        self._paging_state["records_per_page"] //= 2
                        continue
                    else:
                        raise translate_database_error(e) from e

                if "columnMetadata" in page and not self.description:
                    self._set_description(page["columnMetadata"])
                if len(page["records"]) == 0:
                    break
                page = self._render_response(page)
                for record in page["records"]:
                    yield record
        else:
            for record in self._current_response.get("records", []):
                yield record

    def fetchone(self):
        try:
            return next(self._iterator)
        except StopIteration:
            pass

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        results = []
        while size > 0:
            result = self.fetchone()
            if result is None:
                break
            results.append(result)
            size -= 1
        return results

    def fetchall(self):
        return list(self._iterator)

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, err_type, value, traceback):
        self._iterator = None
        self._current_response = None


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
):
    return AuroraDataAPIClient(
        dbname=database,
        aurora_cluster_arn=aurora_cluster_arn,
        secret_arn=secret_arn,
        rds_data_client=rds_data_client,
        charset=charset,
        continue_after_timeout=continue_after_timeout,
    )
