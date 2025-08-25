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

    def _set_description(self, column_metadata):
        self.description = build_description(column_metadata)

    def _start_paginated_query(self, execute_statement_args, records_per_page=None):
        pg_cursor_name = '"{}_{}_{}"'.format(
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

    @retry_exceptions(4, 2, 2, 4, exceptions="DatabaseResumingException")
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
            raise translate_database_error(e) from e
        except self._connection.client.exceptions.UnsupportedResultException as e:
            if "The result exceeds the size limit" in str(e):
                try:
                    self._start_paginated_query(execute_statement_args)
                except self._connection.client.exceptions.UnsupportedResultException as e:
                    if "The result exceeds the size limit" in str(e):
                        self._start_paginated_query(
                            execute_statement_args, records_per_page=max(1, self.arraysize // 2))
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

    def _page_input(self, iterable, page_size=1000):
        iterable = iter(iterable)
        return iter(lambda: list(itertools.islice(iterable, page_size)), [])

    @retry_exceptions(4, 2, 2, 4, exceptions="DatabaseResumingException")
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
                except self._connection.client.exceptions.UnsupportedResultException as e:
                    cur_rpp = self._paging_state["records_per_page"]
                    if "The result exceeds the size limit" in str(e) and cur_rpp > 1:
                        self.scroll(-self._paging_state["records_per_page"])
                        logger.debug("Halving records per page")
                        self._paging_state["records_per_page"] //= 2
                        continue
                    else:
                        raise e
                except (self._connection.client.exceptions.BadRequestException,
                        self._connection.client.exceptions.DatabaseErrorException) as e:
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
