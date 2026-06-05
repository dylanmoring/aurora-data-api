from .mysql_error_codes import MySQLErrorCodes
from .postgresql_error_codes import PostgreSQLErrorCodes


class Warning(Exception):
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class _DatabaseErrorFactory:
    # Override in subclass to map a code → DB-API base class. Default
    # keeps the historical behavior of "everything is DatabaseError".
    @staticmethod
    def base_class_for(err_code):  # noqa: ARG004
        return DatabaseError

    def __getattr__(self, a):
        member = getattr(self.err_index, a)
        base = self.base_class_for(member.value)
        err_cls = type(member.name, (base,), {})
        setattr(self, a, err_cls)
        return err_cls

    def from_code(self, err_code):
        return getattr(self, self.err_index(err_code).name)


class _MySQLErrorFactory(_DatabaseErrorFactory):
    err_index = MySQLErrorCodes


# PostgreSQL SQLSTATE class → DB-API exception. The class is the first two
# characters of the five-character SQLSTATE; the codes themselves are
# documented at https://www.postgresql.org/docs/current/errcodes-appendix.html.
# This mapping mirrors what psycopg2/asyncpg do, so that SQLAlchemy's
# ``dialect.dbapi.IntegrityError`` / ``ProgrammingError`` / ``OperationalError``
# isinstance checks resolve correctly and callers' ``except IntegrityError``
# clauses fire.
_PG_SQLSTATE_CLASS_TO_BASE = {
    # 08: Connection Exception
    "08": OperationalError,
    # 22: Data Exception (invalid input, division by zero, etc.)
    "22": DataError,
    # 23: Integrity Constraint Violation (unique, fk, check, not-null)
    "23": IntegrityError,
    # 25: Invalid Transaction State
    "25": InternalError,
    # 28: Invalid Authorization Specification
    "28": OperationalError,
    # 40: Transaction Rollback (serialization, deadlock)
    "40": OperationalError,
    # 42: Syntax Error or Access Rule Violation
    "42": ProgrammingError,
    # 53: Insufficient Resources
    "53": OperationalError,
    # 54: Program Limit Exceeded
    "54": OperationalError,
    # 57: Operator Intervention (cancel, shutdown)
    "57": OperationalError,
    # 58: System Error (out of memory, disk full)
    "58": OperationalError,
    # 0A: Feature Not Supported
    "0A": NotSupportedError,
    # F0: Configuration File Error
    "F0": InternalError,
    # XX: Internal Error
    "XX": InternalError,
}


class _PostgreSQLErrorFactory(_DatabaseErrorFactory):
    err_index = PostgreSQLErrorCodes

    @staticmethod
    def base_class_for(err_code):
        # Code is a five-char SQLSTATE like "23505". First two chars are
        # the class. Fall back to DatabaseError for unknown classes.
        if isinstance(err_code, str) and len(err_code) >= 2:
            return _PG_SQLSTATE_CLASS_TO_BASE.get(err_code[:2], DatabaseError)
        return DatabaseError


MySQLError = _MySQLErrorFactory()
PostgreSQLError = _PostgreSQLErrorFactory()
