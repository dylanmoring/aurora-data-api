import re

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

def translate_database_error(original_error):
    """Map boto3 Data API exceptions to DB-API error subclasses."""
    error_msg = getattr(original_error, "response", {}).get("Error", {}).get("Message", "")
    try:
        res = re.search(r"Error code: (\d+); SQLState: (\d+)$", error_msg)
        if res:  # MySQL error
            error_code = int(res.group(1))
            error_class = MySQLError.from_code(error_code)
            error = error_class(error_msg)
            error.response = getattr(original_error, "response", {})
            return error
        # ``Position`` is only present for syntax-style errors; integrity /
        # check / FK / unique violations omit it. Make it optional so those
        # errors still translate to the right DB-API subclass via the
        # SQLSTATE class prefix.
        res = re.search(
            r"ERROR: .*(?:\n |;)(?: Position: \d+;)? SQLState: (\w+)$",
            error_msg,
        )
        if res:  # PostgreSQL error
            error_code = res.group(1)
            error_class = PostgreSQLError.from_code(error_code)
            error = error_class(error_msg)
            error.response = getattr(original_error, "response", {})
            return error
    except Exception:
        pass
    return DatabaseError(original_error)