"""
aurora-data-api - A Python DB-API 2.0 client for the AWS Aurora Serverless Data API
"""
import datetime, ipaddress, uuid
from decimal import Decimal
from collections import namedtuple
from collections.abc import Mapping

from .exceptions import NotSupportedError

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

ColumnDescription = namedtuple(
    "ColumnDescription",
    "name type_code display_size internal_size precision scale null_ok pg_type_name",
)
ColumnDescription.__new__.__defaults__ = (None,) * len(ColumnDescription._fields)

# Postgres type name -> Python type for description.type_code
_PG_TYPE_MAP = {
    "int": int, "int2": int, "int4": int, "int8": int,
    "float4": float, "float8": float,
    "serial2": int, "serial4": int, "serial8": int,
    "bool": bool,
    "varbit": bytes, "bytea": bytearray,
    "char": str, "varchar": str, "text": str,
    "cidr": ipaddress.ip_network, "inet": ipaddress.ip_address,
    "date": datetime.date,
    "time": datetime.time, "timetz": datetime.time,
    "timestamp": datetime.datetime, "timestamptz": datetime.datetime,
    "uuid": uuid.UUID,
    "json": dict, "jsonb": dict,
    "money": str,
    "numeric": Decimal, "decimal": Decimal,
}

# PG types whose returned value represents a moment in time anchored to UTC.
# The Data API normalises timestamptz/timetz to UTC on the wire and strips
# the offset suffix, so the parsed value comes back naive even though it is
# semantically UTC. We attach tzinfo=utc on parse to restore that meaning.
_PG_TZ_AWARE_TYPES = frozenset({"timestamptz", "timetz"})

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
        pg_type_name = column["typeName"].lower()
        type_code = _PG_TYPE_MAP.get(pg_type_name, str)
        col_desc = ColumnDescription(
            name=column["name"],
            type_code=type_code,
            pg_type_name=pg_type_name,
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
            parsed = tc.fromisoformat(scalar)
        except (AttributeError, ValueError):
            # Fallbacks for older Python / non-ISO strings
            if tc is datetime.date:
                parsed = datetime.datetime.strptime(scalar, "%Y-%m-%d").date()
            elif tc is datetime.time:
                parsed = datetime.datetime.strptime(scalar, "%H:%M:%S").time()
            elif "." in scalar:
                parsed = datetime.datetime.strptime(scalar, "%Y-%m-%d %H:%M:%S.%f")
            else:
                parsed = datetime.datetime.strptime(scalar, "%Y-%m-%d %H:%M:%S")
        # For tz-aware PG types the Data API serialises in UTC but strips the
        # offset, so fromisoformat returns a naive value. Attach UTC so consumer
        # code can do arithmetic against other aware datetimes without TypeError.
        if (
            col_desc.pg_type_name in _PG_TZ_AWARE_TYPES
            and isinstance(parsed, (datetime.datetime, datetime.time))
            and parsed.tzinfo is None
        ):
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed
    return scalar

