"""Unit tests for parameter serialization to the Data API wire format.

Pure-Python tests, no DB."""
import pytest

from aurora_data_api.type_conversion import _prepare_param, _list_to_array_value


# ---- _list_to_array_value: structural array conversion ---- #

@pytest.mark.parametrize("lst, expected", [
    ([1, 2, 3], {"longValues": [1, 2, 3]}),
    ([1.5, 2.5], {"doubleValues": [1.5, 2.5]}),
    (["a", "b"], {"stringValues": ["a", "b"]}),
    ([True, False, True], {"booleanValues": [True, False, True]}),
    ([], {"stringValues": []}),
])
def test_flat_arrays(lst, expected):
    assert _list_to_array_value(lst) == expected


def test_nested_arrays():
    # Multi-dim: each inner list becomes its own arrayValue.
    assert _list_to_array_value([[1, 2], [3, 4]]) == {
        "arrayValues": [
            {"longValues": [1, 2]},
            {"longValues": [3, 4]},
        ]
    }


def test_nested_string_arrays():
    assert _list_to_array_value([["a", "b"], ["c", "d"]]) == {
        "arrayValues": [
            {"stringValues": ["a", "b"]},
            {"stringValues": ["c", "d"]},
        ]
    }


def test_mixed_type_falls_back_to_stringvalues():
    # Truly mixed Python types -> string representations.
    inner = _list_to_array_value([1, "a"])
    assert inner == {"stringValues": ["1", "a"]}


def test_none_in_list_falls_back_to_str():
    # Data API doesn't allow null inside typed *Values lists; fall back
    # to a single stringValues element with the repr of the whole list.
    inner = _list_to_array_value([1, None, 2])
    assert "stringValues" in inner
    assert inner["stringValues"] == [str([1, None, 2])]


# ---- _prepare_param: the full wire dict ---- #

def test_none_is_isnull():
    assert _prepare_param("p", None) == {"name": "p", "value": {"isNull": True}}


def test_int_uses_longvalue():
    assert _prepare_param("p", 5) == {"name": "p", "value": {"longValue": 5}}


def test_str_uses_stringvalue():
    assert _prepare_param("p", "hi") == {"name": "p", "value": {"stringValue": "hi"}}


def test_list_of_ints_emits_arrayvalue():
    assert _prepare_param("p", [1, 2, 3]) == {
        "name": "p",
        "value": {"arrayValue": {"longValues": [1, 2, 3]}},
    }


def test_nested_list_emits_nested_arrayvalues():
    assert _prepare_param("p", [[1, 2], [3, 4]]) == {
        "name": "p",
        "value": {
            "arrayValue": {
                "arrayValues": [
                    {"longValues": [1, 2]},
                    {"longValues": [3, 4]},
                ]
            }
        },
    }


def test_list_does_not_attach_typehint():
    # The hint map is keyed on scalar Python types; arrays bypass it.
    out = _prepare_param("p", [1, 2, 3])
    assert "typeHint" not in out


def test_scalar_datetime_still_gets_typehint():
    # Regression: scalar non-list path keeps the existing TIMESTAMP hint.
    import datetime
    out = _prepare_param("p", datetime.datetime(2024, 1, 1, 12, 0, 0))
    assert out["typeHint"] == "TIMESTAMP"
