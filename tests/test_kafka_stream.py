"""Unit tests for the API helpers in dags/kafka_stream.py.

Airflow is stubbed out so the DAG module can be imported without an Airflow
installation; only get_data and format_data are exercised.
"""
import importlib.util
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "kafka_stream.py"

SAMPLE_USER = {
    "name": {"first": "Ada", "last": "Lovelace"},
    "gender": "female",
    "location": {
        "street": {"name": "Main St", "number": 42},
        "city": "London",
        "state": "Greater London",
        "country": "United Kingdom",
        "postcode": 12345,
    },
    "email": "ada@example.com",
    "login": {"username": "ada42"},
    "dob": {"date": "1815-12-10T00:00:00.000Z"},
    "registered": {"date": "2020-01-01T00:00:00.000Z"},
    "phone": "000-000",
    "picture": {"medium": "https://example.com/ada.jpg"},
}


@pytest.fixture(scope="module")
def dag_module():
    airflow = types.ModuleType("airflow")
    airflow.DAG = mock.MagicMock()
    operators = types.ModuleType("airflow.operators")
    python_ops = types.ModuleType("airflow.operators.python")
    python_ops.PythonOperator = mock.MagicMock()
    stubs = {
        "airflow": airflow,
        "airflow.operators": operators,
        "airflow.operators.python": python_ops,
    }
    with mock.patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location("kafka_stream", DAG_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        yield module


def _response(payload, status_error=None):
    response = mock.Mock()
    response.json.return_value = payload
    response.raise_for_status.side_effect = status_error
    return response


def test_get_data_uses_timeout_and_returns_first_result(dag_module):
    with mock.patch("requests.get", return_value=_response({"results": [SAMPLE_USER]})) as get:
        assert dag_module.get_data() == SAMPLE_USER
    get.assert_called_once_with(dag_module.API_URL, timeout=dag_module.API_TIMEOUT_SECONDS)


def test_get_data_raises_on_empty_results(dag_module):
    with mock.patch("requests.get", return_value=_response({"results": []})):
        with pytest.raises(ValueError):
            dag_module.get_data()


def test_get_data_propagates_http_errors(dag_module):
    import requests

    error = requests.HTTPError("503 Service Unavailable")
    with mock.patch("requests.get", return_value=_response({}, status_error=error)):
        with pytest.raises(requests.HTTPError):
            dag_module.get_data()


def test_format_data_flattens_user(dag_module):
    data = dag_module.format_data(SAMPLE_USER)
    assert data == {
        "first_name": "Ada",
        "last_name": "Lovelace",
        "gender": "female",
        "address": "Main St 42 London Greater London United Kingdom",
        "postcode": "12345",
        "email": "ada@example.com",
        "username": "ada42",
        "dob": "1815-12-10T00:00:00.000Z",
        "registered_date": "2020-01-01T00:00:00.000Z",
        "phone": "000-000",
        "picture": "https://example.com/ada.jpg",
    }
