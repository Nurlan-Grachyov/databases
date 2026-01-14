import decimal
from datetime import time
from parser.async_download.models import Data
from unittest.mock import patch

from app.utils import decimal_default, is_after_1411, to_dict


@patch("app.utils.datetime")
def test_is_after_1411_true(mock_datetime):
    mock_now = mock_datetime.now.return_value
    mock_now.time.return_value = time(14, 12)

    assert is_after_1411() is True


def test_to_dict():
    data = Data(
        id=1,
        exchange_product_id="A100ANK060F",
        exchange_product_name="Бензин (АИ-100-К5), Ангарск-группа станций (ст. отправления)",
        delivery_basis_name="Ангарск-группа станций",
        volume=60,
        total=3459540,
        count=1,
    )
    result = to_dict(data)
    assert result == {
        "id": 1,
        "exchange_product_id": "A100ANK060F",
        "exchange_product_name": "Бензин (АИ-100-К5), Ангарск-группа станций (ст. отправления)",
        "oil_id": None,
        "delivery_basis_id": None,
        "delivery_basis_name": "Ангарск-группа станций",
        "delivery_type_id": None,
        "volume": 60,
        "total": 3459540,
        "count": 1,
        "date": None,
        "created_on": None,
        "updated_on": None,
    }


def test_decimal_default():
    dec_value = decimal.Decimal("123.456")
    result = decimal_default(dec_value)
    assert result == "123.456"
