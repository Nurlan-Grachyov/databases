import decimal
from datetime import datetime
from parser.async_download.models import Data

import app.utils


def test_is_after_1411():
    now = datetime.now()
    if now.hour >= 14 and now.minute > 11:
        result = app.utils.is_after_1411()
        assert result
    else:
        result = app.utils.is_after_1411()
        assert not result


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
    result = app.utils.to_dict(data)
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
    result = app.utils.decimal_default(dec_value)
    assert result == "123.456"
