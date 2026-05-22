def test_revenue_calculation():
    quantity = 2
    price = 1200

    revenue = quantity * price

    assert revenue == 2400

def test_quantity_type():
    quantity = int(5)

    assert isinstance(quantity, int)

def test_null_check():
    product = "Laptop"

    assert product is not None  

def test_duplicate_order_ids():
    order_ids = [1001, 1002, 1003]

    assert len(order_ids) == len(set(order_ids))

def test_negative_price():
    price = 1200

    assert price > 0

def test_order_date_exists():
    order_date = "2026-05-01"

    assert order_date is not None


def test_revenue_not_negative():
    revenue = 2400
    assert revenue >= 0


def test_category_not_null():
    category = "Electronics"
    assert category is not None


def test_orderid_unique():
    order_ids = [1001, 1002, 1003]
    assert len(order_ids) == len(set(order_ids))
