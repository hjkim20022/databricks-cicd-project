def test_revenue_calculation():
    quantity = 2
    price = 1200

    revenue = quantity * price

    assert revenue == 2400

def test_quantity_type():
    quantity = int(5)

    assert isinstance(quantity, int)