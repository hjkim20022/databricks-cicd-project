def calculate_revenue(quantity, price):
    return quantity * price


def test_revenue():
    assert calculate_revenue(2, 100) == 200


print("Revenue calculation test passed")