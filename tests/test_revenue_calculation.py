def calculate_revenue(quantity, price):
    return quantity * price


def test_revenue():
    assert calculate_revenue(2, 100) == 200
    assert calculate_revenue(5, 80) == 400

print("Revenue calculation test passed")