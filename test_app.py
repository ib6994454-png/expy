from data_utils import rial_to_toman
a_rial = 50000
a_toman = rial_to_toman(a_rial)
print(a_toman)


from data_utils import is_valid_transaction
def test_is_valid_transaction():
    assert is_valid_transaction(12) == True
    assert is_valid_transaction(-1) == False