import unittest
from calculator_app import Calculator

class TestOperations(unittest.TestCase):

    def setUp(self):
        self.calculation=Calculator(8,2)
    
    # Test funtions must start with test_*
    def test_sum(self):
        self.assertEqual(self.calculation.do_sum(),10, 'Sum is wrong (not 10)')
    
    def test_subtract(self):
        self.assertEqual(self.calculation.do_subtract(),6, 'Difference is wrong (not 6)')

    def test_divide(self):
        self.assertEqual(self.calculation.do_divide(),4, 'Quotient is wrong (not 4)')
    
    def test_product(self):
        self.assertEqual(self.calculation.do_product(),16, 'Product is wrong (not 16)')

if __name__ == '__main__':
    unittest.main()