import unittest
import pandas as pd
import datetime as dt
from datacleaner import loanCleaner

# Min tests
# Check duration is an integer and above zero
# Then move on to testing rest of functions

class TestOperations(unittest.TestCase):

    def setUp(self):

        self.test_df = pd.DataFrame({
            'start_date': ['01/01/2025','01/02/2025','01/03/2025'],
            'end_date': ['03/01/2025','05/02/2025','04/02/2025']
        })

        self.test_df['start_date'] = pd.to_datetime(self.test_df['start_date'], format = '%d/%m/%Y')
        self.test_df['end_date'] = pd.to_datetime(self.test_df['end_date'], format = '%d/%m/%Y')

        self.test_data = loanCleaner(self.test_df, 'start_date', 'end_date')

    def test_duration_as_int(self):
        self.assertTrue(pd.api.types.is_integer_dtype(self.test_data['duration']), "The duration is not an integer")

    def test_duration_above_zero(self):
        self.assertTrue(((self.test_data['duration']>=0) | (self.test_data['loan_valid']== False)).all(),"Duration is below zero but valid is not False")

if __name__ == '__main__':
    unittest.main()