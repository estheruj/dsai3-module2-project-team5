import unittest
from src.dq_checks.completeness import check_completeness
from src.dq_checks.accuracy import check_accuracy
from src.dq_checks.uniqueness import check_uniqueness
from src.dq_checks.validity import check_validity
from src.dq_checks.consistency import check_consistency
from src.dq_checks.timeliness import check_timeliness
from src.utils.score_calculator import calculate_score
from src.utils.threshold_validator import validate_threshold

class TestDataQualityChecks(unittest.TestCase):

    def setUp(self):
        self.data = {
            'column1': [1, 2, 3, None],
            'column2': ['a', 'b', 'c', 'd'],
            'column3': [1, 1, 2, 3],
            'column4': ['2021-01-01', '2021-01-02', None, '2021-01-04']
        }
        self.threshold = 98

    def test_completeness(self):
        result = check_completeness(self.data)
        self.assertGreaterEqual(result, self.threshold)

    def test_accuracy(self):
        result = check_accuracy(self.data)
        self.assertGreaterEqual(result, self.threshold)

    def test_uniqueness(self):
        result = check_uniqueness(self.data)
        self.assertGreaterEqual(result, self.threshold)

    def test_validity(self):
        result = check_validity(self.data)
        self.assertGreaterEqual(result, self.threshold)

    def test_consistency(self):
        result = check_consistency(self.data)
        self.assertGreaterEqual(result, self.threshold)

    def test_timeliness(self):
        result = check_timeliness(self.data)
        self.assertGreaterEqual(result, self.threshold)

    def test_score_calculation(self):
        score = calculate_score(self.data)
        self.assertIsInstance(score, float)

    def test_threshold_validation(self):
        is_valid = validate_threshold(99, self.threshold)
        self.assertTrue(is_valid)

if __name__ == '__main__':
    unittest.main()