import unittest
import numpy as np
from main import Predict


class TestCalculator(unittest.TestCase):

    def test_add(self):
        result = Predict().model_predict(np.ones((1, 68)))
        self.assertEqual(result, 1)


if __name__ == '__main__':
    unittest.main()