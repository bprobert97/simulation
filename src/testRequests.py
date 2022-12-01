import unittest
from src.main import get_request_inter_arrival_time


class RequestArrivalTests(unittest.TestCase):
    def test_request_arrival_rate_based_on_congestion(self):
        congestion = 0.5
        outflow = 1000  # volume of data that can be "delivered"
        simulation_time = 10000
        bundle_size = 4

        # Assert that the inter-request arrival time should be 80, for the above
        self.assertEqual(
            get_request_inter_arrival_time(
                simulation_time, outflow, congestion, bundle_size
            ), 80
        )


if __name__ == '__main__':
    unittest.main()
