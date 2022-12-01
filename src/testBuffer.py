import unittest

from src.bundles import Buffer, Bundle


class BufferTest(unittest.TestCase):
	def setUp(self) -> None:
		self.buffer_capacity = 100
		self.buffer = Buffer(capacity=self.buffer_capacity)

	def tearDown(self) -> None:
		del self.buffer

	def test_bundle_append(self):
		"""
		Test that adding a bundle to the buffer works as expected
		"""
		bundle_size = 10
		bundle = Bundle(src=0, dst=1, size=bundle_size)
		self.buffer.append(bundle)

		self.assertEqual(self.buffer.is_empty(), False)
		self.assertEqual(self.buffer.min_bundle_size, bundle_size)
		self.assertEqual(self.buffer.capacity_remaining, self.buffer_capacity-bundle_size)

	def test_bundle_extract(self):
		self.assertEqual(True, False)

	def test_bundle_extract_when_empty(self):
		self.assertEqual(True, False)

	def test_buffer_full(self):
		self.assertEqual(True, False)

	def test_remaining_capacity(self):
		self.assertEqual(True, False)

	def test_is_empty(self):
		self.assertEqual(True, False)


if __name__ == '__main__':
	unittest.main()
