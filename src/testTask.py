import unittest
from scheduling import Task


class TaskTesting(unittest.TestCase):
	def test_task_order(self):
		task_pending = Task()
		task_pending2 = Task()

		task_acquired = Task()
		task_acquired.status = "acquired"

		task_redundant = Task()
		task_redundant.status = "redundant"

		task_delivered = Task()
		task_delivered.status = "delivered"

		task_failed = Task()
		task_failed.status = "failed"

		task_rescheduled = Task()
		task_rescheduled.status = "rescheduled"

		self.assertFalse(task_pending < task_pending2)

		self.assertTrue(task_pending < task_acquired)

		self.assertTrue(task_acquired < task_delivered)
		self.assertTrue(task_pending < task_delivered)
		self.assertTrue(task_redundant < task_delivered)

		self.assertLess(task_pending, task_redundant)
		self.assertLess(task_acquired, task_redundant)


if __name__ == '__main__':
	unittest.main()
