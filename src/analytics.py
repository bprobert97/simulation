#!/usr/bin/env python3


class Analytics:
	def __init__(self):
		self.tasks = []
		self.failed_tasks = 0
		self.redundant_tasks = 0
		self.renewed_tasks = 0

		self.bundles = []
		self.bundles_acquired = 0
		self.bundles_forwarded = 0
		self.bundles_delivered = 0
		self.bundles_dropped = 0
		self.bundles_rerouted = 0

	def add_task(self, t):
		self.tasks.append(t)

	def task_failed(self):
		self.failed_tasks += 1

	def task_redundant(self):
		self.redundant_tasks += 1

	def task_renewal(self):
		"""
		If a redundant task is replaced by a new task, this method is triggered
		"""
		self.renewed_tasks += 1

	def add_bundle(self, b):
		self.bundles.append(b)
		self.bundles_acquired += 1

	def forward_bundle(self):
		self.bundles_forwarded += 1

	def deliver_bundle(self):
		self.bundles_delivered += 1

	def drop_bundle(self):
		self.bundles_dropped += 1

	def reroute_bundle(self):
		"""
		Method invoked any time a bundle is assigned to a route that differs from the
		one along which it was most recently assigned. E.g. if a Bundle was due to
		traverse the route 3->4->6, but doesn't make it over contact 3 and therefore
		gets reassigned to the route 5->7, this would constitute a "reroute" event
		"""
		self.bundles_rerouted += 1

