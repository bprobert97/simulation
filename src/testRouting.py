import unittest
from copy import deepcopy

import simpy
from pubsub import pub

from main import init_analytics
from node import Node
from bundles import Buffer, Bundle
from scheduling import Scheduler, Request
from routing import Contact, cgr_yens


def init_contact_plan():
	cp = [
		Contact(0, 1, 0, 2),
		Contact(0, 2, 2, 4),
		Contact(1, 2, 13, 15),
		Contact(2, 1, 13, 15),
		Contact(1, 0, 20, 21),
		Contact(2, 0, 25, 28)
	]
	return cp


def init_contact_plan_targets():
	cp = [
		Contact(2, 20, 5, 5),
		Contact(1, 10, 10, 10),
	]
	return cp


def init_requests(env, scheduler):
	wait_times = [0, 2]
	requests = [
		Request(10, deadline_deliver=22, time_created=0, destination=0),
		Request(20, deadline_deliver=30, time_created=2, destination=0)
	]
	while wait_times:
		wait = wait_times.pop(0)
		yield env.timeout(wait)
		request = requests.pop(0)
		print(f'Request submitted for pickup from Target {request.target_id} at time'
		      f' {env.now}')
		scheduler.request_received(request, env.now)


class MyTestCase(unittest.TestCase):
	def test_msr_vs_cgr_simple_case(self):
		"""Validate benefit of MSR over CGR in a simple example

		The scenario here is:
		- Request 1 arrives at time 0, which gets processed into a task for acquisition
			at time 10 and delivery at time 20. The delivery deadline is 21.
		- Request 2 arrives at time 2, which gets processed into a task for acquisition
			at time 5 and delivery at time 25.
		It is possible for the bundle for R2 to be delivered earlier, however because of
		the capacity constraints on the earlier delivery, which has already been
		assigned to R1's bundle, it is deliberately delayed. In the CGR case, however,
		this is not done, since R2's bundle is "older" and therefore should have priority.

		"""
		# analytics = init_analytics()
		# nodes = network_setup(msr=True)
		# del analytics
		# del nodes
		# self.assertEqual(True, False)  # add assertion here
		analytics = init_analytics()
		nodes = network_setup(msr=False)
		print(analytics.bundles_delivered)
		print(analytics.bundles_forwarded)
		print(analytics.bundles_dropped)


def network_setup(msr):
	scheduler = Node(0, Scheduler())
	scheduler.scheduler.parent = scheduler
	node1 = Node(1, msr=msr)
	node2 = Node(2, msr=msr)
	nodes = [scheduler, node1, node2]

	cp = init_contact_plan()
	cpt = init_contact_plan_targets()
	for node in nodes:
		node.update_contact_plan(deepcopy(cp), deepcopy(cpt))
		node.outbound_queues = {x.uid: [] for x in nodes if x.uid != node.uid}
		pub.subscribe(node.bundle_receive, str(node.uid) + "bundle")
		for n_ in [x for x in [0, 1, 2, 10, 20] if x != node.uid]:
			node.route_table[n_] = cgr_yens(
				node.uid, n_, 0, 100, node.contact_plan)

	env = simpy.Environment()
	env.process(init_requests(env, scheduler))
	for node in nodes:
		env.process(node.bundle_assignment_controller(env))
		env.process(node.contact_controller(env))  # Generator that initiates contacts
	env.run(until=100)
	return nodes


if __name__ == '__main__':
	unittest.main()
