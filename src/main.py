#!/usr/bin/env python3
import itertools
import random
from queue import PriorityQueue
from copy import deepcopy
import simpy
from pubsub import pub

from node import Node
from routing import Contact, cgr_yens
from scheduling import Scheduler
from bundles import Buffer, Bundle


SCHEDULER_ID = 999
SCHEDULER_BUFFER_CAPACITY = 1000
NUM_NODES = 4
NODE_BUFFER_CAPACITY = 100
NUM_BUNDLES = [5, 10]
BUNDLE_SIZE = [1, 3]


def generate_random_bundles(num_bundles, source, destinations):
	bundles = []
	for b in range(random.randint(*num_bundles)):
		bundles.append(
			Bundle(
				source,
				random.choice(destinations),
				size=random.randint(*BUNDLE_SIZE)
			)
		)
	return bundles


def init_nodes(num_nodes, cp):
	node_dict = {}
	for n_uid in range(num_nodes):
		n = Node(
			n_uid,
			buffer=Buffer(NODE_BUFFER_CAPACITY),
			outbound_queues={x: [] for x in range(num_nodes)},
			contact_plan=cp
		)

		# Subscribe to any published messages that indicate a bundle has been sent to
		# this node. This will execute the bundle_receive() method on board the node
		pub.subscribe(n.bundle_receive, str(n_uid) + "bundle")

		bundles = generate_random_bundles(
			NUM_BUNDLES,
			n_uid,
			[x for x in range(num_nodes) if x != n_uid]
		)

		for b in bundles:
			n.buffer.append(b)

		node_dict[n_uid] = n

	# Add the scheduler node
	# node_dict[SCHEDULER_ID] = Node(
	# 	SCHEDULER_ID,
	# 	buffer=Buffer(SCHEDULER_BUFFER_CAPACITY),
	# 	contact_plan=cp,
	# 	scheduler=Scheduler(contact_plan=cp)
	# )

	return {uid: n for uid, n in node_dict.items()}


def init_contact_plan():
	return [
		Contact(0, 1, 5, 10, owlt=1),
		Contact(1, 0, 6, 12, owlt=1),
		Contact(0, 2, 15, 20, owlt=1),
		Contact(2, 0, 15, 21, owlt=1),
		Contact(2, 3, 24, 26, owlt=1),
		Contact(3, 2, 25, 26, owlt=1)
	]


def create_route_tables(nodes, cp):
	for n_uid, node in nodes.items():
		for other_uid, other in {
			x_uid: x for x_uid, x in nodes.items() if x_uid != n_uid
		}.items():
			routes = cgr_yens(n_uid, other_uid, 0, 5, cp)
			node.route_table[other_uid] = [] if not routes else deepcopy(routes)


if __name__ == "__main__":
	"""
	Contact Graph Routing implementation
	
	Requests are submitted to one or more Scheduler nodes, which process into Tasks 
	that are distributed through a delay-tolerant network so that nodes can execute 
	tasks according to their assignation (i.e. bundle acquisition). Acquired bundles 
	are routed through the network via either CGR or MSR, as specified.
	"""
	env = simpy.Environment()
	cp = init_contact_plan()
	nodes = init_nodes(NUM_NODES, cp)
	create_route_tables(nodes, cp)

	for node in nodes.values():
		node.bundle_assignment(env)
		env.process(node.contact_controller(env))  # Generator that initiates contacts
		# TODO Need to add in the generators that do the regular bundle assignment and
		#  route discovery (if applicable)

	env.run(until=30)

	print('')
