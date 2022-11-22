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


def generate_random_bundles(num_bundles, source, destinations):
	bundles = []
	for b in range(random.randint(*num_bundles)):
		bundles.append(
			Bundle(
				source,
				random.choice(destinations),
				size=random.randint(*[1, 3])
			)
		)
	return bundles


def init_nodes(num_nodes, cp):
	node_dict = {}
	for n_uid in range(num_nodes):
		node = Node(
			n_uid,
			buffer=Buffer(100),
			outbound_queues={x: [] for x in range(num_nodes)},
			contact_plan=cp
		)

		# Subscribe to any published messages that indicate a bundle has been sent to
		# this node. This will execute the bundle_receive() method on board the node
		pub.subscribe(node.bundle_receive, str(n_uid) + "bundle")

		bundles = generate_random_bundles(
			[5, 10],
			n_uid,
			[x for x in range(num_nodes) if x != n_uid]
		)

		for b in bundles:
			node.outbound_queues[b.dst].append(b)

		node_dict[n_uid] = node

	return {uid: n for uid, n in node_dict.items()}


def init_contact_plan():
	return [
		Contact(0, 1, 5, 10),
		Contact(1, 0, 6, 12),
		Contact(0, 2, 15, 20),
		Contact(2, 0, 15, 21),
		Contact(2, 3, 24, 26),
		Contact(3, 2, 25, 26)
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
	nodes = init_nodes(4, cp)
	create_route_tables(nodes, cp)

	for node in nodes.values():
		env.process(node.contact_controller(env))  # Generator that initiates contacts
		# TODO Need to add in the generators that do the regular bundle assignment and
		#  route discovery (if applicable)

	env.run(until=30)

	print('')
