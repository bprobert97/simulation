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
BUNDLE_ARRIVAL_RATE = .5  # Mean number of bundles to be generated per unit time
BUNDLE_TTL = 100  # Time to live for a bundle


def bundle_generator(env, sources, destinations):
	"""
	Process that generates bundles on nodes according to some probability for the
	duration of the simulation
	"""
	while True:
		yield env.timeout(random.expovariate(1 / BUNDLE_ARRIVAL_RATE))
		source = random.choice(sources)
		dests = [x for x in destinations if x != source.uid]
		destination = random.choice(dests)
		size = random.randint(*BUNDLE_SIZE)
		deadline = env.now + BUNDLE_TTL
		print(f"bundle generated on node {source.uid} at time {env.now}")
		source.buffer.append(
			Bundle(
				source.uid,
				destination,
				size=size,
				deadline=deadline
			)
		)


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
		# this node. This will execute the bundle_receive() method on board the
		# receiving node at the time when the FULL bundle has been received, including
		# any delay incurred through travel (OWLT)
		pub.subscribe(n.bundle_receive, str(n_uid) + "bundle")

		# bundles = bundle_generator(
		# 	NUM_BUNDLES,
		# 	n_uid,
		# 	[x for x in range(num_nodes) if x != n_uid]
		# )
		#
		# for b in bundles:
		# 	n.buffer.append(b)

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
			node.route_table[other_uid] = [] if not routes else routes


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

	env.process(bundle_generator(env, nodes, nodes))
	for node in nodes.values():
		env.process(node.bundle_assignment_controller(env))
		env.process(node.contact_controller(env))  # Generator that initiates contacts
		# TODO Need to add in the generators that do the regular bundle assignment and
		#  route discovery (if applicable)

	env.run(until=30)

	print('')
