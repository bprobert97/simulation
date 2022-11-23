#!/usr/bin/env python3
import itertools
import random
import sys
from queue import PriorityQueue
from copy import deepcopy
import simpy
from pubsub import pub

from node import Node
from routing import Contact, cgr_yens
from scheduling import Scheduler
from bundles import Buffer, Bundle


SCHEDULER_ID = 0
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
		dests = [x for x in destinations if x.uid != source.uid]
		destination = random.choice(dests)
		size = random.randint(*BUNDLE_SIZE)
		deadline = env.now + BUNDLE_TTL
		print(f"bundle generated on node {source.uid} at time {env.now} for destination {destination.uid}")
		source.buffer.append(
			Bundle(
				source.uid,
				destination.uid,
				size=size,
				deadline=deadline
			)
		)


def init_nodes(num_nodes, cp):
	node_list = []
	for n_uid in range(1, num_nodes+1):
		n = Node(
			n_uid,
			buffer=Buffer(NODE_BUFFER_CAPACITY),
			outbound_queues={x: [] for x in range(1, num_nodes+1)},
			contact_plan=cp
		)

		# Subscribe to any published messages that indicate a bundle has been sent to
		# this node. This will execute the bundle_receive() method on board the
		# receiving node at the time when the FULL bundle has been received, including
		# any delay incurred through travel (OWLT)
		pub.subscribe(n.bundle_receive, str(n_uid) + "bundle")

		node_list.append(n)

	return node_list


def init_contact_plan():
	return [
		Contact(0, 1, 0, sys.maxsize),
		Contact(0, 2, 0, sys.maxsize),
		Contact(0, 3, 0, sys.maxsize),
		Contact(0, 4, 0, sys.maxsize),
		Contact(1, 2, 5, 10, owlt=1),
		Contact(2, 1, 6, 12, owlt=1),
		Contact(1, 3, 15, 20, owlt=1),
		Contact(3, 1, 15, 21, owlt=1),
		Contact(3, 4, 24, 26, owlt=1),
		Contact(4, 3, 25, 26, owlt=1)
	]


def create_route_tables(nodes, cp):
	for node in nodes:
		for other in [
			x for x in nodes if x.uid != node.uid
		]:
			routes = cgr_yens(node.uid, other.uid, 0, 5, cp)
			node.route_table[other.uid] = [] if not routes else routes


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
	scheduler = Node(
		SCHEDULER_ID,
		buffer=Buffer(SCHEDULER_BUFFER_CAPACITY),
		contact_plan=cp,
		scheduler=Scheduler(contact_plan=cp)
	)
	nodes = init_nodes(NUM_NODES, cp)
	create_route_tables(nodes, cp)

	env.process(bundle_generator(env, nodes, nodes))
	for node in [scheduler] + nodes:
		env.process(node.bundle_assignment_controller(env))
		env.process(node.contact_controller(env))  # Generator that initiates contacts
		# TODO Need to add in the generators that do the regular bundle assignment and
		#  route discovery (if applicable)

	env.run(until=30)

	print('')
