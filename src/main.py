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
from scheduling import Scheduler, Request
from bundles import Buffer, Bundle


SCHEDULER_ID = 0
SCHEDULER_BUFFER_CAPACITY = 1000
NUM_NODES = 4
NODE_BUFFER_CAPACITY = 100
NUM_BUNDLES = [5, 10]
REQUEST_ARRIVAL_RATE = 0.2
TARGET_UID = 1000
BUNDLE_SIZE = [1, 3]
BUNDLE_ARRIVAL_RATE = 0.2  # Mean number of bundles to be generated per unit time
BUNDLE_TTL = 25  # Time to live for a


def requests_generator(env, nodes, scheduler):
	"""
	Generate requests that get submitted to a scheduler where they are processed into
	tasks, added to a task table, and distributed through the network for execution by
	nodes.
	"""
	while True:
		# yield env.timeout(random.expovariate(1 / REQUEST_ARRIVAL_RATE))
		yield env.timeout(0)
		request = Request(
			TARGET_UID,
			destination=4,  # random.choice(nodes),
			data_volume=1,  # random.randint(*BUNDLE_SIZE),
			time_created=env.now
		)
		scheduler.scheduler.request_received(request, env.now)
		scheduler.task_table = scheduler.scheduler.task_table
		break


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
			contact_plan=deepcopy(cp)
		)

		# Subscribe to any published messages that indicate a bundle has been sent to
		# this node. This will execute the bundle_receive() method on board the
		# receiving node at the time when the FULL bundle has been received, including
		# any delay incurred through travel (OWLT)
		pub.subscribe(n.bundle_receive, str(n_uid) + "bundle")

		node_list.append(n)

	return node_list


def init_contact_plan():
	"""
	Create a contact plan. This must include contacts between the Scheduler (UID=0) and
	remote nodes (UID=[0, 999]) and also between remote nodes and the target (UID=1000).
	"""
	cp = [
		Contact(0, 1, .1, sys.maxsize),
		Contact(0, 2, .2, sys.maxsize),
		Contact(0, 3, .3, sys.maxsize),
		Contact(0, 4, .4, sys.maxsize),
		Contact(1, 2, 5, 10, owlt=1),
		Contact(2, 1, 6, 12, owlt=1),
		Contact(3, 1000, 22, 22),  # Contact with the target
		Contact(1, 3, 15, 20, owlt=1),
		Contact(3, 1, 15, 21, owlt=1),
		Contact(3, 4, 24, 26, owlt=1),
		Contact(4, 3, 25, 26, owlt=1)
	]
	return cp


def create_route_tables(nodes, cp):
	"""
	Route Table creation - Invokes Yen's CGR algorithm to discover routes between
	node-pairs, stores them in a dictionary and updates the route table on each node
	"""
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
	random.seed(0)
	env = simpy.Environment()
	cp = init_contact_plan()
	scheduler = Node(
		SCHEDULER_ID,
		buffer=Buffer(SCHEDULER_BUFFER_CAPACITY),
		contact_plan=cp,
		scheduler=Scheduler(SCHEDULER_ID, contact_plan=cp),
		outbound_queues={x: [] for x in range(1, NUM_NODES + 1)}
	)
	nodes = init_nodes(NUM_NODES, cp)
	create_route_tables(nodes, cp)
	# env.process(bundle_generator(env, nodes, nodes))
	env.process(requests_generator(env, nodes, scheduler))
	for node in [scheduler] + nodes:
		env.process(node.bundle_assignment_controller(env))
		env.process(node.contact_controller(env))  # Generator that initiates contacts
		# TODO Need to add in the generators that do the regular bundle assignment and
		#  route discovery (if applicable)

	env.run(until=30)

	print('')
