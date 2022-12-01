#!/usr/bin/env python3

import itertools
import random
import sys
import json

from queue import PriorityQueue
from copy import deepcopy
import simpy
from pubsub import pub

from node import Node
from routing import Contact, cgr_yens
from scheduling import Scheduler, Request
from bundles import Buffer, Bundle
from spaceNetwork import setup_satellites, setup_ground_nodes
from spaceMobility import review_contacts
from analytics import Analytics


SCHEDULER_ID = 0
SCHEDULER_BUFFER_CAPACITY = 1000
NUM_NODES = 4
NODE_BUFFER_CAPACITY = 100
NUM_BUNDLES = [5, 10]
REQUEST_ARRIVAL_WAIT = 500  # average waiting time between requests (s)
BUNDLE_SIZE = [1, 3]
BUNDLE_ARRIVAL_RATE = 0.2  # Mean number of bundles to be generated per unit time
BUNDLE_TTL = 25  # Time to live for a
CONGESTION = 0.5


def request_inter_arrival_time_from_congestion(
		sim_time, outflow, congestion, bundle_size) -> int:
	"""Returns the mean time between request arrivals based on congestion target.

	Given a certain amount of delivery capacity (i.e. the long-term average rate of
	delivery per unit time), some target level of congestion in the network (ratio of
	inflow to outflow) and the size of each bundle (i.e. the package generated in
	response to a request), return the mean time to wait between request arrivals.
	"""
	return (sim_time * bundle_size) / (outflow * congestion)


def requests_generator(env, sources, sinks, moc, inter_arrival_time):
	"""
	Generate requests that get submitted to a scheduler where they are processed into
	tasks, added to a task table, and distributed through the network for execution by
	nodes.
	"""
	while True:
		yield env.timeout(random.expovariate(1 / inter_arrival_time))
		# yield env.timeout(0)
		request = Request(
			random.choice(sources),
			destination=random.choice(sinks),  # random.choice(nodes),
			data_volume=1,  # random.randint(*BUNDLE_SIZE),
			time_created=env.now
		)
		moc.request_received(request, env.now)


def bundle_generator(env, sources, destinations):
	"""
	Process that generates bundles on nodes according to some probability for the
	duration of the simulation
	"""
	while True:
		yield env.timeout(random.expovariate(BUNDLE_ARRIVAL_RATE))
		source = random.choice(sources)
		dests = [x for x in destinations if x.uid != source.uid]
		destination = random.choice(dests)
		size = random.randint(*BUNDLE_SIZE)
		deadline = env.now + BUNDLE_TTL
		print(
			f"bundle generated on node {source.uid} at time {env.now} for destination"
			f" {destination.uid}")
		b = Bundle(
			src=source.uid, dst=destination.uid, target_id=source.uid, size=size,
			lifetime=deadline, created_at=env.now)
		source.buffer.append(b)
		pub.sendMessage("bundle_acquired", b=b)


def init_space_nodes(nodes, targets, cp):
	node_ids = [x for x in nodes]
	node_list = []
	for n_uid, n in nodes.items():
		n = Node(
				n_uid,
				buffer=Buffer(NODE_BUFFER_CAPACITY),
				outbound_queues={x: [] for x in node_ids},
				contact_plan=deepcopy(cp)
		)
		n._targets = targets
		pub.subscribe(n._bundle_receive, str(n_uid) + "bundle")
		node_list.append(n)
	return node_list


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
		pub.subscribe(n._bundle_receive, str(n_uid) + "bundle")

		node_list.append(n)

	return node_list


def init_contact_plan():
	"""
	Create a contact plan. This must include contacts between the Scheduler (UID=0) and
	remote nodes (UID=[0, 999]) and also between remote nodes and the target (UID=1000).
	"""
	cp = [
		Contact(SCHEDULER_ID, 2, 0, 4),
		Contact(1, 2, 5, 10, owlt=1),
		Contact(2, 1, 6, 12, owlt=1),
		Contact(1, 3, 15, 20, owlt=1),
		Contact(3, 1, 15, 21, owlt=1),
		Contact(3, TARGET_UID, 22, 22),  # Contact with the target
		Contact(3, 4, 24, 26, owlt=1),
		Contact(4, 3, 25, 26, owlt=1)
	]

	cp.sort()

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


def init_analytics():
	a = Analytics()
	pub.subscribe(a.add_task, "task_added")
	pub.subscribe(a.add_bundle, "bundle_acquired")
	pub.subscribe(a.deliver_bundle, "bundle_delivered")
	pub.subscribe(a.forward_bundle, "bundle_forwarded")
	pub.subscribe(a.drop_bundle, "bundle_dropped")

	return a


def init_space_network(inputs_):
	targets = setup_ground_nodes(
		inputs_["simulation"]["date_start"],
		inputs_["simulation"]["duration"],
		inputs_["simulation"]["step_size"],
		inputs_["targets"],
		is_source=True,
		id_counter=1000
	)

	satellites = setup_satellites(
		inputs_["simulation"]["date_start"],
		inputs_["simulation"]["duration"],
		inputs_["simulation"]["step_size"],
		inputs_["satellites"],
		counter=2000
	)

	gateways = setup_ground_nodes(
		inputs_["simulation"]["date_start"],
		inputs_["simulation"]["duration"],
		inputs_["simulation"]["step_size"],
		inputs_["gateways"],
		id_counter=3000
	)

	return targets, satellites, gateways


def get_download_capacity(contact_plan, sinks, sats):
	"""Return the total delivery capacity from satellites to gateway nodes

	The total download capacity is the sum of the data transfer capacity from all
	possible download opportunities (i.e. from satellite to gateway)
	"""
	# TODO This does not consider any overlap restrictions that may exist
	total = 0
	for contact in contact_plan:
		if contact.frm in sats and contact.to in sinks:
			total += contact.volume
	return total


if __name__ == "__main__":
	"""
	Contact Graph Scheduling implementation
	
	Requests are submitted to a central Scheduler node, which process requests into Tasks 
	that are distributed through a delay-tolerant network so that nodes can execute 
	pick-ups according to their assignation (i.e. bundle acquisition). Acquired bundles 
	are routed through the network using either CGR or MSR, as specified.
	"""
	random.seed(0)

	# set up the space network nodes (satellites and gateways, and if known in advance,
	# the targets)
	filename = "input_files//sim0.json"
	with open(filename, "r") as read_content:
		inputs = json.load(read_content)
	times = [x for x in range(
		0, inputs["simulation"]["duration"], inputs["simulation"]["step_size"]
	)]
	targets, satellites, gateways = init_space_network(inputs)

	cp = review_contacts(
		times,
		{**satellites, **gateways, **targets},
		satellites,
		gateways,
		targets
	)

	# Add a permanent contact between the MOC and the Gateways so that they can always
	# be up-to-date in terms of the Task Table
	for g in gateways:
		cp.insert(
			0,
			Contact(SCHEDULER_ID, g, 0, inputs["simulation"]["duration"], sys.maxsize)
		)

	download_capacity = get_download_capacity(
		cp,
		[g for g in gateways],
		[s for s in satellites]
	)

	# FIXME This won't work if we have multiple types of target with different sizes
	bundle_size = inputs["targets"]["size"]

	bundle_arrival_wait_time = request_inter_arrival_time_from_congestion(
			inputs["simulation"]["duration"],
			download_capacity,
			CONGESTION,
			bundle_size
		)

	# Instantiate the Mission Operations Center, i.e. the Node at which requests arrive
	# and then set up each of the remote nodes (including both satellites and gateways).
	moc = Node(
		SCHEDULER_ID,
		buffer=Buffer(SCHEDULER_BUFFER_CAPACITY),
		contact_plan=cp,
		scheduler=Scheduler(),
		outbound_queues={x: [] for x in {**satellites,  **gateways}}
	)
	moc.scheduler.parent = moc
	nodes = init_space_nodes({**satellites,  **gateways}, [x for x in targets], cp)
	create_route_tables(nodes, cp)

	# Initiate the simpy environment, which keeps track of the event queue and triggers
	# the next discrete event to take place
	env = simpy.Environment()
	env.process(requests_generator(
		env,
		[x for x in targets],
		[x for x in gateways],
		moc,
		bundle_arrival_wait_time
	))

	# Set up the Simpy Processes on each of the Nodes. These are effectively the
	# generators that iterate continuously throughout the simulation, allowing us to
	# jump ahead to whatever the next event is, be that bundle assignment, handling a
	# contact or discovering more routes downstream
	for node in [moc] + nodes:
		env.process(node.bundle_assignment_controller(env))
		env.process(node.contact_controller(env))  # Generator that initiates contacts
		# TODO Need to add in the generator that does regular route discovery. This
		#  will effectively be something that runs every so often and makes sure we
		#  have a sufficient number of routes in our route tables with enough capacity.
		#  We could actually have something that watches our Route Tables and triggers
		#  the Route Discovery whenever we drop below a certain number of good options

	analytics = init_analytics()
	env.run(until=inputs["simulation"]["duration"])

	print('')
