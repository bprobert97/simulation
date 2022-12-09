#!/usr/bin/env python3

import itertools
import random
import sys
import json
import cProfile

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


SCHEDULER_BUFFER_CAPACITY = 1000
NUM_NODES = 4
NODE_BUFFER_CAPACITY = 100000
NUM_BUNDLES = [5, 10]
BUNDLE_ARRIVAL_RATE = 0.2  # Mean number of bundles to be generated per unit time
BUNDLE_TTL = 25  # Time to live for a
CONGESTION = 0.5
TARGET_UID = 999

SCHEDULER_ID = 0
TARGET_ID_BASE = 3000
SATELLITE_ID_BASE = 2000
GATEWAY_ID_BASE = 1000


def get_request_inter_arrival_time(sim_time, outflow, congestion, size) -> int:
	"""Returns the mean time between request arrivals based on congestion target.

	Given a certain amount of delivery capacity (i.e. the long-term average rate of
	delivery per unit time), some target level of congestion in the network (ratio of
	inflow to outflow) and the size of each bundle (i.e. the package generated in
	response to a request), return the mean time to wait between request arrivals.
	"""
	return (sim_time * size) / (outflow * congestion)


def requests_generator(env, sources, sinks, moc, inter_arrival_time, size, priority, ttl):
	"""
	Generate requests that get submitted to a scheduler where they are processed into
	tasks, added to a task table, and distributed through the network for execution by
	nodes.
	"""
	while True:
		yield env.timeout(random.expovariate(1 / inter_arrival_time))
		source = random.choice([s for s in sources.values()])
		request = Request(
			source.uid,
			destination=random.choice(sinks),  # random.choice(nodes),
			data_volume=size,
			priority=priority,
			bundle_lifetime=ttl,
			time_created=env.now,
		)
		moc.request_received(request, env.now)
		pub.sendMessage("request_submit", r=request)


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
			deadline=deadline, created_at=env.now)
		source.buffer.append(b)
		pub.sendMessage("bundle_acquired", b=b)


def init_space_nodes(nodes, targets, cp, cpwt):
	node_ids = [x for x in nodes]
	node_list = []
	for n_uid, n in nodes.items():
		n = Node(
			n_uid,
			buffer=Buffer(NODE_BUFFER_CAPACITY),
			outbound_queues={x: [] for x in node_ids},
			contact_plan=deepcopy(cp),
			contact_plan_targets=deepcopy(cpwt)
		)
		n._targets = targets
		pub.subscribe(n.bundle_receive, str(n_uid) + "bundle")
		node_list.append(n)
	return node_list


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
			# TODO Does this need to be a deepcopy, else we'll be pointing at the same
			#  contacts across different nodes...
			node.route_table[other.uid] = [] if not routes else routes


def init_analytics():
	a = Analytics()

	pub.subscribe(a.submit_request, "request_submit")
	pub.subscribe(a.fail_request, "request_fail")
	pub.subscribe(a.duplicated_request, "request_duplicated")

	pub.subscribe(a.add_task, "task_add")
	pub.subscribe(a.redundant_task, "task_redundant")  # TODO
	pub.subscribe(a.fail_task, "task_failed")  # TODO
	pub.subscribe(a.renew_task, "task_renew")  # TODO

	pub.subscribe(a.add_bundle, "bundle_acquired")
	pub.subscribe(a.deliver_bundle, "bundle_delivered")
	pub.subscribe(a.forward_bundle, "bundle_forwarded")
	pub.subscribe(a.drop_bundle, "bundle_dropped")
	pub.subscribe(a.reroute_bundle, "bundle_reroute")  # TODO

	return a


def init_space_network(epoch, duration, step_size, targets_, satellites_, gateways_):
	targets = setup_ground_nodes(
		epoch,
		duration,
		step_size,
		targets_,
		is_source=True,
		id_counter=TARGET_ID_BASE
	)

	satellites = setup_satellites(
		epoch,
		duration,
		step_size,
		satellites_,
		counter=SATELLITE_ID_BASE
	)

	gateways = setup_ground_nodes(
		epoch,
		duration,
		step_size,
		gateways_,
		id_counter=GATEWAY_ID_BASE
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

	# ****************** SPACE NETWORK SETUP ******************
	# set up the space network nodes (satellites and gateways, and if known in advance,
	# the targets)
	filename = "input_files//sim_polar_simple.json"
	with open(filename, "r") as read_content:
		inputs = json.load(read_content)

	sim_epoch = inputs["simulation"]["date_start"]
	sim_duration = inputs["simulation"]["duration"]
	sim_step_size = inputs["simulation"]["step_size"]
	times = [x for x in range(0, sim_duration, sim_step_size)]
	# FIXME This won't work if we have multiple types of bundles with different sizes
	bundle_size = inputs["bundles"]["size"]

	targets, satellites, gateways = init_space_network(
		sim_epoch, sim_duration, sim_step_size, inputs["targets"], inputs["satellites"],
		inputs["gateways"]
	)

	# Get Contact Plan from the relative mobility between satellites, targets (sources)
	# and gateways (sinks)
	cp = review_contacts(
		times,
		{**satellites, **targets, **gateways},
		satellites,
		gateways,
		targets
	)

	# ****************** SCHEDULING SPECIFIC PREPARATION ******************
	# Create a contact plan that ONLY has contacts with target nodes and a contact plan
	# that ONLY has contacts NOT with target nodes. The target CP will be used to
	# extend the non-target one during request processing, but since target nodes don't
	# participate in routing, they slow down the route discovery process if considered.
	cp_with_targets = [c for c in cp if c.to in [t for t in targets]]
	cp = [c for c in cp if c.to not in [t for t in targets]]

	# Instantiate the Mission Operations Center, i.e. the Node at which requests arrive
	# and then set up each of the remote nodes (including both satellites and gateways).
	moc = Node(
		SCHEDULER_ID,
		buffer=Buffer(SCHEDULER_BUFFER_CAPACITY),
		contact_plan=cp,
		contact_plan_targets=cp_with_targets,
		scheduler=Scheduler(),
		outbound_queues={x: [] for x in {**satellites,  **gateways}}
	)
	moc.scheduler.parent = moc

	# Add a permanent contact between the MOC and the Gateways so that they can always
	# be up-to-date in terms of the Task Table
	for g in gateways:
		cp.insert(0, Contact(moc.uid, g, 0, sim_duration, sys.maxsize))

	download_capacity = get_download_capacity(
		cp,
		[*gateways],
		[*satellites]
	)

	request_arrival_wait_time = get_request_inter_arrival_time(
			sim_duration,
			download_capacity,
			CONGESTION,
			bundle_size
		)

	nodes = init_space_nodes(
		{**satellites,  **gateways}, [*targets], cp, cp_with_targets)

	create_route_tables(nodes, cp)

	# Initiate the simpy environment, which keeps track of the event queue and triggers
	# the next discrete event to take place
	env = simpy.Environment()
	env.process(requests_generator(
		env,
		targets,
		[x for x in gateways],
		moc,
		request_arrival_wait_time,
		bundle_size,
		inputs["bundles"]["priority"],
		inputs["bundles"]["lifetime"]
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
	env.run(until=sim_duration)
	# cProfile.run('env.run(until=sim_duration)')

	print("*** REQUEST DATA ***")
	print(f"{analytics.requests_submitted} Requests were submitted")
	print(f"{analytics.requests_failed} Requests could not be fulfilled")
	print(f"{analytics.requests_duplicated} Requests already handled by existing tasks\n")
	print("*** TASK DATA ***")
	print(f"{analytics.tasks_processed} Tasks were created")
	print(f"{analytics.tasks_failed} Tasks were unsuccessful\n")
	print("*** BUNDLE DATA ***")
	print(f"{analytics.bundles_acquired} Bundles were acquired")
	print(f"{analytics.bundles_forwarded} Bundles were forwarded")
	print(f"{analytics.bundles_delivered} Bundles were delivered")
	print(f"{analytics.bundles_dropped} Bundles were dropped")

	print('')
