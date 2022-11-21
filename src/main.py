#!/usr/bin/env python3
import itertools
from queue import PriorityQueue
from copy import deepcopy

from node import Node
from routing import Contact, cgr_yens


class Simulation:
	"""
	The main discrete-event simulation controller, which maintains a queue of events
	that are awaiting completion, ordered by time.
	"""
	def __init__(self):
		self.now = 0
		self.event_queue = PriorityQueue()
		self.item_counter = -1

	def run(self):
		while not self.event_queue.empty():
			self.next_event()

	def add_event(self, time_start: int, *args):
		"""
		Method to add events to the simulation queue
		"""
		self.item_counter += 1
		process_tuple = (time_start, self.item_counter)
		self.event_queue.put(process_tuple)

	def next_event(self):
		"""
		Extract the next event from the queue (i.e. the one that is soonest to occur),
		update the time to the moment it starts and execute the appropriate function
		"""
		event = self.event_queue.get()
		self.now = event[0]

		# Invoke the bound method for this event, passing in the data
		event[2](event[3])


def contact_generator(sim, cp, nodes):
	while cp:
		contact = cp.pop(0)
		yield sim.add_event(contact.start - sim.now)
		nodes[contact.frm].contact_start(contact.to, sim.now)


def init_nodes():
	return {n.uid: n for n in [Node(x) for x in range(4)]}


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
	nodes = init_nodes()
	cp = init_contact_plan()
	create_route_tables(nodes, cp)

	sim = Simulation()

	contact_generator(sim, cp, nodes)

	# for contact in cp:
	# 	sim.add_event(contact.start, nodes[contact.frm].contact_end, contact)
	# 	sim.add_event(contact.end, nodes[contact.frm].contact_end, contact)

	sim.run()

	print('')
