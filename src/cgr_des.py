import simpy
import random
from dataclasses import dataclass
from queue import Queue


TIME_BETWEEN_CONTACTS = [5, 10]
LENGTH_OF_CONTACTS = [3, 5]


@dataclass
class Contact:
	frm: int
	to: int
	start: float
	end: float
	rate: float = 1.
	confidence: float = 1.
	owlt: float = 1.


def init_contact_plan():
	return [
		Contact(0, 1, 5, 10),
		Contact(1, 0, 6, 12),
		Contact(0, 2, 15, 20),
		Contact(2, 0, 15, 21),
		Contact(2, 3, 24, 26),
		Contact(3, 2, 25, 26)
	]


def init_buffer():
	buffer = Queue()
	for x in range(8):
		buffer.put(x)
	return buffer


class Node:
	def __init__(self, name, uid):
		self.name = name
		self.uid = uid
		self.buffer = init_buffer()
		self.in_contact = []
		self.cp = init_contact_plan()
		self.cp_self = [x for x in self.cp if x.frm == self.uid]
		self.no_more_contacts = False

	def contact_controller(self, env):
		while self.cp_self:
			contact = self.cp_self.pop(0)
			time_to_next_contact = contact.start - env.now
			yield env.timeout(time_to_next_contact)
			self.in_contact.append(contact.to)
			print(f"contact started on {self.uid} with {contact.to} at {env.now}")
			env.process(self.bundle_send(env, contact))

	def bundle_send(self, env, contact):
		contact_end = contact.end
		while env.now <= contact_end:
			if self.buffer.empty():
				print(f"node {self.uid} exhausted its buffer at {env.now}, waiting...")
				yield env.timeout(2)
				continue
			bundle = self.buffer.get()
			send_time = 1 / contact.rate
			print(f"bundle {bundle} sent from {self.uid} to {contact.to} at time {env.now}")
			yield env.timeout(send_time)
			if contact.to not in self.in_contact:
				break

		print(f"contact between {self.uid} and {contact.to} ended at {env.now}")


if __name__ == "__main__":

	random.seed(0)

	env = simpy.Environment()

	nodes = []
	for k in range(4):
		nodes.append(Node(f"node_{k}", k))

	for node in nodes:
		env.process(node.contact_controller(env))
		# env.process(node.bundle_send(env, [x.name for x in nodes if x != node][0]))

	env.run(until=60)
