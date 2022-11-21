import simpy
import random
from dataclasses import dataclass


TIME_BETWEEN_CONTACTS = [5, 10]
LENGTH_OF_CONTACTS = [3, 5]


# def contact_controller(env, nodes):
# 	next_contact = random.randint(*TIME_BETWEEN_CONTACTS)
# 	yield env.timeout(next_contact)
# 	sender = random.choice(nodes)
# 	receiver = [x for x in nodes if x != sender]


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


class Node:
	def __init__(self, name, uid):
		self.name = name
		self.uid = uid
		self.buffer = list(range(100))
		self.in_contact = []
		self.cp = init_contact_plan()
		self.cp_self = [x for x in self.cp if x.frm == self.uid]
		self.no_more_contacts = False

	def contact_init(self, env):
		while self.cp_self:
			contact = self.cp_self.pop(0)
			time_to_next_contact = contact.start - env.now
			yield env.timeout(time_to_next_contact)
			self.in_contact.append(contact.to)
			print(f"contact started on {self.uid} with {contact.to} at {env.now}")
			env.process(self.contact_ender(env, contact))
			env.process(self.bundle_send(env, contact))

	def contact_ender(self, env, contact):
		contact_duration = contact.end - env.now
		yield env.timeout(contact_duration)
		self.in_contact.remove(contact.to)
		print(f"contact between {self.uid} and {contact.to} ended at {env.now}")

	def bundle_send(self, env, contact):
		while self.buffer:
			bundle = self.buffer.pop()
			send_time = 1 / contact.rate
			print(f"bundle {bundle} sent from {self.uid} to {contact.to} at time {env.now}")
			yield env.timeout(send_time)

			if not self.in_contact:
				break


if __name__ == "__main__":

	random.seed(0)

	env = simpy.Environment()

	nodes = []
	for k in range(4):
		nodes.append(Node(f"node_{k}", k))

	for node in nodes:
		env.process(node.contact_init(env))
		# env.process(node.bundle_send(env, [x.name for x in nodes if x != node][0]))

	env.run(until=60)
