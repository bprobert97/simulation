#!/usr/bin/env python3

import sys
from dataclasses import dataclass, field
from typing import List


@dataclass
class Buffer:
	"""
	Container for bundles

	Arguments:
		capacity (int): Maximum volume of data that can be stored
	"""
	capacity: int = sys.maxsize

	def __post_init__(self):
		self.bundles = []

	@property
	def min_bundle_size(self):
		return min([b.size for b in self.bundles]) if self.bundles else None

	@property
	def capacity_remaining(self):
		return self.capacity - sum([b.size for b in self.bundles])

	def append(self, bundle):
		"""
		Add a bundle to the buffer. If the bundle cannot be added, return False
		"""
		if self.capacity_remaining >= bundle.size:
			self.bundles.append(bundle)
			return True
		return False

	def extract(self):
		"""
		Remove bundles from the front of the list (i.e. FIFO scheme)
		"""
		return self.bundles.pop(0) if self.bundles else None

	def is_empty(self):
		return True if not self.bundles else False


@dataclass
class Bundle:
	"""Bundle class, following the format as specified in the Bundle Protocol

	Args:
		src: ID of the node on which the bundle was generated initially
		dst: Endpoint ID to which the bundle must be delivered (the "destination")
		target_id: If applicable, ID of the target to which the bundle relates
		created_at: Time at which the bundle was created
		size: Nominal size of the bundle payload data
		lifetime: Absolute time after which the bundle has zero value
		priority: Level of priority, higher value = higher priority. 0 = "Bulk",
			1 = normal, 2 = expedited
		critical: If True, the bundle is of "critical" type
		fragment: If True, the bundle must not be fragmented
		previous_node: ID of the last node to forward (transmit) the bundle
		hop_count: Number of contacts over which the bundle has been forwarded
		_age: Age of the bundle immediately prior to the most recent forwarding event
		_is_fragment: If True, indicates that the bundle is a fragment of its original
	"""
	src: int
	dst: int
	target_id: int = None
	created_at: int = 0
	size: int = 1
	lifetime: int = 1000
	priority: int = 0  #
	critical: bool = False
	fragment: bool = True
	task_id: int = None
	previous_node: int = field(init=False, default=None)
	hop_count: int = field(init=False, default=0)
	_route: List = field(init=False, default_factory=list)
	_age: int = field(init=False, default=0)
	_is_fragment: bool = field(init=False, default=False)

	def __post_init__(self) -> None:
		self.evc = max(self.size * 1.03, 100)

	@property
	def age(self):
		return self._age

	@property
	def route(self) -> list:
		"""The path along which the bundle has been assigned.

		Each item in the list is a Contact ID
		"""
		return self._route

	@route.setter
	def route(self, hops: list) -> None:
		self._route = hops

	def update_age(self, t_now):
		self._age = t_now - self.created_at

	def __repr__(self):
		return "Bundle: Created at %d | By %d | From %d | Going to %d" % (
			self.created_at,
			self.src,
			self.target_id,
			self.dst
		)
