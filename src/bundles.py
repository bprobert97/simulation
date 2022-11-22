#!/usr/bin/env python3

import sys
from dataclasses import dataclass
from collections import deque


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
	"""
	Bundle class, following the format as specified in the Bundle Protocol
	"""
	src: int
	dst: int
	size: int = 1
	deadline: int = 1000
	priority: int = 0
	critical: bool = False
	custody: bool = False
	fragment: bool = True
	sender: int = 0
	created_at: int = 0

	def __post_init__(self):
		self.evc = max(self.size * 1.03, 100)

	def __repr__(self):
		return "Bundle: Destination %d | Deadline %d" % (
			self.dst,
			self.deadline
		)