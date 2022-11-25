import datetime
import unittest
from math import radians, sqrt, acos, pi

import pymap3d

from src.spaceNetwork import Spacecraft, GroundNode, Orbit
from src.spaceMobility import review_contacts


class SpaceNetworkTest(unittest.TestCase):
	"""
	Tests that ensure the space network elements (satellites and gateways/targets) are
	built correctly, their mobility propagates as expected, and the relationship
	between them is logical.
	"""
	def test_satellite_orbit_period(self):
		s = Spacecraft()
		coe0 = [7136635.81, 0., radians(90.), 0., 0., 0.]
		s.orbit = Orbit(0.0, coe0)

		# Test that this orbit is ~100 mins long
		self.assertAlmostEqual(s.orbit.period, 100*60, 1)

		s.orbit.propagate_orbit(s.orbit.period, 1)

		# Test that the maximum z-direction vector is within 10km of the SMA
		self.assertTrue(sqrt((s.orbit.eci.max(axis=0)[2] - coe0[0])**2), 10000)
		self.assertTrue(sqrt((s.orbit.eci.min(axis=0)[2] + coe0[0]) ** 2), 10000)

	def test_ground_node_mobility(self):
		jd0 = 2459659.
		g = GroundNode(0, 0., 0., 0., 0.)

		g.eci_coords(jd0, 86164, 1)
		diff = [abs(j - i) for i, j in zip(g.eci[0], g.eci[-1])]
		# d0 = datetime.datetime(2022, 3, 20, 12)
		# eci_from_pymap3d = geodetic2eci(0., 0., 0., d0, [x for x in range(86164)])

		# Test that the maximum difference between the initial and final coordinates (
		# in ECI frame), for a point on the ground, after approx. one siderial day,
		# is less than 1km.
		self.assertTrue(max(diff) < 1000)

		print('')

	def test_space_to_ground_contact_schedule(self):
		"""
		Demonstrate that a satellites makes contact with ground locations, and each
		other as expected

		A satellite in a polar orbit, with a period of 100 minutes, should exhibit one
		contact with the Northern ground station (for a duration 893s) at time 1054 - 1947
		and a contact with a Southern target at time 4496. The other satellite,
		in an equatorial orbit, makes contact with the polar one for half a contact at
		the beginning of the simulation (for 95s) and another halfway through, at 2901
		for 189s.
		"""
		jd0 = 0
		id_counter = 0

		s1 = Spacecraft(id_counter)
		coe0_1 = [7136635.81, 0., 0., 0., 0., 0.]
		s1.orbit = Orbit(0.0, coe0_1)
		s1.orbit.propagate_orbit(int(s1.orbit.period), 1)
		id_counter += 1

		s2 = Spacecraft(id_counter)
		coe0_2 = [7136635.81, 0., radians(90.), 0., 0., 0.]
		s2.orbit = Orbit(0.0, coe0_2)
		s2.orbit.propagate_orbit(int(s2.orbit.period), 1)

		satellites = {s1.uid: s1, s2.uid: s2}

		id_counter += 1
		g_north = GroundNode(id_counter, 90., 0., 0., 0.)
		g_north.eci_coords(jd0, int(s1.orbit.period), 1)

		id_counter += 1
		t_south = GroundNode(id_counter, -90., 0., 0., 0., True)
		t_south.eci_coords(jd0, int(s1.orbit.period), 1)

		times = [x for x in range(int(s1.orbit.period))]
		gateways = {g_north.uid: g_north}
		targets = {t_south.uid: t_south}

		# Calculate the pass duration, given we have a min elevation angle of 0
		pass_duration = int(s1.orbit.period * 2 * acos(6371000/7136635.81) / (2 * pi))

		cp = review_contacts(
			times,
			{**satellites, **gateways, **targets},
			satellites,
			gateways,
			targets
		)

		self.assertEqual(cp[0].start, 0)
		self.assertEqual(cp[1].start, 0)
		self.assertEqual(cp[0].end, 95)
		self.assertEqual(cp[1].end, 95)
		self.assertEqual(cp[2].start, 1054)
		self.assertEqual(cp[3].start, 1054)
		self.assertEqual(cp[2].end, 1947)
		self.assertEqual(cp[3].end, 1947)
		self.assertEqual(cp[4].start, 2901)
		self.assertEqual(cp[5].start, 2901)
		self.assertEqual(cp[4].end, 3090)
		self.assertEqual(cp[5].end, 3090)
		self.assertEqual(cp[6].start, 4496)
		self.assertEqual(cp[6].end, 4496)


def geodetic2eci(lat, lon, alt, t0, times):
	eci = []
	for t in times:
		date = t0 + datetime.timedelta(seconds=t)
		eci_location = pymap3d.geodetic2eci(lat, lon, alt, date)
		eci.append([
			eci_location[0][0],
			eci_location[1][0],
			eci_location[2][0]
		])
	return eci


if __name__ == '__main__':
	unittest.main()
