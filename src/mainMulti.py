#!/usr/bin/env python3

import json
from types import SimpleNamespace
import numpy as np
import pickle

from main import main
import misc as _misc

# [valid_pickup, define_pickup, valid_delivery, resource_aware, define_delivery]
schemes = {
	"naive":              [False, False, False, False, False],
	"first":              [True,  True,  False, False, False],
	"cgs_cgr":            [True,  True,  True,  False, False],
	# "cgs_cgr_resource":   [True,  True,  True,  True,  False],
	# "cgs_msr":            [True,  True,  True,  True,  True],
}

# uncertainties = [1.0, 0.9, 0.8, 0.7]
uncertainties = [0.7]

congestions = [.1, .2, .3, .4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.]
# congestions = np.linspace(0.1, 0.9, 3)
# congestions = [0.2, 0.5, 1.0]

filename = "input_files//walker_delta_16.json"
results_file_base = "results//uncertainty//results"
with open(filename, "rb") as read_content:
	inputs = json.load(read_content, object_hook=lambda d: SimpleNamespace(**d))

for con in congestions:
	for scheme_name, scheme in schemes.items():
		for uncertainty in uncertainties:
			_misc.USED_IDS = set()
			inputs.traffic.congestion = con
			inputs.traffic.msr = True if scheme[4] else False
			analytics = main(inputs, scheme, uncertainty)
			filename = f"{scheme_name}_{uncertainty}_{round(con, 1)}"
			with open(f"{results_file_base}_{filename}", "wb") as file:
				pickle.dump(analytics, file)

		# print(f"Actual congestion, after considering rejected requests, was {analytics.traffic_load}")
		#
		# print("*** REQUEST DATA ***")
		# print(f"{analytics.requests_submitted_count} Requests were submitted")
		# print(f"{analytics.requests_failed_count} Requests could not be fulfilled")
		# print(f"{analytics.requests_delivered_count} Requests were delivered\n")
		#
		# print("*** TASK DATA ***")
		# print(f"{analytics.tasks_processed_count} Tasks were created")
		# print(f"{analytics.tasks_acquired_count} Tasks remain in an 'acquired' state")
		# print(f"{analytics.tasks_delivered_count} Tasks were delivered")
		# print(f"{analytics.tasks_failed_count} Tasks were unsuccessful\n")
		#
		# print("*** BUNDLE DATA ***")
		# print(f"{analytics.bundles_acquired_count} Bundles were acquired")
		# print(f"{analytics.bundles_delivered_count} Bundles were delivered")
		# print(f"{analytics.bundles_dropped_count} Bundles were dropped\n")
		#
		# print("*** PERFORMANCE DATA ***")
		# print(f"The average bundle PICKUP latency is {analytics.pickup_latency_ave}")
		# print(f"The bundle PICKUP latency Std. Dev. is {analytics.pickup_latency_stdev}")
		# print(f"The average bundle DELIVERY latency is {analytics.delivery_latency_ave}")
		# print(f"The bundle DELIVERY latency Std. Dev. is {analytics.delivery_latency_stdev}")
		# print(f"The average bundle REQUEST latency is {analytics.request_latency_ave}")
		# print(f"The bundle REQUEST latency Std. Dev. is {analytics.request_latency_stdev}")
		# print(f"The average HOPS PER DELIVERED BUNDLE is {analytics.hop_count_average}")