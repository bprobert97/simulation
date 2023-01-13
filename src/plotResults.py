import pickle
import matplotlib.pyplot as plt
import numpy as np
import itertools
from statistics import mean, stdev
from misc import my_ceil


congestions = [round(x, 1) for x in np.linspace(0.1, 0.9, 9)]
# congestions = [0.2, 0.6, 1.0]

schemes = {
	# "naive": {"colour": "black"},
	"first": {"colour": "blue"},
	"cgs_cgr": {"colour": "red"},
	"cgs_cgr_resource": {"colour": "green"},
	"cgs_msr": {"colour": "orange"}
}

request_latency = {"row": 0, "col": 0, "y_label": "Request latency", "max": 0, "tick": 1000}
task_latency = {"row": 0, "col": 1, "y_label": "Pickup latency", "max": 0, "tick": 1000}
bundle_latency = {"row": 0, "col": 2, "y_label": "Bundle latency", "max": 0, "tick": 1000}

request_ratio = {"row": 1, "col": 0, "y_label": "Request ratio", "max": 1, "tick": 0.1}
task_ratio = {"row": 1, "col": 1, "y_label": "Delivery ratio", "max": 1, "tick": 0.1}
delivery_ratio = {"row": 1, "col": 2, "y_label": "Delivery ratio", "max": 1, "tick": 0.1}

requests_accepted = {"row": 2, "col": 0, "y_label": "No. requests accepted", "max": 0, "tick": 100}
requests_rejected = {"row": 2, "col": 1, "y_label": "No. requests rejected", "max": 0, "tick": 100}
requests_delivered = {"row": 2, "col": 2, "y_label": "No. requests delivered", "max": 0, "tick": 100}

metrics = [
	request_latency,
	task_latency,
	bundle_latency,
	request_ratio,
	task_ratio,
	delivery_ratio,
	requests_accepted,
	requests_rejected,
	requests_delivered,
]

for metric in metrics:
	for scheme in schemes:
		metric[scheme] = []

for scheme, con in itertools.product(schemes, congestions):
	filename = f"results//results_{scheme}_{con}"
	results = pickle.load(open(filename, "rb"))

	request_latency[scheme].append(mean(results.request_latencies))
	task_latency[scheme].append(mean(results.pickup_latencies))
	bundle_latency[scheme].append(mean(results.delivery_latencies))

	request_ratio[scheme].append(results.request_delivery_ratio)
	task_ratio[scheme].append(results.task_delivery_ratio)
	delivery_ratio[scheme].append(results.bundle_delivery_ratio)

	requests_accepted[scheme].append(results.tasks_processed_count)
	requests_rejected[scheme].append(results.requests_rejected_count)
	requests_delivered[scheme].append(results.requests_delivered_count)
	# Hop count

	request_latency["max"] = max(request_latency["max"], mean(results.request_latencies))
	task_latency["max"] = max(task_latency["max"], mean(results.pickup_latencies))
	bundle_latency["max"] = max(bundle_latency["max"], mean(results.delivery_latencies))

	request_ratio["max"] = max(request_ratio["max"], results.request_delivery_ratio)
	task_ratio["max"] = max(task_ratio["max"], results.task_delivery_ratio)
	delivery_ratio["max"] = max(delivery_ratio["max"], results.bundle_delivery_ratio)

	requests_accepted["max"] = max(requests_accepted["max"], results.tasks_processed_count)
	requests_rejected["max"] = max(requests_rejected["max"], results.requests_rejected_count)
	requests_delivered["max"] = max(requests_delivered["max"], results.requests_delivered_count)

plt.style.use('_mpl-gallery')
fig, ax = plt.subplots(3, 3)

for scheme, props in schemes.items():
	for metric in metrics:
		ax[metric["row"], metric["col"]].plot(
			congestions, metric[scheme], linewidth=1, color=props["colour"]
		)
		# Pick-up latency, from Request to Pickup
		ax[metric["row"], metric["col"]].set(
			xlim=(0, 1), xticks=np.arange(0, 1.1, 0.1),
			ylim=(0, metric["max"]),
			yticks=np.arange(0, metric["max"] + metric["tick"], metric["tick"])
		)

		ax[metric["row"], metric["col"]].set_ylabel(metric["y_label"])

ax[2, 0].set_xlabel("Congestion")
ax[2, 1].set_xlabel("Congestion")
ax[2, 2].set_xlabel("Congestion")

# for a in ax.flat:
# 	a.label_outer()

plt.show()
