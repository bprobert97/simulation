import pickle
import matplotlib.pyplot as plt
import numpy as np
import itertools
from statistics import mean, stdev
# import seaborn as sns
# sns.color_palette("pastel")

from misc import my_ceil


congestions = [round(x, 1) for x in np.linspace(0.1, 0.9, 9)]
congestions.extend([round(x, 1) for x in np.linspace(1.0, 2.0, 6)])
# congestions = [0.5]

schemes = {
	"naive": {"colour": "black"},
	"first": {"colour": "blue"},
	"cgs_cgr": {"colour": "red"},
	"cgs_cgr_resource": {"colour": "green"},
	"cgs_msr": {"colour": "orange"}
}

request_latency = {
	"row": 0, "col": 0, "y_label": "Total latency (hrs)", "max": 0, "tick": 1}
task_latency = {
	"row": 0, "col": 1, "y_label": "Pickup latency (hrs)", "max": 0, "tick": 1}
bundle_latency = {
	"row": 0, "col": 2, "y_label": "Delivery latency (hrs)", "max": 0, "tick": 1}

request_ratio = {
	"row": 1, "col": 0, "y_label": "Request ratio", "max": 1, "tick": 0.2}
task_ratio = {
	"row": 1, "col": 1, "y_label": "Pickup ratio", "max": 1, "tick": 0.2}
# delivery_ratio = {
# 	"row": 1, "col": 2, "y_label": "Delivery ratio", "max": 1, "tick": 0.2}
hop_count = {
	"row": 1, "col": 2, "y_label": "Hop count", "max": 0, "tick": 0.5}

requests_accepted = {
	"row": 2, "col": 0, "y_label": "Req. accepted, x1000", "max": 0, "tick": 1}
# requests_rejected = {
# 	"row": 2, "col": 1, "y_label": "Req. rejected, x1000", "max": 0, "tick": 0.5}
requests_failed = {
	"row": 2,  "col": 1, "y_label": "Req. failed, x1000", "max": 0, "tick": 1}
requests_delivered = {
	"row": 2, "col": 2, "y_label": "Req. delivered, x1000", "max": 0, "tick": 1}

metrics = [
	request_latency,
	task_latency,
	bundle_latency,
	request_ratio,
	task_ratio,
	# delivery_ratio,
	hop_count,
	requests_accepted,
	# requests_rejected,
	requests_failed,
	requests_delivered,
]

for metric in metrics:
	for scheme in schemes:
		metric[scheme] = []

for scheme, con in itertools.product(schemes, congestions):
	filename = f"results//results_{scheme}_{con}"
	results = pickle.load(open(filename, "rb"))

	request_latency[scheme].append(mean(results.request_latencies) / 3600)
	task_latency[scheme].append(mean(results.pickup_latencies_delivered) / 3600)
	bundle_latency[scheme].append(mean(results.delivery_latencies) / 3600)

	request_ratio[scheme].append(results.request_delivery_ratio)
	task_ratio[scheme].append(results.task_delivery_ratio)
	# delivery_ratio[scheme].append(results.bundle_delivery_ratio)
	hop_count[scheme].append(results.hop_count_average_delivered)

	requests_accepted[scheme].append(results.tasks_processed_count / 1000)
	# requests_rejected[scheme].append(results.requests_rejected_count / 1000)
	requests_failed[scheme].append(results.requests_failed_count / 1000)
	requests_delivered[scheme].append(results.requests_delivered_count / 1000)
	# Hop count

	request_latency["max"] = max(request_latency["max"], request_latency[scheme][-1])
	task_latency["max"] = max(task_latency["max"], task_latency[scheme][-1])
	bundle_latency["max"] = max(bundle_latency["max"], bundle_latency[scheme][-1])

	request_ratio["max"] = max(request_ratio["max"], request_ratio[scheme][-1])
	task_ratio["max"] = max(task_ratio["max"], task_ratio[scheme][-1])
	# delivery_ratio["max"] = max(delivery_ratio["max"], delivery_ratio[scheme][-1])
	hop_count["max"] = max(hop_count["max"], hop_count[scheme][-1])

	requests_accepted["max"] = max(requests_accepted["max"], requests_accepted[scheme][-1])
	# requests_rejected["max"] = max(requests_rejected["max"], requests_rejected[scheme][-1])
	requests_failed["max"] = max(requests_failed["max"], requests_failed[scheme][-1])
	requests_delivered["max"] = max(requests_delivered["max"], requests_delivered[scheme][-1])

# plt.rcParams['axes.grid'] = True
plt.style.use('_mpl-gallery')
fig, ax = plt.subplots(3, 3)
plt.subplots_adjust(left=0.05, bottom=0.07, right=0.96, top=0.93)

for scheme, props in schemes.items():
	for metric in metrics:
		ax[metric["row"], metric["col"]].plot(
			congestions,
			metric[scheme],
			linewidth=2,
			color=props["colour"],
		)

		# Pick-up latency, from Request to Pickup
		ax[metric["row"], metric["col"]].set(
			xlim=(0, 2), xticks=np.arange(0, 2.5, 0.5),
			ylim=(0, metric["max"]),
			yticks=np.arange(0, metric["max"] + metric["tick"], metric["tick"])
		)

		ax[metric["row"], metric["col"]].set_ylabel(metric["y_label"])

ax[2, 0].set_xlabel("Congestion")
ax[2, 1].set_xlabel("Congestion")
ax[2, 2].set_xlabel("Congestion")

# Add a legend at the top of the figure
fig.legend(
	ax[0, 0].lines,
	["Naive", "First", "CGS (PU)", "CGS (CGR)", "CGS (MSR)"],
	loc='upper center',
	ncol=len(schemes)
)

# for a in ax.flat:
# 	a.label_outer()

plt.show()
