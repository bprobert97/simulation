import pickle
import matplotlib.pyplot as plt
import numpy as np
import itertools
from statistics import mean, stdev
# import seaborn as sns
# sns.color_palette("pastel")

from misc import my_ceil


def plot_performance_metrics(schemes, uncertainties, congestions, metrics):
	plt.style.use('_mpl-gallery')
	fig, ax = plt.subplots(3, 3)
	plt.subplots_adjust(left=0.05, bottom=0.07, right=0.96, top=0.93)

	for scheme, props_scheme in schemes.items():
		for uncertainty, prop_uncertainty in uncertainties.items():
			for metric in metrics:
				ax[metric["row"], metric["col"]].plot(
					congestions,
					metric[scheme][uncertainty],
					linewidth=2,
					color=props_scheme["colour"],
					linestyle=prop_uncertainty["linestyle"]
				)

				ax[metric["row"], metric["col"]].set(
					xlim=(0, 2),
					xticks=np.arange(0, 2.5, 0.5),
					ylim=(0, metric["max"]),
					yticks=np.arange(0, metric["max"] + metric["tick"], metric["tick"])
				)

				ax[metric["row"], metric["col"]].set_ylabel(metric["y_label"])

				ax[metric["row"], metric["col"]].set_title(metric["label"], loc="left")

	ax[2, 0].set_xlabel("Request submission load (RSL)")
	ax[2, 1].set_xlabel("Request submission load (RSL)")
	ax[2, 2].set_xlabel("Request submission load (RSL)")

	# Add a legend at the top of the figure
	if len(schemes) > 1:
		legend_labels = list(schemes)
	else:
		legend_labels = list(uncertainties)

	fig.legend(
		ax[0, 0].lines,
		legend_labels,
		# ["Naive", "First", "CGS (PU)", "CGS (CGR)", "CGS (MSR)"],
		# ["Reliability = 0.7", "Reliability = 0.8", "Reliability = 0.9", "Reliability = 1.0"],
		loc='upper center',
		ncol=len(legend_labels)
	)

	plt.show()


# TODO Update the base filename to reflect location of the results
filename_base = "results/multi/nominal//results"

# Request submission loads to be included in plots
rsls = [round(x, 1) for x in np.linspace(0.1, 0.9, 9)]
rsls.extend([round(x, 1) for x in np.linspace(1.0, 2.0, 6)])

# TODO: Plot should be either a set of schemes at a single uncertainty, or a single
#  scheme across a set of uncertainties
schemes = {
	"naive": {"colour": "black"},
	"first": {"colour": "blue"},
	"cgs_pu": {"colour": "red"},
	"cgs_cgr": {"colour": "green"},
	"cgs_msr": {"colour": "orange"}
}

uncertainties = {
	# 0.7: {"linestyle": "dotted"},
	# 0.8: {"linestyle": "dashdot"},
	# 0.9: {"linestyle": "dashed"},
	1.0: {"linestyle": "solid"}
}

request_latency = {
	"row": 0, "col": 0, "label": "(a)", "y_label": "Total latency (hrs)", "max": 1, "tick": 1}
task_latency = {
	"row": 0, "col": 1, "label": "(b)", "y_label": "Pickup latency (hrs)", "max": 1, "tick": 1}
bundle_latency = {
	"row": 0, "col": 2, "label": "(c)", "y_label": "Delivery latency (hrs)", "max": 1, "tick": 1}

request_ratio = {
	"row": 1, "col": 0, "label": "(d)", "y_label": "Request ratio", "max": 1, "tick": 0.2}
task_ratio = {
	"row": 1, "col": 1, "label": "(e)", "y_label": "Pickup ratio", "max": 1, "tick": 0.2}
hop_count = {
	"row": 1, "col": 2, "label": "(f)", "y_label": "Hop count", "max": 2, "tick": 0.5}

requests_accepted = {
	"row": 2, "col": 0, "label": "(g)", "y_label": "Req. accepted, x1000", "max": 6, "tick": 1}
requests_failed = {
	"row": 2,  "col": 1, "label": "(h)", "y_label": "Req. failed, x1000", "max": 4, "tick": 1}
requests_delivered = {
	"row": 2, "col": 2, "label": "(i)", "y_label": "Req. delivered, x1000", "max": 3, "tick": 1}

metrics = [
	request_latency,
	task_latency,
	bundle_latency,
	request_ratio,
	task_ratio,
	hop_count,
	requests_accepted,
	requests_failed,
	requests_delivered,
]

# Predefine an empty List for each metric:scheme:uncertainty combination. This will
# store the results for each of the request submission loads
for metric in metrics:
	for scheme in schemes:
		metric[scheme] = {}
		for uncertainty in uncertainties:
			metric[scheme][uncertainty] = []

# Load each results file, one by one, and extract the necessary metrics for plotting
for scheme, uncertainty, rsl in itertools.product(schemes, uncertainties, rsls):
	filename = f"{filename_base}_{scheme}_{uncertainty}_{rsl}"
	results = pickle.load(open(filename, "rb"))

	request_latency[scheme][uncertainty].append(mean(results.request_latencies) / 3600)
	task_latency[scheme][uncertainty].append(mean(results.pickup_latencies_delivered) / 3600)
	bundle_latency[scheme][uncertainty].append(mean(results.delivery_latencies) / 3600)

	request_ratio[scheme][uncertainty].append(results.request_delivery_ratio)
	task_ratio[scheme][uncertainty].append(results.task_delivery_ratio)
	hop_count[scheme][uncertainty].append(results.hop_count_average_delivered)

	requests_accepted[scheme][uncertainty].append(results.tasks_processed_count / 1000)
	requests_failed[scheme][uncertainty].append(results.requests_failed_count / 1000)
	requests_delivered[scheme][uncertainty].append(results.requests_delivered_count / 1000)

	request_latency["max"] = max(request_latency["max"], request_latency[scheme][uncertainty][-1])
	task_latency["max"] = max(task_latency["max"], task_latency[scheme][uncertainty][-1])
	bundle_latency["max"] = max(bundle_latency["max"], bundle_latency[scheme][uncertainty][-1])

	request_ratio["max"] = max(request_ratio["max"], request_ratio[scheme][uncertainty][-1])
	task_ratio["max"] = max(task_ratio["max"], task_ratio[scheme][uncertainty][-1])
	hop_count["max"] = max(hop_count["max"], hop_count[scheme][uncertainty][-1])

	requests_accepted["max"] = max(requests_accepted["max"], requests_accepted[scheme][uncertainty][-1])
	requests_failed["max"] = max(requests_failed["max"], requests_failed[scheme][uncertainty][-1])
	requests_delivered["max"] = max(requests_delivered["max"], requests_delivered[scheme][uncertainty][-1])

plot_performance_metrics(schemes, uncertainties, rsls, metrics)
