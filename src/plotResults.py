import pickle
import matplotlib.pyplot as plt
import numpy as np
import itertools

if __name__ == "__main__":
	latency_full_mean = []
	latency_full_stdv = []
	latency_del_mean = []
	latency_del_stdv = []
	latency_task_mean = []
	delivery_ratio = []
	congestions = [0.1, 0.3, 0.5, 0.7, 0.9]
	schemes = ["naive", "first", "cgs_cgr", "cgs_cgr_resource", "cgs_msr"]
	for scheme, con in itertools.product(schemes, congestions):
		filename = f"results//results_{scheme}_{con}"
		results = pickle.load(open(filename, "rb"))
		latency_full_mean.append(results.request_latency_ave)
		latency_full_stdv.append(results.request_latency_stdev)
		latency_del_mean.append(results.delivery_latency_ave)
		latency_del_stdv.append(results.delivery_latency_stdev)
		latency_task_mean.append(results.pickup_latency_ave)
		delivery_ratio.append(results.bundle_delivery_ratio)

	plt.style.use('_mpl-gallery')

	x = np.array(congestions)
	y_request = np.array(latency_full_mean)
	y_request_upper = np.array([x[0] + x[1] for x in zip(latency_full_mean, latency_full_stdv)])
	y_request_lower = np.array([x[0] - x[1] for x in zip(latency_full_mean, latency_full_stdv)])

	y_task = np.array(latency_task_mean)

	y_delivery = np.array(latency_del_mean)
	y_delivery_upper = np.array([x[0] + x[1] for x in zip(latency_del_mean, latency_del_stdv)])
	y_delivery_lower = np.array([x[0] - x[1] for x in zip(latency_del_mean, latency_del_stdv)])

	# np.random.seed(1)
	# x = np.linspace(0, 8, 16)
	# y1 = 3 + 4 * x / 8 + np.random.uniform(0.0, 0.5, len(x))
	# y2 = 1 + 2 * x / 8 + np.random.uniform(0.0, 0.5, len(x))

	# Latency plot
	fig, ax = plt.subplots(2, 2)
	# fig.suptitle('Performance vs. traffic load')

	ax[0, 0].plot(x, y_request, linewidth=2)
	# ax[0, 0].fill_between(x, y_request_upper, y_request_lower, alpha=.25, linewidth=0)

	ax[0, 1].plot(x, y_task, linewidth=2, color="green")
	# ax[0, 0].fill_between(x, y_delivery_upper, y_delivery_lower, alpha=.25, linewidth=0, color="green")

	ax[1, 0].plot(x, y_delivery, linewidth=2, color="orange")

	ax[1, 1].plot(x, np.array(delivery_ratio), linewidth=2, color="purple")

	ax[0, 0].set(
		xlim=(0, 1), xticks=np.arange(0, 1.1, 0.1),
		ylim=(0, 12000), yticks=np.arange(0, 13000, 2000)
	)

	ax[0, 1].set(
		xlim=(0, 1), xticks=np.arange(0, 1.1, 0.1),
		ylim=(0, 8000), yticks=np.arange(0, 9000, 1000)
	)

	ax[1, 0].set(
		xlim=(0, 1), xticks=np.arange(0, 1.1, 0.1),
		ylim=(0, 4000), yticks=np.arange(0, 5000, 1000)
	)

	ax[1, 1].set(
		xlim=(0, 1), xticks=np.arange(0, 1.1, 0.1),
		ylim=(0, 1), yticks=np.arange(0, 1.1, 0.2)
	)

	ax[1, 0].set_xlabel("Congestion")
	ax[1, 1].set_xlabel("Congestion")
	ax[0, 0].set_ylabel("Total latency (s)")
	ax[0, 1].set_ylabel("Task latency (s)")
	ax[1, 0].set_ylabel("Bundle latency (s)")
	ax[1, 1].set_ylabel("Delivery ratio")

	# for a in ax.flat:
	# 	a.label_outer()

	plt.show()
