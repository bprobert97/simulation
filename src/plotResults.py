import pickle
import matplotlib.pyplot as plt
import numpy as np

if __name__ == "__main__":
	latency_full_mean = []
	latency_full_stdv = []
	latency_del_mean = []
	latency_del_stdv = []
	delivery_ratio = []
	congestions = [0.1, 0.3, 0.5, 0.7, 0.9]
	for con in congestions:
		filename = f"results//results_{con}"
		results = pickle.load(open(filename, "rb"))
		latency_full_mean.append(results.request_latency_ave)
		latency_full_stdv.append(results.request_latency_stdev)
		latency_del_mean.append(results.delivery_latency_ave)
		latency_del_stdv.append(results.delivery_latency_stdev)
		delivery_ratio.append(results.delivery_ratio)

	plt.style.use('_mpl-gallery')

	x = np.array(congestions)
	y1_request = np.array([x[0] + x[1] for x in zip(latency_full_mean, latency_full_stdv)])
	y2_request = np.array([x[0] - x[1] for x in zip(latency_full_mean, latency_full_stdv)])
	y1_delivery = np.array([x[0] + x[1] for x in zip(latency_del_mean, latency_del_stdv)])
	y2_delivery = np.array([x[0] - x[1] for x in zip(latency_del_mean, latency_del_stdv)])

	# np.random.seed(1)
	# x = np.linspace(0, 8, 16)
	# y1 = 3 + 4 * x / 8 + np.random.uniform(0.0, 0.5, len(x))
	# y2 = 1 + 2 * x / 8 + np.random.uniform(0.0, 0.5, len(x))

	# Latency plot
	fig, ax = plt.subplots(2)
	fig.suptitle('Performance vs. traffic load')

	ax[0].plot(x, (y1_request + y2_request) / 2, linewidth=2)
	ax[0].fill_between(x, y1_request, y2_request, alpha=.25, linewidth=0)

	ax[0].plot(x, (y1_delivery + y2_delivery) / 2, linewidth=2, color="green")
	ax[0].fill_between(x, y1_delivery, y2_delivery, alpha=.25, linewidth=0, color="green")

	ax[0].set(
		xlim=(0, 1), xticks=np.arange(0, 1.1, 0.1),
		ylim=(0, 7000), yticks=np.arange(0, 8000, 1000)
	)

	ax[1].plot(x, np.array(delivery_ratio), linewidth=2)
	ax[1].set(
		xlim=(0, 1), xticks=np.arange(0, 1.1, 0.1),
		ylim=(0, 1), yticks=np.arange(0, 1.1, 0.2)
	)

	ax[1].set_xlabel("Congestion")
	ax[0].set_ylabel("Latency (s)")
	ax[1].set_ylabel("Delivery ratio")

	for a in ax.flat:
		a.label_outer()

	plt.show()
