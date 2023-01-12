#!/usr/bin/env python3

import json
from types import SimpleNamespace
import numpy as np
import pickle

from main import main

# [define_pickup, valid_delivery, resource_aware, define_delivery]
schemes = {
	"naive": [False, False, False, False, False],
	"first": [True, True, False, False, False],
	"cgs_cgr": [True, True, True, False, False],
	"cgs_cgr_resource": [True, True, True, True, False],
	"cgs_msr": [True, True, True, True, True],
}

congestions = np.linspace(0.1, 0.2, 2)

filename = "input_files//walker_delta_16.json"
with open(filename, "rb") as read_content:
	inputs = json.load(read_content, object_hook=lambda d: SimpleNamespace(**d))

for con in congestions:
	for scheme_name, scheme in schemes.items():

		inputs.traffic.congestion = con
		inputs.traffic.msr = True if scheme[4] else False
		analytics = main(inputs, scheme)
		filename = f"{scheme_name}_{con}"
		with open(f"results//results_{filename}", "wb") as file:
			pickle.dump(analytics, file)
