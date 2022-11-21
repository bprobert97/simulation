#!/usr/bin/env python3


from dataclasses import dataclass, field
from typing import List, Dict
from collections import deque

from scheduling import Scheduler
from routing import cgr_yens


@dataclass
class Node:
    """
    A Node object is a network element that can participate, in some way, to the data
    scheduling, generation, routing and/or delivery process.
    """
    uid: int
    scheduler: Scheduler = None
    buffer: deque = None
    route_table: Dict = field(default_factory=lambda: {})

    def contact_start(self, n, t_now):
        """
        Method to mark the initialisation of a contact with another node (n) at time
        "t_now"
        """
        pass

    def contact_end(self, n, t_now):
        """
        Method to mark the conclusion of a contact with another node (n) at time
        "t_now"
        """
        pass

    def extract_bundle_to_send(self, n, t_now):
        pass

    def bundle_send(self, b, n, t_now):
        """
        Send bundle b to node n at time t_now

        This process involves transmitting the bundle, at the transmission data rate.
        In addition to this, if more bundles are awaiting transmission, a new bundle
        send process is added to the event queue
        """
        pass

    def bundle_receive(self, b, t_now):
        """
        Begin to receive bundle b

        If the bundle is too large to be accommodated, reject, else accept
        """
        pass
