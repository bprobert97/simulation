#!/usr/bin/env python3
import sys
from dataclasses import dataclass, field
from typing import List, Dict
from collections import deque

from pubsub import pub

from scheduling import Scheduler
from bundleMgmt import Buffer, Bundle
from routing import cgr_yens


@dataclass
class Node:
    """
    A Node object is a network element that can participate, in some way, to the data
    scheduling, generation, routing and/or delivery process.
    """
    uid: int
    scheduler: Scheduler = None
    buffer: Buffer = Buffer()
    contact_plan: List = field(default_factory=lambda: [])
    route_table: Dict = field(default_factory=lambda: {})
    outbound_q: Dict = field(default_factory=lambda: {})

    def contact_controller(self, env):
        """
        Generator that triggers the start and end of contacts according to the contact
        plan.

        Iterates over the contacts in which self is the sending node and invokes the
        contact procedure so that, if appropriate, bundles are forwarded to the current
        neighbour. Once the contact has concluded, we exit from this contact
        """
        while True:
            next_contact = self.get_current_contact(env)
            time_to_contact_start = next_contact.start - env.now
            # Delay until the contact starts and then resume
            yield env.timeout(time_to_contact_start)
            env.process(self.contact_procedure(env, next_contact))

    def contact_procedure(self, env, contact):
        """
        Carry out the contact with a neighbouring node. This involves a handshake (if
        applicable/possible), sending of data and closing down contact
        """
        failed_bundles = []
        print(f"contact started on {self.uid} with {contact.to} at {env.now}")
        while env.now < contact.end:
            if not self.outbound_q[contact.to]:
                print(f"node {self.uid} exhausted the outbound queue at {env.now} waiting...")
                # If the outbound queue for this node is empty, wait and check again
                yield env.timeout(1)
                continue

            # Extract a bundle from the outbound queue and send it over the contact
            bundle = self.outbound_q[contact.to].pop(0)
            send_time = bundle.size / contact.rate
            if contact.end - env.now >= send_time:
                self.bundle_send(bundle, contact.to, env.now)
                # Halt until the bundle has been sent
                yield env.timeout(send_time)
            else:
                failed_bundles.append(bundle)

        print(f"contact between {self.uid} and {contact.to} ended at {env.now}")

    def bundle_send(self, b, n, t_now):
        """
        Send bundle b to node n at time t_now

        This process involves transmitting the bundle, at the transmission data rate.
        In addition to this, if more bundles are awaiting transmission, a new bundle
        send process is added to the event queue
        """
        print(f"bundle {b} sending started from {self.uid} to {n} at time {t_now}")

        # b.excluded_nodes.append(self.uid)
        b.sender = self.uid

        # Publish message so that the revieving node picks up the bundle
        pub.sendMessage(
            str(n) + "bundle",
            bundle=b
        )

    def bundle_receive(self, b):
        """
        Begin to receive bundle b

        If the bundle is too large to be accommodated, reject, else accept
        """
        if self.buffer.capacity_remaining < b.size:
            # TODO Handle the case where a bundle is too large to be accommodated
            print("")
        else:
            self.buffer.append(b)

    def get_current_contact(self, env):
        for contact in self.contact_plan:
            if contact.start >= env.now and contact.frm == self.uid:
                return contact
