#!/usr/bin/env python3
import sys
from dataclasses import dataclass, field
from typing import List, Dict
from collections import deque

from pubsub import pub

from scheduling import Scheduler
from bundles import Buffer, Bundle
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
    outbound_queues: Dict = field(default_factory=lambda: {})
    contact_plan: List = field(default_factory=lambda: [])
    route_table: Dict = field(default_factory=lambda: {})
    drop_list: List = field(default_factory=lambda: [])
    delivered_bundles: List = field(default_factory=lambda: [])
    bundle_assignment_repeat_time: int = 1

    def contact_controller(self, env):
        """
        Generator that triggers the start and end of contacts according to the contact
        plan.

        Iterates over the contacts in which self is the sending node and invokes the
        contact procedure so that, if appropriate, bundles are forwarded to the current
        neighbour. Once the contact has concluded, we exit from this contact
        """
        while self.get_current_contact(env):
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
            if not self.outbound_queues[contact.to]:
                # If the outbound queue for this node is empty, wait and check again
                yield env.timeout(1)
                continue

            # Extract a bundle from the outbound queue and send it over the contact
            bundle = self.outbound_queues[contact.to].pop(0)
            send_time = bundle.size / contact.rate
            if contact.end - env.now >= send_time:
                env.process(self.bundle_send(env, bundle, contact.to,
                                             contact.owlt+send_time))
                print(f"bundle sent from {self.uid} to {contact.to} at time {env.now}, "
                      f"size {bundle.size}, total delay {contact.owlt + send_time}")

                # Wait until the bundle has been sent
                yield env.timeout(send_time)
            else:
                failed_bundles.append(bundle)

        print(f"contact between {self.uid} and {contact.to} ended at {env.now}")

        # Add any bundles that couldn't fit across the contact back in to the
        #  buffer so that they can be assigned to another outbound queue.
        for b in failed_bundles + self.outbound_queues[contact.to]:
            self.buffer.append(b)

    def bundle_send(self, env, b, n, delay):
        """
        Send bundle b to node n

        This process involves transmitting the bundle, at the transmission data rate.
        In addition to this, if more bundles are awaiting transmission, a new bundle
        send process is added to the event queue
        """
        while True:
            b.sender = self.uid

            # Wait until the whole message has arrived and then invoke the "receive"
            # method on the receiving node
            yield env.timeout(delay)
            pub.sendMessage(str(n) + "bundle", env=env, bundle=b)
            break

    def bundle_receive(self, env, bundle):
        """
        Begin to receive bundle b

        If the bundle is too large to be accommodated, reject, else accept
        """
        if self.buffer.capacity_remaining < bundle.size:
            # TODO Handle the case where a bundle is too large to be accommodated
            print("")
            return

        if bundle.dst == self.uid:
            print(f"bundle delivered to {self.uid} from {bundle.sender} at {env.now}")
            self.delivered_bundles.append(bundle)
            return

        print(f"bundle received on {self.uid} from {bundle.sender} at {env.now}")
        self.buffer.append(bundle)

    def get_current_contact(self, env):
        for contact in self.contact_plan:
            if contact.start > env.now and contact.frm == self.uid:
                return contact

    def bundle_assignment_controller(self, env):
        while True:
            self.bundle_assignment(env)
            yield env.timeout(self.bundle_assignment_repeat_time)

    def bundle_assignment(self, env):
        """
        For each bundle in the virtual buffer, identify the route over which it should
        be sent and reduce the resources along each Contact in that route accordingly.
        Stop once all bundles have been assigned a route. Bundles for which a
        feasible route (i.e. one that ensures delivery before the bundle's expiry) is
        not available shall be dropped from the buffer.
        :return:
        """
        while not self.buffer.is_empty():
            assigned = False
            b = self.buffer.extract()

            # if config.MSR and any(
            #         [b.base_route == [int(x.uid) for x in y.hops]
            #          for y in self.route_table[b.destination]]
            # ):
            #     for route in self.route_table[b.destination]:
            #         if b.base_route == [int(x.uid) for x in route.hops]:
            #             # Add the bundle-route pair to the send_list for the "next node"
            #             self.outbound_queues[route.hops[0].to].append((b, route))
            #
            #             # Update the resources on the selected route
            #             self.resource_consumption(
            #                 b.size,
            #                 route
            #             )
            #             break
            #     continue
            for route in self.route_table[b.dst]:
                # If any of the nodes along this route are in the "excluded nodes"
                # list, then we shouldn't assign it along this route
                # TODO in CGR, this simply looks at the "next node" rather than the
                #  receiving node in all hops, but why send the bundle along a route that
                #  includes a node it shouldn't be routed via??
                # if any(hop.to in b.excluded_nodes for hop in route.hops):
                #     continue

                # if the route is not of higher value than the current best
                # route, break from for loop as none of the others will be better
                # TODO change this if converting to generic value rather than arrival time
                if route.bdt > b.deadline:
                    continue

                # Check each of the hops and make sure the bundle can actually traverse
                # that hop based on the current time and the end time of the hop
                # TODO this should really take into account the backlog over each
                #  contact and the first & last byte transmission times for this
                #  bundle. Currently, we assume that we can traverse the contact IF it
                #  ends after the current time, however in reality there's more to it
                #  than this
                for hop in route.hops:
                    if hop.end <= env.now:
                        continue

                # If this route cannot accommodate the bundle, skip
                if route.volume < b.size:
                    continue

                assigned = True
                # b.base_route = [int(x.uid) for x in route.hops]

                # Add the bundle-route pair to the send_list for the bundle's "next node"
                self.outbound_queues[route.hops[0].to].append(b)

                # Update the resources on the selected route
                # self.resource_consumption(
                #     b.size,
                #     route
                # )
                break

            if not assigned:
                self.drop_list.append(b)
