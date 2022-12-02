#!/usr/bin/env python3

import sys
from dataclasses import dataclass, field
from typing import List, Dict
from copy import deepcopy

from pubsub import pub

from scheduling import Scheduler, Request
from bundles import Buffer, Bundle


OUTBOUND_QUEUE_INTERVAL = 1
BUNDLE_ASSIGN_REPEAT_TIME = 1


@dataclass
class Node:
    """
    A Node object is a network element that can participate, in some way, to the data
    scheduling, generation, routing and/or delivery process.
    """
    uid: int
    scheduler: Scheduler = None
    buffer: Buffer = Buffer()
    outbound_queues: Dict = field(default_factory=dict)
    contact_plan: List = field(default_factory=list)
    contact_plan_targets: List = field(default_factory=list)

    _bundle_assign_repeat: int = field(init=False, default=BUNDLE_ASSIGN_REPEAT_TIME)
    _outbound_repeat_interval: int = field(init=False, default=OUTBOUND_QUEUE_INTERVAL)
    route_table: Dict = field(init=False, default_factory=dict)
    request_queue: List = field(init=False, default_factory=list)
    task_table: List = field(init=False, default_factory=list)
    drop_list: List = field(init=False, default_factory=list)
    delivered_bundles: List = field(init=False, default_factory=list)
    _task_table_updated: bool = field(init=False, default=False)
    _targets: List = field(init=False, default_factory=list)
    _contact_plan_self: List = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        # TODO will need to update this IF we update the contact plan
        self._contact_plan_self = [c for c in self.contact_plan if c.frm == self.uid]
        self._contact_plan_self.extend(
            [c for c in self.contact_plan_targets if c.frm == self.uid]
        )
        self._contact_plan_self.sort()

    # *** REQUEST HANDLING (I.E. SCHEDULING) ***
    def request_received(self, request, t_now):
        """
        When a request is received, it gets added to the request queue
        """
        self.request_queue.append(request)

        # TODO this will trigger the request processing immediately having received a
        #  request, however we may want to set this process to be periodic
        self._process_requests(t_now)

    def _process_requests(self, curr_time):
        """
        Process each request in the queue, by identifying the assignee-target contact
        that will collect the payload, creating a Task for this and adding it to the table
        :return:
        """
        while self.request_queue:
            request = self.request_queue.pop(0)
            # Check to see if any existing tasks exist that could service this request.
            if self._is_request_serviced(request):
                pub.sendMessage("request_duplicated")
                continue

            task = self.scheduler.schedule_task(request, curr_time, self.contact_plan,
                                                self.contact_plan_targets)

            # If a task has been created (i.e. there is a feasible acquisition and
            # delivery opportunity), add the task to the table. Else, that request
            # cannot be fulfilled so log something to that effect
            # TODO If the task table has been updated, it should be shared with anyone
            #  with whom we are currently in a contact, since it may be of value to them
            if task:
                self.task_table.append(task)
                self._task_table_updated = True
            else:
                print(f"Request for {request.target_id}, submitted at "
                      f"{request.time_created} cannot be processed")
            # self._remove_contact_from_cp(request.target_id)

    def _is_request_serviced(self, request: Request) -> bool:
        """Returns True if the request is already handled by an existing Task.

        Check to see if any of the existing tasks would satisfy the request. I.e. the
        target ID is the same and the (ideal) time of acquisition is at, or after,
        the request arrival time. Effectively, this bundle could be delivered in
        response to this request.

        Args:
            request: A Request object

        Returns:
            A boolean indicating whether (True) or not (False) the request is already
            being handled by an existing task
        """
        for task in self.task_table:
            if task.target == request.target_id and task.pickup_time >= \
                    request.time_created:
                task.request_ids.append(request.uid)
                return True
        return False

    # *** CONTACT HANDLING ***
    def contact_controller(self, env):
        """Generator that iterates over every contact in which this node is the sender.

        Iterates over the contacts in which self is the sending node and invokes the
        contact procedure so that, if appropriate, bundles are forwarded to the current
        neighbour. Once the contact has concluded, we exit from this contact, but until
        that point the contact procedure remains live so that any bundles/Task Table
        updates that arrive during the contact can be shared (if applicable)
        """
        while self._contact_plan_self:
            next_contact = self._contact_plan_self.pop(0)
            time_to_contact_start = next_contact.start - env.now

            # Delay until the contact starts and then resume
            yield env.timeout(time_to_contact_start)
            if next_contact.to in self._targets:
                self._target_contact_procedure(env.now, next_contact.to)
            else:
                env.process(self._node_contact_procedure(env, next_contact))

    def _target_contact_procedure(self, t_now, target):
        """
        Procedure to follow if we're in contact w/ a Target node
        """
        for task in self.task_table:
            if task.pickup_time == t_now and task.target == target:
                bundle_lifetime = min(task.deadline_delivery, t_now+task.lifetime)
                bundle = Bundle(
                    src=self.uid,
                    dst=task.destination,
                    target_id=target,
                    size=task.size,
                    lifetime=bundle_lifetime,
                    created_at=t_now,
                    priority=task.priority
                )
                self.buffer.append(bundle)
                print(f"Bundle acquired on node {self.uid} at time {t_now} from target "
                      f"{target}")
                pub.sendMessage("bundle_acquired", b=bundle)
                return

    def _node_contact_procedure(self, env, contact):
        """
        Carry out the contact with a neighbouring node. This involves a handshake (if
        applicable/possible), sending of data and closing down contact
        """
        failed_bundles = []
        # print(f"contact started on {self.uid} with {contact.to} at {env.now}")
        self._handshake(env, contact.to, contact.owlt)
        while env.now < contact.end:
            # If the task table has been updated while we've been in this contact,
            # send that before sharing any more bundles as it may be of value to the
            # neighbour
            if self._task_table_updated:
                env.process(
                    self._bundle_send(
                        env,
                        deepcopy(self.task_table),
                        contact.to,
                        contact.owlt,
                        True
                    )
                )
                self._task_table_updated = False
                yield env.timeout(0)
                continue

            # If we don't have any bundles waiting in the current neighbour's outbound
            # queue, we can just wait a bit and try again later
            if not self.outbound_queues[contact.to]:
                yield env.timeout(self._outbound_repeat_interval)
                continue

            # Extract a bundle from the outbound queue and send it over the contact
            bundle = self.outbound_queues[contact.to].pop(0)
            send_time = bundle.size / contact.rate
            if contact.end - env.now >= send_time:
                bundle.previous_node = self.uid
                bundle.update_age(env.now)
                env.process(
                    self._bundle_send(
                        env,
                        bundle,
                        contact.to,
                        contact.owlt+send_time
                    )
                )
                # Wait until the bundle has been sent (note it may not have been fully
                # received at this time, due to the OWLT, but that's fine)
                yield env.timeout(send_time)

            # If we don't have enough time remaining to send this bundle, pop it into a
            # list that can be processed (i.e. returned to the buffer) after the
            # contact. If we added it back into the buffer right away, it might get put
            # right back into the outbound queue...
            # FIXME We should technically be able to put this into the buffer to get
            #  reprocessed, because if there's insufficient resources to handle this
            #  bundle over this contact, that should get spotted during the bundle
            #  assignment process and it should therefore NOT get added to the OBQ
            else:
                failed_bundles.append(bundle)

        # print(f"contact between {self.uid} and {contact.to} ended at {env.now}")

        # Add any bundles that couldn't fit across the contact back in to the
        #  buffer so that they can be assigned to another outbound queue.
        for b in failed_bundles + self.outbound_queues[contact.to]:
            self.buffer.append(b)

    def _handshake(self, env, to, delay):
        """
        Carry out the handshake at the beginning of the contact,
        """
        env.process(self._bundle_send(env, deepcopy(self.task_table), to, delay, True))

    def _bundle_send(self, env, b, n, delay, is_task_table=False):
        """
        Send bundle b to node n

        This process involves transmitting the bundle, at the transmission data rate.
        In addition to this, if more bundles are awaiting transmission, a new bundle
        send process is added to the event queue
        """
        while True:
            if isinstance(b, Bundle):
                print(f"bundle sent from {self.uid} to {n} at time {env.now}, "
                      f"size {b.size}, total delay {delay:.1f}")
            # Wait until the whole message has arrived and then invoke the "receive"
            # method on the receiving node
            yield env.timeout(delay)
            pub.sendMessage(
                str(n) + "bundle",
                env=env, bundle=b, is_task_table=is_task_table
            )
            break

    def _bundle_receive(self, env, bundle, is_task_table=False):
        """
        Receive bundle from neighbouring node. This also includes the receiving of Task
        Tables, as indicated by the flag in the args.

        If the bundle is too large to be accommodated, reject, else accept
        """
        if is_task_table:
            self._merge_task_tables(bundle)
            return

        if self.buffer.capacity_remaining < bundle.size:
            # TODO Handle the case where a bundle is too large to be accommodated
            print("")
            return

        bundle.hop_count += 1

        if bundle.dst == self.uid:
            print(f"bundle delivered to {self.uid} from {bundle.previous_node} at {env.now:.1f}")
            pub.sendMessage("bundle_delivered")
            self.delivered_bundles.append(bundle)
            return

        print(f"bundle received on {self.uid} from {bundle.previous_node} at {env.now:.1f}")
        pub.sendMessage("bundle_forwarded")
        self.buffer.append(bundle)

    # *** BUNDLE & TASK TABLE HANDLING ***
    def bundle_assignment_controller(self, env):
        while True:
            self._bundle_assignment(env)
            yield env.timeout(self._bundle_assign_repeat)

    def _bundle_assignment(self, env):
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
                if route.bdt > b.lifetime:
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
                pub.sendMessage("bundle_dropped")

    def _merge_task_tables(self, tt):
        """
        Combine the task table received from a neighbour, with one's own task table,
        to ensure it is up-to-date
        """
        for task in tt:
            present, idx = self._get_matching_task_idx(task)
            # If the task exists in the task table, but the statuses don't match and
            # the local task is "pending", then the other task must have been modified
            # already, so should be updated to match
            if present:
                if task.status != self.task_table[idx].status and \
                        self.task_table[idx].status == "pending":
                    self.task_table[idx].status = task.status
                    self._task_table_updated = True
                else:
                    continue
            else:
                self.task_table.append(deepcopy(task))
                self._task_table_updated = True

    def _get_matching_task_idx(self, task):
        # TODO The use of this flag avoids an issue when the idx is 0, but it feels hacky
        flag = False
        for idx, t in enumerate(self.task_table):
            # If it's the same task, but the status is different, this means that
            # there's a mismatch that needs addressing.
            if task.target == t.target and task.assignee == t.assignee and \
                    task.scheduled_at == t.scheduled_at:
                flag = True
                return flag, idx
        return flag, None
