#!/usr/bin/env python3

import sys
from dataclasses import dataclass, field
from typing import List

from pubsub import pub

from routing import Route, Contact, dijkstra_cgr


@dataclass
class Request:
    target_id: int
    target_lat: float = None
    target_lon: float = None
    target_alt: float = None
    time_acq: int = sys.maxsize
    time_del: int = sys.maxsize
    priority: int = 0
    destination: int = 999
    data_volume: int = 5
    time_created: int = None

    def __post_init__(self):
        # Define a unique ID based on the time of request arrival and ID of the target
        self.__uid = f"{self.time_created}_{self.target_id}"

    @property
    def uid(self):
        return self.__uid


@dataclass
class Task:
    request_id: str
    deadline_acquire: int
    deadline_delivery: int
    target: int
    priority: int
    destination: int
    size: int
    assignee: int
    scheduled_at: int | float
    time_acquire: int | float = None  # Intended pick-up time
    time_deliver: int | float = None  # Intended delivery time
    acq_path: List = field(default_factory=lambda: [])
    del_path: List = field(default_factory=lambda: [])
    _acquired_at: int | float = None
    _delivered_at: int | float = None
    _status: str = "pending"

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def acquired_at(self):
        return self._acquired_at

    @acquired_at.setter
    def acquired_at(self, v):
        self._acquired_at = v


@dataclass
class Scheduler:
    """The Scheduler is an object that enables a node to carry out Contact Graph
    Scheduling operations. I.e. it can receive requests and process them to create
    tasks, which are added to a task table"""
    uid: int
    task_table: List = field(default_factory=lambda: [])
    contact_plan: List = field(default_factory=lambda: [])
    request_queue: List = field(default_factory=lambda: [])

    def request_received(self, request, t_now):
        """
        When a request is received, it gets added to the request queue
        """
        self.request_queue.append(request)

        # TODO this will trigger the request processing immediately having received a
        #  request, however we may want to set this process to be periodic
        self.process_requests(t_now)

    def process_requests(self, curr_time):
        """
        Process each request in the queue, by identifying the assignee-target contact
        that will collect the payload, creating a Task for this and adding it to the table
        :return:
        """
        while self.request_queue:
            request = self.request_queue.pop(0)
            # Adjust the contact plan to include contacts with the target node
            # self._add_target_contacts(request)
            task = self.schedule_task(request, curr_time)

            # If a task has been created (i.e. there is a feasible acquisition and
            # delivery opportunity), add the task to the table. Else, that request
            # cannot be fulfilled so log something to that effect
            # TODO If the task table has been updated, it should be shared with anyone
            #  with whom we are currently in a contact, since it may be of value to them
            if task:
                self.task_table.append(task)
            else:
                print(f"Request for {request.target_id}, submitted at "
                      f"{request.time_created} cannot be processed")
            # self._remove_contact_from_cp(request.target_id)

    def schedule_task(self, request, curr_time):
        """
        Identify the contact, between a satellite & target, in which the request should
        be fulfilled and return a task that includes the necessary info. The process to do
        this is effectively two-executions of Dijkstra's algorithm, whereby routes to
        the acquisition opportunity are found, followed immediately by finding the
        shortest route to the destination from that point.

        As requests are handled, the main contact schedule is updated to account for
        the resources used, such that future requests are scheduled based on the
        network state having already processed previous requests.
        :param request:
        :return:
        """
        # TODO setting the UID of the Scheduler to = 1
        acq_path, del_path = self._cgs_routing(self.uid, request, curr_time)
        if acq_path and del_path:
            for hop in del_path.hops:
                hop.volume -= request.data_volume
            task = Task(
                request.uid,
                request.time_acq,
                request.time_del,
                request.target_id,
                request.priority,
                request.destination,
                request.data_volume,
                del_path.hops[0].frm,
                curr_time,
                acq_path.bdt,
                del_path.bdt,
                [x.uid for x in acq_path.hops],
                [x.uid for x in del_path.hops]
            )
            pub.sendMessage("task_added", t=task)
            return task

        else:
            # If no assignee has been identified, then it means there's no feasible way the
            # data can be acquired and delivered that fulfills the requirements.
            # TODO add in some exception that handles a lack of feasible acquisition
            print(
                "No task was created for request ",
                request.uid,
                ", as either acquisition or delivery wasn't feasible")
            return

    def _cgs_routing(self, root: int, request: Request, curr_time: int) -> tuple:
        """
        Find the acquisition and delivery paths such that the data is delivered at the
        earliest opportunity.
        The general procedure is:
        ---------------------------------------------------------------------------------
        While a possible better acquisition opportunity exists:
        -- Find the route to the earliest acquisition opportunity
        -- If either no route is found, or the time of acquisition is greater than the
        earliest time of delivery:
        ---- break
        -- Remove all contacts between the most recent assignee and the target
        -- Find the shortest delivery route from the most recent acquisition opportunity
        -- If a delivery route is found:
        ---- add the acquisition-delivery route pair to the list of potential paths
        ---------------------------------------------------------------------------------
        :param root: ID of the starting Contact going from/to source node (self)
        :param request: Request object defining the target node ID, deadlines, etc
        :return:
        """
        path_acq_selected = None
        path_del_selected = None
        earliest_delivery_time = sys.maxsize

        # reset contacts
        self._reset_contacts()

        # Root contact is the connection to self that acts as the source vertex in the
        # Contact Graph
        root = Contact(root, root, curr_time, sys.maxsize, sys.maxsize)
        root.arrival_time = curr_time

        while True:
            # Find the lowest cost acquisition path using Dijkstra
            # TODO This is using the Dijkstra function from the routing.py file. There
            #  must be a cleaner way to use this, e.g. having a "Router" object that is
            #  an attribute on both this scheduler and the node objects??
            path_acq = dijkstra_cgr(self.contact_plan, root, request.target_id,
                                    request.time_acq)

            if not path_acq or path_acq.bdt >= earliest_delivery_time:
                break

            # suppress all acquisition opportunities from this node so that it's not
            # considered in any other searches (later acquisitions cannot be better)
            self._suppress_contacts_from_node(path_acq.hops[-1].frm, request.target_id)

            # Create a root contact from which we can find a delivery path
            root_delivery = Contact(
                path_acq.hops[-1].frm,
                path_acq.hops[-1].frm,
                path_acq.bdt,
                sys.maxsize,
                sys.maxsize
            )
            root_delivery.arrival_time = path_acq.bdt

            # Identify best route to the destination from our current acquiring node
            path_del = dijkstra_cgr(
                self.contact_plan,
                root_delivery,
                request.destination,
                request.time_del,
                request.data_volume
            )

            # If there are no valid routes to the destination from this target
            # acquisition, skip
            if not path_del:
                continue

            # If this delivery route is better than our current "best", assign it
            if path_del.bdt < earliest_delivery_time:
                earliest_delivery_time = path_del.bdt
                path_acq_selected = path_acq
                path_del_selected = path_del

        # If we've not been able to find any feasible delivery opportunities, then we
        # cannot fulfil the task, otherwise, return the selected paths
        if not path_del_selected:
            return None, None
        return path_acq_selected, path_del_selected

    def _dijkstra(self, root, dest, deadline, size=0):
        # TODO We should be able to reuse the CGR Dijkstra code, since it's VERY
        #  similar to this
        """
        Finds the lowest cost Route from the current node to a destination node,
        arriving before some deadline
        :return:
        """
        # TODO Consider restricting which contacts are added to the CG, since for a
        #  long time horizon with many contacts, this could become unnecessarily large.
        # Set of contacts that have been visited during the contact plan search
        # unvisited = [c.uid for c in self.contacts]
        [c.clear_dijkstra_area() for c in self.contact_plan if c is not root]

        current = root

        # Pre-set the variables used to track the "optimal" route and set the arrival
        # time along the "best" route (the "best delivery time", bdt) to be large
        route = None  # optimal route so far
        final = None  # The "final" contact along the current route
        bdt = sys.maxsize  # "best delivery time"

        # TODO Identify a situation when this would ever NOT be the case, seems redundant
        if current.to not in current.visited_nodes:
            current.visited_nodes.append(current.to)

        while True:
            final, bdt = self._contact_review(current, dest, final, bdt, deadline, size)
            next_contact = self._contact_selection(bdt, deadline)

            if not next_contact:
                break

            current = next_contact

        # Done contact graph exploration, check and store new route
        if final is not None:
            hops = []
            contact = final
            while contact != root:
                hops.insert(0, contact)
                contact = contact.predecessor

            route = Route(hops[0])
            for hop in hops[1:]:
                route.append(hop)

        return route

    def _contact_review(self, current, dest, final_contact, bdt, deadline, size):
        """
        Review each contact that is adjacent to the current one (i.e. the sending node
        of the next contact = the receiving node of the current one) and update the
        best case arrival time. If any of the contacts have the destination as the
        receiving node, mark as "final" and log the time at which we could arrive
        there. Return the "final node" and the time of arrival at that node (delivery).
        :param current: Current contact from which we are searching
        :param dest: Destination node for the route being constructed
        :param final_contact: Adjacent contact with earliest arrival time and
            destination as the receiving node
        :param bdt: Best case delivery time at the destination via the "final" contact
        :return final_contact:
        :return bdt:
        """
        # TODO No point looking beyond the acquisition deadline, but currently not
        #  stopping
        for contact in self.contact_plan:
            if contact.frm != current.to:
                continue
            if contact in current.suppressed_next_hop:
                continue
            if contact.suppressed or contact.visited:
                continue
            if contact.to in current.visited_nodes:
                continue
            if contact.t_end <= current.arrival_time:
                continue
            if contact.volume < size:
                continue
            # TODO remove this as should never be the case I don't think
            if current.frm == contact.to:
                continue
            if contact.t_start > deadline:
                continue

            # Calculate arrival time
            # If the next contact begins before we could have arrived at the
            # current contact, set the arrival time to be the arrival at the
            # current plus the time to traverse the contact
            if contact.t_start < current.arrival_time:
                arrival_time = current.arrival_time + contact.owlt
            else:
                arrival_time = contact.t_start + contact.owlt

            # Update attribs if arrival is better or equal and update other parameters in
            # next contact so that we assume we're coming from the current
            if arrival_time <= contact.arrival_time:
                contact.arrival_time = arrival_time
                contact.predecessor = current
                contact.visited_nodes = current.visited_nodes[:]
                contact.visited_nodes.append(contact.to)

                # Mark if destination reached
                if contact.to == dest and contact.arrival_time < bdt:
                    bdt = contact.arrival_time
                    final_contact = contact
            # TODO Can we break here if just looking at min latency? We've
            #  reached the destination and have identified all of the other
            #  contacts that happen before this

        # This completes our assessment of the current contact
        current.visited = True
        return final_contact, bdt

    def _contact_selection(self, bdt, deadline):
        """
        Return the contact that has the earliest arrival time, which hasn't already
        been "visited" or "suppressed", and that starts before the current deadline.
        Basically, in the contact_review procedure, we've been through all of the
        contacts that we can get to next and marked the time at which we can arrive
        there. This is simply identifying which of those adjacent contacts is the best
        one to explore next.
        :param bdt: Best delivery time, i.e. the earliest time that we know we can
            arrive at the destination, as discovered so far
        :param deadline: The latest time we can arrive at the destination
        :return:
        """
        earliest_arr_t = sys.maxsize
        next_contact = None

        for contact in self.contact_plan:

            # Ignore visited or suppressed
            if contact.suppressed or contact.visited:
                continue

            # Ignore contacts that start after the deadline has already passed
            if contact.t_start > deadline:
                break  # This is only valid if the Contact list is ordered by arrival
            # continue  # Use this if the Contact list is not ordered by arrival

            # If we know there is another, better contact, break from the search as
            # nothing else in the CP is going to be better
            if contact.arrival_time > bdt:
                break  # This is only valid if the Contact list is ordered by arrival
            # continue  # Use this if the Contact list is not ordered by arrival

            if contact.arrival_time < earliest_arr_t:
                earliest_arr_t = contact.arrival_time
                next_contact = contact

        return next_contact

    def _suppress_contacts_from_node(self, node_from, node_to):
        for contact in self.contact_plan:
            if int(contact.frm) == node_from and int(contact.to) == node_to:
                contact.suppressed = True

    def _add_target_contacts(self, request):
        """
        Add contacts to the request target node, to the contact list, so that they can
        be considered as part of the CGS procedure
        """
        # TODO change this to find connections in real-time, rather than finding
        #  already identified contacts with pre-defined targets.
        target_contacts = []
        for idx, row in self.cp_targets.iterrows():
            if row['receiving'] == request.target_id:
                target_contacts.append(
                    Contact(
                        int(row['id']),
                        int(row['sending']),
                        int(row['receiving']),
                        row['start'],
                        row['end'],
                        row['capacity'] / (row['end'] - row['start'])
                    )
                )

        self.contact_plan.extend(target_contacts)
        self.contact_plan.sort(key=lambda k: k.t_start)

    def _remove_contact_from_cp(self, node_id):
        """
        Remove all contacts to a particular node, from the contact list
        """
        self.contact_plan = [x for x in self.contact_plan if x.to != node_id]

    def _reset_contacts(self):
        for contact in self.contact_plan:
            contact.clear_dijkstra_area()
            contact.clear_management_area()


def get_matching_task_idx(self, task):
    # TODO The use of this flag avoids an issue when the idx is 0, but it feels hacky
    flag = False
    for idx, t in enumerate(self.table):
        # If it's the same task, but the status is different, this means that
        # there's a mismatch that needs addressing.
        if task.target == t.target and task.assignee == t.assignee and \
                task.scheduled_at == t.scheduled_at:
            flag = True
            return flag, idx
    return flag, None


def update_task_status(self, task, new_status: str):
    flag, idx = self.get_matching_task_idx(task)
    if flag:
        self.table[idx].status = new_status