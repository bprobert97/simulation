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
        self.__uid = f"{self.time_created:.1f}_{self.target_id}"

    @property
    def uid(self):
        return self.__uid


@dataclass
class Task:
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
    request_ids: List = field(default_factory=lambda: [])
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
    Scheduling operations. I.e. it can schedule tasks in response to requests, based on
    a certain Contact Plan"""
    parent = None

    def schedule_task(self, request: Request, curr_time: int, contact_plan: list
                      ) -> Task | None:
        """

        Identify the contact, between a satellite & target, in which the request should
        be fulfilled and return a task that includes the necessary info. The process to do
        this is effectively two-executions of Dijkstra's algorithm, whereby routes to
        the acquisition opportunity are found, followed immediately by finding the
        shortest route to the destination from that point.

        As requests are handled, the main contact schedule is updated to account for
        the resources used, such that future requests are scheduled based on the
        network state having already processed previous requests.

        Args:
            request: Request object that is being processed into a Task
            curr_time: Current time
            contact_plan: List of Contact objects on which the scheduling will occur

        Returns:
            task: a Task object (if possible), else None

        """
        acq_path, del_path = self._cgs_routing(self.parent.uid, request, curr_time,
                                               contact_plan)
        if acq_path and del_path:
            for hop in del_path.hops:
                hop.volume -= request.data_volume
            task = Task(
                deadline_acquire=request.time_acq,
                deadline_delivery=request.time_del,
                target=request.target_id,
                priority=request.priority,
                destination=request.destination,
                size=request.data_volume,
                assignee=del_path.hops[0].frm,
                scheduled_at=curr_time,
                time_acquire=acq_path.bdt,
                time_deliver=del_path.bdt,
                acq_path=[x.uid for x in acq_path.hops],
                del_path=[x.uid for x in del_path.hops]
            )
            task.request_ids.append(request.uid)
            pub.sendMessage("task_added", t=task)
            return task

        else:
            # If no assignee has been identified, then it means there's no feasible way the
            # data can be acquired and delivered that fulfills the requirements.
            # TODO add in some exception that handles a lack of feasible acquisition
            print(f"No task was created for request {request.uid} as either acquisition "
                  f"or delivery wasn't feasible")
            return

    def _cgs_routing(self, root: int, request: Request, curr_time: int, contact_plan
                     ) -> tuple:
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
        for contact in contact_plan:
            contact.clear_dijkstra_area()
            contact.clear_management_area()

        # Root contact is the connection to self that acts as the source vertex in the
        # Contact Graph
        root = Contact(root, root, curr_time, sys.maxsize, sys.maxsize)
        root.arrival_time = curr_time

        while True:
            # Find the lowest cost acquisition path using Dijkstra
            # TODO This is using the Dijkstra function from the routing.py file. There
            #  must be a cleaner way to use this, e.g. having a "Router" object that is
            #  an attribute on both this scheduler and the node objects??
            path_acq = dijkstra_cgr(contact_plan, root, request.target_id,
                                    request.time_acq)

            if not path_acq or path_acq.bdt >= earliest_delivery_time:
                break

            # suppress all acquisition opportunities from this node so that it's not
            # considered in any other searches (later acquisitions cannot be better)
            self._suppress_contacts_from_node(
                path_acq.hops[-1].frm,
                request.target_id,
                contact_plan
            )

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
                contact_plan,
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

    @staticmethod
    def _suppress_contacts_from_node(node_from, node_to, contact_plan):
        for contact in contact_plan:
            if int(contact.frm) == node_from and int(contact.to) == node_to:
                contact.suppressed = True


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