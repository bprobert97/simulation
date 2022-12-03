#!/usr/bin/env python3

import sys
from dataclasses import dataclass, field
from typing import List

from pubsub import pub

from routing import Route, Contact, dijkstra_cgr
from misc import id_generator


@dataclass
class Request:
    target_id: int = None
    target_lat: float = None
    target_lon: float = None
    target_alt: float = None
    deadline_acquire: int = None
    deadline_deliver: int = None
    bundle_lifetime: int = None
    priority: int = 0
    destination: int = 999
    data_volume: int = 1
    time_created: int = None
    __uid: str = field(init=False, default_factory=lambda: id_generator())

    def __post_init__(self):
        # Define a unique ID based on the time of request arrival and ID of the target
        if not self.deadline_acquire:
            self.deadline_acquire = sys.maxsize
        if not self.deadline_deliver:
            self.deadline_deliver = sys.maxsize

    @property
    def uid(self):
        return self.__uid


@dataclass
class Task:
    """
    Args:
        _status: Options include: "pending", "acquired", "redundant", "re-scheduled",
            "delivered" or "failed"
    """
    deadline_acquire: int = sys.maxsize
    deadline_delivery: int = sys.maxsize
    lifetime: int = sys.maxsize
    target: int = 0
    priority: int = 0
    destination: int = None
    size: int = 1
    assignee: int = None
    scheduled_at: int | float = None
    scheduled_by: int = None
    pickup_time: int | float = None  # Intended pick-up time
    delivery_time: int | float = None  # Intended delivery time
    acq_path: List = field(default_factory=list)
    del_path: List = field(default_factory=list)
    request_ids: List = field(default_factory=list)

    _acquired_at: int | float = field(init=False, default=None)
    _delivered_at: int | float = field(init=False, default=None)
    _status: str = field(init=False, default="pending")
    __uid: str = field(init=False, default_factory=lambda: id_generator())

    @property
    def uid(self):
        return self.__uid

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        """
        Options are:
        - "pending"
        - "acquired"
        - "delivered"
        - "redundant" if this task needs rescheduling
        - "rescheduled" if this task has now been rescheduled
        - "failed"
        """
        self._status = value

    @property
    def acquired_at(self):
        return self._acquired_at

    @acquired_at.setter
    def acquired_at(self, v):
        self._acquired_at = v

    def __lt__(self, other):
        """Order Tasks based on their status value

        Task ordering is required when merging Task Tables and identifying which of two
        tasks are the most "up-to-date", resulting in the other one being updated to match
        """
        if self.status == other.status:
            return False
        if self.status == "pending":
            return True
        if self.status == "acquired" and other.status != "pending":
            return True
        if self.status == "redundant" and \
                (other.status != "pending" and other.status != "acquired"):
            return True
        return False

    def __repr__(self):
        return "Task: ID %s | Target %d | Assignee %s | Status %s | pickup time %d" % (
            self.__uid, self.target, self.assignee, self.status, self.pickup_time
        )


@dataclass
class Scheduler:
    """The Scheduler is an object that enables a node to carry out Contact Graph
    Scheduling operations. I.e. it can schedule tasks in response to requests, based on
    a certain Contact Plan"""
    parent = None

    def schedule_task(self, request: Request, curr_time: int, contact_plan: list,
                      contact_plan_targets: list) -> Task | None:
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
            contact_plan_targets: List of Contact objects with target nodes

        Returns:
            task: a Task object (if possible), else None

        """
        # TODO This is fine while we have a contact plan with target contacts
        #  available, but in the more general case we're more likely to simply have a
        #  target location with which we need to evaluate (in real time) contact
        #  opportunities
        contact_plan.extend([c for c in contact_plan_targets if c.to == request.target_id])

        acq_path, del_path = self._cgs_routing(self.parent.uid, request, curr_time,
                                               contact_plan)

        # Remove any contacts with this target before moving on, so that we don't clutter
        [
            contact_plan.remove(x) for x in [
                c for c in contact_plan_targets if c.to == request.target_id
            ]
        ]

        if acq_path and del_path:
            for hop in del_path.hops:
                hop.volume -= request.data_volume

            parent = self.parent.uid if self.parent else None

            task = Task(
                deadline_acquire=request.deadline_acquire,
                deadline_delivery=request.deadline_deliver,
                lifetime=request.bundle_lifetime,
                target=request.target_id,
                priority=request.priority,
                destination=request.destination,
                size=request.data_volume,
                assignee=del_path.hops[0].frm,
                scheduled_at=curr_time,
                scheduled_by=parent,
                pickup_time=acq_path.bdt,
                delivery_time=del_path.bdt,
                acq_path=[x.uid for x in acq_path.hops],
                del_path=[x.uid for x in del_path.hops]
            )

            task.request_ids.append(request.uid)
            pub.sendMessage("task_add", t=task)
            return task

        else:
            # If no assignee has been identified, then it means there's no feasible way
            # the data can be acquired and delivered that fulfills the requirements.
            # TODO add in some exception that handles a lack of feasible acquisition
            print(f"No task was created for request {request.uid} as either acquisition "
                  f"or delivery wasn't feasible")
            pub.sendMessage("request_fail")
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
                                    request.deadline_acquire)

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
                request.deadline_deliver,
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