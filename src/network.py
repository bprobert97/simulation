from dataclasses import dataclass


@dataclass
class TaskTable:
    """A Task Table is a list of Task objects, each one defining the acquisition/collection of one or more bundles from
    a target node at a specific time"""
    table: list = None


@dataclass
class Scheduler:
    """The Scheduler is an object that enables a node to carry out Contact Graph Scheduling operations. I.e. it can
    receive requests and process them to create tasks, which are added to a task table"""
    task_table: TaskTable


@dataclass
class Router:
    """The Router is an object that enables a node to carry out Contact Graph Routing. I.e. it can identify routes to
    different endpoints, assign bundles to specific routes and forward bundles during the appropriate contact"""
    pass


@dataclass
class Node:
    """A Node object is a network element that can participate, in some way, to the data scheduling, generation,
    routing and/or delivery process"""
    uid: int
    endpoints: list = None
    scheduler: Scheduler = None
    router: Router = None


