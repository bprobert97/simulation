#!/usr/bin/env python3


import sys
from dataclasses import dataclass, field
from typing import List


@dataclass
class Contact:
    frm: int
    to: int
    start: int
    end: int
    rate: int = 1
    confidence: int = 1
    owlt: float = 0
    # route search working area
    arrival_time: int = sys.maxsize
    visited: bool = False
    visited_nodes: List = field(default_factory=lambda: [])
    predecessor: int = 0
    # route management working area
    suppressed: bool = False
    suppressed_next_hop: List = field(default_factory=lambda: [])
    # forwarding working area
    first_byte_tx_time: int = None
    last_byte_tx_time: int = None
    last_byte_arr_time: int = None
    effective_volume_limit: int = None

    def __post_init__(self):
        self.volume = self.rate * (self.end - self.start)
        self.mav = [self.volume, self.volume, self.volume]

    def clear_dijkstra_area(self):
        self.arrival_time = sys.maxsize
        self.visited = False
        self.predecessor = 0
        self.visited_nodes = []

    def clear_management_area(self):
        self.suppressed = False
        self.suppressed_next_hop = []

    def __repr__(self):

        # replace with inf
        if self.end == sys.maxsize:
            end = "inf"
        else:
            end = self.end

        # mav in %
        volume = 100 * min(self.mav) / self.volume

        return "%s->%s(%s-%s,d%s)[mav%d%%]" % (self.frm, self.to, self.start, end, self.owlt, volume)


class Route:
    def __init__(self, contact):
        """
        A Route is an ordered sequence of contact events.

        The capacity of each contact, combined with the availability (and other factors
        such as the available storage capacity on board the receiving node) will
        dictate the feasibility of the journey for a particular bundle.

        :param contact: Contact object
        """
        self._hops = []
        self.append(contact)

    @property
    def hops(self):
        """
        Ordered sequence of Contacts that make up the route
        :return:
        """
        return self._hops

    @property
    def confidence(self):
        avail = 1
        for x in self._hops:
            avail *= x.confidence
        return avail

    @property
    def volume(self):
        return min([x.volume for x in self.hops])

    @property
    def bdt(self):
        # Best-case delivery time (i.e. the earliest time a byte of data could arrive
        # at the destination)
        return max([c.start for c in self.hops])

    def append(self, contact):
        """
        Add a hop to the journey
        :return:
        """
        self._hops.append(contact)

    # OPERATOR OVERLOAD FOR SELECTION #
    # Less than = this route is better than the other (less costly)
    def __lt__(self, other_route):
        # 1st priority : arrival time
        if self.bdt < other_route.bdt:
            return True

        # FIXME This should prioritise routes that have earlier intermediate contacts.
        #  Currently, a route containing 3 contacts, with the middle one coming later,
        #  could be deemed better than one with the middle contact earlier.
        # 2nd: volume
        elif self.bdt == other_route.bdt:
            if self.volume > other_route.volume:
                return True

            # 3rd: confidence
            elif self.volume == other_route.volume:
                if self.confidence >= other_route.confidence:
                    return True

        return False

    def __repr__(self):
        return "to:%s | via:%s | arvl:%s | hops:%s | volume:%s | conf:%s" % \
               (
                   self.hops[-1].to,
                   self.hops[0].to,
                   self.bdt,
                   len(self.hops),
                   self.volume,
                   self.confidence
               )


def cgr_yens(src, dest, t_now, num_routes, contact_plan):
    """
    Find the k shortest paths between nodes s & d in the graph, g
    :param dest: destination node
    :param num_routes: number of shortest paths to return
    :return:
    """
    # TODO Change this from finding the k-shortest routes, to simply finding routes
    #  until we have enough resources to send all the bundles with this destination
    potential_routes = []

    # Root contact is the connection to self that acts as the source vertex in the
    # Contact Graph
    root = Contact(src, src, t_now, sys.maxsize, sys.maxsize)
    root.arrival_time = t_now

    # reset contacts
    for contact in contact_plan:
        contact.clear_dijkstra_area()
        contact.clear_management_area()

    # Find the lowest cost path using Dijkstra
    route = dijkstra_cgr(contact_plan, root, dest)
    if route is None:
        return route

    routes = [route]

    [r.hops.insert(0, root) for r in routes]

    for k in range(num_routes - len(routes)):
        # For each contact in the most recently identified (k-1'th) route (apart
        # from the last contact
        for spur_contact in routes[-1].hops[:-1]:

            # create root_path that follows the most recently found route from the
            # root_contact up to and including the spur_contact. It is fron this
            # point that we'll branch off and do a new search.
            spur_contact_index = routes[-1].hops.index(spur_contact)

            root_path = Route(routes[-1].hops[0])
            for hop in routes[-1].hops[1:spur_contact_index + 1]:
                root_path.append(hop)

            # reset contacts
            for contact in contact_plan:
                contact.clear_dijkstra_area()
                contact.clear_management_area()

            # suppress all contacts in root_path except spur_contact
            for contact in root_path.hops[:-1]:
                contact.suppressed = True

            # suppress outgoing edges from spur_contact covered by known routes
            for route in routes:
                if root_path.hops == route.hops[0:(len(root_path.hops))]:
                    if route.hops[
                        len(root_path.hops)
                    ] not in spur_contact.suppressed_next_hop:
                        spur_contact.suppressed_next_hop.append(
                            route.hops[len(root_path.hops)]
                        )

            # prepare spur_contact as root contact
            spur_contact.clear_dijkstra_area()
            spur_contact.arrival_time = root_path.bdt
            # spur_contact.cost = root_path.cost
            for hop in root_path.hops:  # add visited nodes to spur_contact
                spur_contact.visited_nodes.append(hop.to)

            # try to find a spur_path with dijkstra
            spur_path = dijkstra_cgr(contact_plan, spur_contact, dest)

            # if found store new route in potential_routes
            if spur_path:
                total_path = Route(root_path.hops[0])
                for hop in root_path.hops[1:]:  # append root_path
                    total_path.append(hop)
                for hop in spur_path.hops:  # append spur_path
                    total_path.append(hop)
                if total_path.hops not in [p.hops for p in potential_routes]:
                    potential_routes.append(total_path)

        # if no more potential routes end search
        if not potential_routes:
            break

        # sort potential routes by arrival_time
        potential_routes.sort(reverse=True)

        # add best route to routes
        routes.append(potential_routes.pop())

    # remove root_contact from hops
    for route in routes:
        route.hops.pop(0)

    return routes


def dijkstra_cgr(contact_plan, root, dest):
    """
    Finds the lowest cost Route from the current node to a destination node
    :return:
    """
    # TODO Consider restricting which contacts are added to the CG, since for a
    #  long time horizon with many contacts, this could become unnecessarily large.
    # Set of contacts that have been visited during the contact plan search
    # unvisited = [c.uid for c in self.contacts]
    [c.clear_dijkstra_area() for c in contact_plan if c is not root]

    current = root

    # Pre-set the variables used to track the "optimal" route and set the arrival
    # time along the "best" route (the "best delivery time", bdt) to be large
    route = None  # optimal route so far
    final = None  # The "final" contact along the current route
    bdt = sys.maxsize  # "best delivery time"
    # lcr = sys.maxsize  # "lowest cost route"

    # TODO Identify a situation when this would ever NOT be the case, seems redundant
    if current.to not in current.visited_nodes:
        current.visited_nodes.append(current.to)

    while True:
        final, bdt = contact_review(contact_plan, current, dest, final, bdt)
        next_contact = contact_selection(contact_plan, bdt)

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


def contact_review(contact_plan, current, dest, final_contact, bdt):
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
    contact_plan_hash = {}
    for contact in contact_plan:
        if contact.frm not in contact_plan_hash:
            contact_plan_hash[contact.frm] = []
        if contact.to not in contact_plan_hash:
            contact_plan_hash[contact.to] = []
        contact_plan_hash[contact.frm].append(contact)

    for contact in contact_plan_hash[current.to]:
        if contact in current.suppressed_next_hop:
            continue
        if contact.suppressed or contact.visited:
            continue
        if contact.to in current.visited_nodes:
            continue
        if contact.end <= current.arrival_time:
            continue
        # TODO Check should this be <= min bundle size?
        # TODO Should we even have this here at all? This will discard routes that
        #  don't have any capacity (right now), but what if, in the future,
        #  with un-assign a bundle that uses this contact? This contact would then
        #  become available, but we'd have discarded... We'll capture the lack of
        #  resource by the Route property anyway, so I think this should be
        #  included. If we ARE going to include it, then we should really do
        #  something similar for the storage resource. Perhaps a Contact has a
        #  similar attribute to Routes, which simply captures the more general
        #  "resource" availability
        if contact.volume <= 0:
            continue
        # TODO remove this as should never be the case I don't think
        if current.frm == contact.to and current.to == contact.frm:
            continue

        # Calculate arrival time (cost)
        # If the next contact begins before we could have arrived at the
        # current contact, set the arrival time to be the arrival at the
        # current plus the time to traverse the contact
        arrvl_time = max(
            current.arrival_time + contact.owlt,
            contact.start + contact.owlt
        )

        # Calculate the cost associated with reaching this contact via this route
        # cost = current.cost + (arrvl_time - current.arrival_time) * s_cost + t_cost

        # Update cost if better or equal and update other parameters in
        # next contact so that we assume we're coming from the current
        # if cost < contact.cost:
        if arrvl_time < contact.arrival_time:
            # contact.cost = cost
            contact.arrival_time = arrvl_time
            contact.predecessor = current
            contact.visited_nodes = current.visited_nodes[:]
            contact.visited_nodes.append(contact.to)

            # Mark if destination reached
            if contact.to == dest and contact.arrival_time < bdt:
                # if contact.to == dest and contact.cost < lcr:
                bdt = contact.arrival_time
                # lcr = contact.cost
                final_contact = contact
                # TODO Can we break here if just looking at min latency? We've
                #  reached the destination and have identified all of the other
                #  contacts that happen before this

    # This completes our assessment of the current contact
    current.visited = True
    return final_contact, bdt


def contact_selection(contact_plan, bdt):
    # Determine best next contact among all in contact plan
    earliest_arr_t = sys.maxsize
    # lowest_cost = sys.maxsize
    next_contact = None

    for contact in contact_plan:

        # Ignore visited or suppressed
        if contact.visited or contact.suppressed:
            continue

        # TODO Triple check this is ok. In the pycgr implementation they just
        #  "continue" here, and go on to look at the rest of the CP, but I don't
        #  think this is necessary if the CP is ordered based on contact arrival
        #  time. I suppose if we don't want to order the CP, then we'd need to look
        #  through it all every time.
        # If we know there is another, better contact, break from the search as
        # nothing else in the CP is going to be better
        if bdt < contact.arrival_time < sys.maxsize-1:
            # if lcr < contact.cost < sys.maxsize - 1:
            break

        if contact.arrival_time < earliest_arr_t:
            # if contact.cost < lowest_cost:
            earliest_arr_t = contact.arrival_time
            # lowest_cost = contact.cost
            next_contact = contact

    return next_contact
