import sys
from dataclasses import dataclass, field
from typing import List


@dataclass
class Contact:
    frm: int
    to: int
    start: int
    end: int
    rate: int
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

    def clear_dijkstra_working_area(self):
        self.arrival_time = sys.maxsize
        self.visited = False
        self.predecessor = 0
        self.visited_nodes = []

    def clear_management_working_area(self):
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
    def availability(self):
        avail = 1
        for x in self._hops:
            avail *= x.availability
        return avail

    @property
    def capacity(self):
        return min([x.capacity for x in self.hops])

    @property
    def storage(self):
        # FIXME: this only considers storage at the hops used in the route, but really
        #  it should consider the storage at all contacts that would be impacted if a
        #  bundle were to be sent along this route. Can base this on the method used in
        #  the DataRoutingModule._modify_storage() method
        return min([x.to_storage for x in self.hops])

    @property
    def resource(self):
        return min(self.capacity, self.storage)

    @property
    def bdt(self):
        # Best-case delivery time (i.e. the earliest time a byte of data could arrive
        # at the destination)
        return max([c.t_start for c in self.hops])

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
            if self.resource > other_route.resource:
                return True

            # 3rd: confidence
            elif self.resource == other_route.resource:
                if self.availability >= other_route.availability:
                    return True

        return False

    def __repr__(self):
        return "to:%s | via:%s | arvl:%s | hops:%s | resource:%s | conf:%s" % \
               (
                   self.hops[-1].to,
                   self.hops[0].to,
                   self.bdt,
                   len(self.hops),
                   self.resource,
                   self.availability
               )