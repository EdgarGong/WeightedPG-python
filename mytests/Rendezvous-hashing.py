import mmh3
import math
from collections import Counter
import time
from crush import Crush
import utils

class Node:
    """Class representing a node that is assigned keys as part of a weighted rendezvous hash."""
    def __init__(self, id, weight):
        self.id = id
        self.weight = weight

    def compute_weighted_score(self, key):
        score = hash_to_unit_interval("%d: %s" % (self.id, key))
        log_score = 1.0 / -math.log(score)
        print("hash:", score)
        print("value:", self.weight * log_score)
        return self.weight * log_score

def hash_to_unit_interval(id):
    """Hashes a string onto the unit interval (0, 1]"""
    return (float(mmh3.hash128(id)) + 1) / 2**128

def determine_responsible_node(nodes, key):
    """Determines which node of a set of nodes of various weights is responsible for the provided key."""
    return max(nodes, key=lambda node: node.compute_weighted_score(key)) if nodes else None

pg_Rendezvous_node = {}

# for pgid in range(200):
#     pg_Rendezvous_node[pgid] = Node(pgid, pgid)
pg_Rendezvous_node[0] = Node(0, 0.73)
pg_Rendezvous_node[1] = Node(1, 0.90)
pg_Rendezvous_node[2] = Node(2, 1.07)

# node1 = Node(1, 100.5)
# node2 = Node(2, 200.1)
# node3 = Node(3, 300.3)

out = determine_responsible_node(pg_Rendezvous_node.values(), "foo")
print(out.id, out.weight)
out = determine_responsible_node(pg_Rendezvous_node.values(), "bar")
print(out.id, out.weight)
out = determine_responsible_node(pg_Rendezvous_node.values(), "hello")
print(out.id, out.weight)
exit()

obj_num = 1000000
c = Crush()
key = "100" + str('%08x' % 1) + '.' + str('%08x' % 1)
pg_num = 8192
pg_num_mask = 8191
time1 = time.time()
for i in range(obj_num):
    raw_pg_id = c.c.ceph_str_hash_rjenkins(key, len(key))
    pg_id = utils.ceph_stable_mod(raw_pg_id, pg_num, pg_num_mask)
time2 = time.time() 
print(time2-time1)

nodes = pg_Rendezvous_node.values()
time1 = time.time()
responsible_nodes = [determine_responsible_node(
     nodes, "key: {}".format(key)).id for key in range(obj_num)]
time2 = time.time()
print(time2-time1)
# print(Counter(responsible_nodes))


