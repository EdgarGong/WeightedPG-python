import json
import time
from goto import with_goto
from crush import Crush
import utils
import numpy as np

# str = "bnaio90hn.jfaf02%"


# c = Crush()
# time1 = time.time()
# print(utils.ceph_str_hash_rjenkins(str, len(str)) % l)
# time2 = time.time()
# print(c.c.ceph_str_hash_rjenkins(str, len(str)) % l)
# time3 = time.time()
# print(hash(str) % l)

def gen_pg_crushmap(pg_pri_num):
    device_id = 0
    host_id = -2
    children = []
    for host in range(pg_pri_num):
        host_dict = {}
        host_dict["type"] = "host"
        host_dict["name"] = "host" + str(host)
        host_dict["id"] = host_id
        host_id -= 1
        host_dict_children = []
        disk_dict = {}
        disk_dict["id"] = device_id
        disk_dict["name"] = "device" + str(device_id)
        disk_dict["weight"] = 1
        device_id += 1
        host_dict_children.append(disk_dict)
        host_dict["children"] = host_dict_children
        children.append(host_dict)

    trees = []
    trees_dict = {}
    trees_dict["type"] = "root"
    trees_dict["name"] = "default"
    trees_dict["id"] = -1
    trees_dict["children"] = children
    trees.append(trees_dict)

    rules = {}
    default_rule_rep = [["take", "default"], ["chooseleaf", "firstn", 0, "type", "host"], ["emit"]]
    # default_rule_ec = [["take", "default"], ["choose", "indep", 5, "type", "host"], ["chooseleaf", "indep", 2, "type", 0], ["emit"]]
    default_rule_ec = [["take", "default"], ["chooseleaf", "indep", 0, "type", "host"], ["emit"]]

    rules["default_rule"] = default_rule_rep

    types = [{"type_id": 2, "name": "root"}, {"type_id": 1, "name": "host"}]

    crushmap = {}
    crushmap["trees"] = trees
    crushmap["rules"] = rules
    crushmap["types"] = types
    return crushmap

pg_num = 32
crushmap = gen_pg_crushmap(pg_num)
# print(crushmap)
pg_ids = {}
c = Crush()
c.parse(crushmap)
for i in range(100):
    osds = c.map(rule="default_rule", value=i, replication_count=1)
    pg_id = osds[0]
    pg_ids[pg_id] = True

print("pg_num, len(pg_ids)", pg_num, len(pg_ids))