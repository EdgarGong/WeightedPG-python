import json
import time
from goto import with_goto
from crush import Crush
def gen_pg_osd_map(pg_pri_num, pps):
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

    # print(json.dumps(crushmap))

    c = Crush()

    c.parse(crushmap)
    osds = c.map(rule="default_rule", value=pps, replication_count=1)
    return osds[0]
 
# n_host = 3
# n_disk = 1

# device_id = 0
# host_id = -2
# children = []
# for host in range(n_host):
#     host_dict = {}
#     host_dict["type"] = "host"
#     host_dict["name"] = "host" + str(host)
#     host_dict["id"] = host_id
#     host_id -= 1
#     host_dict_children = []
#     for i in range(n_disk):
#         disk_dict = {}
#         disk_dict["id"] = device_id
#         disk_dict["name"] = "device" + str(device_id)
#         disk_dict["weight"] = 1
#         device_id += 1
#         host_dict_children.append(disk_dict)
#     host_dict["children"] = host_dict_children

#     children.append(host_dict)

# trees = []
# trees_dict = {}
# trees_dict["type"] = "root"
# trees_dict["name"] = "default"
# trees_dict["id"] = -1
# trees_dict["children"] = children
# trees.append(trees_dict)

# print(trees)

# rules = {}
# default_rule_rep = [["take", "default"], ["chooseleaf", "firstn", 0, "type", "host"], ["emit"]]
# # default_rule_ec = [["take", "default"], ["choose", "indep", 5, "type", "host"], ["chooseleaf", "indep", 2, "type", 0], ["emit"]]
# default_rule_ec = [["take", "default"], ["chooseleaf", "indep", 0, "type", "host"], ["emit"]]

# rules["default_rule"] = default_rule_rep

# types = [{"type_id": 2, "name": "root"}, {"type_id": 1, "name": "host"}]

# crushmap = {}
# crushmap["trees"] = trees
# crushmap["rules"] = rules
# crushmap["types"] = types



def main():
    c = Crush()
    for i in range(1000, 1200):
        print(gen_pg_osd_map(100, i))

    # print(c.c.ceph_pool_pps(3, 1024, 1024))
    # c.parse(crushmap)
    # print(c.map(rule="default_rule", value=1234, replication_count=3))
    # goto .end
    # print(c.map(rule="r", value=1234, replication_count=3))
    # print(c.map(rule="r", value=1234, replication_count=3))
    # print(c.map(rule="r", value=124, replication_count=3))
    # label .end


main()