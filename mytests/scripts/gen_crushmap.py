import json

n_host = 4
n_disk = 3

device_id = 0
host_id = -2
children = []
for host in range(n_host):
    host_dict = {}
    host_dict["type"] = "host"
    host_dict["name"] = "host" + str(host)
    host_dict["id"] = host_id
    host_id -= 1
    host_dict_children = []
    for i in range(n_disk):
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

print(json.dumps(crushmap))
