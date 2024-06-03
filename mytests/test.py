import hashlib
import mmh3
import utils
from crush import Crush
import time
import json
# class hashtest:
#     def __init__(self):
#         self.c = Crush()
#         self.pg_num = 8192
#         self.pg_num_mask = 8191

#     def hash1(self, key):
#         raw_pg_id = self.c.c.ceph_str_hash_rjenkins(key, len(key))
#         return utils.ceph_stable_mod(raw_pg_id, self.pg_num, self.pg_num_mask)
    
#     def hash2(self, key):
#         return mmh3.hash(key) % self.pg_num
    
#     def hash3(self, key):
#         return hash(key) % self.pg_num
    
#     def hash4(self, key):
#         md5_hash = hashlib.md5(key).hexdigest()
#         # hash_object = hashlib.md5()
#         # hash_object.update(key)
#         # md5_hash = hash_object.hexdigest()
#         hashed_value_mod = int(md5_hash, 16) % self.pg_num
#         return hashed_value_mod
    
#     def hash5(self, key):
#         hash = hashlib.sha1(key).hexdigest()
#         hashed_value_mod = int(hash, 16) % self.pg_num
#         return hashed_value_mod

#     def hash6(self, key):
#         hash = hashlib.sha256(key).hexdigest()
#         hashed_value_mod = int(hash, 16) % self.pg_num
#         return hashed_value_mod
    
# h = hashtest()
# key = "100" + str('%08x' % 1) + '.' + str('%08x' % 2)          
# print(key)
# print(h.hash1(key))
# print(h.hash2(key))
# print(h.hash3(key))
# print(h.hash4(key))
# print(h.hash5(key))
# print(h.hash6(key))

# result = 3 ** 0.5
# print(result)



import json
import time
from goto import with_goto
from crush import Crush
import utils
import random
def gen_crushmap(n_host, n_disk):
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

    return crushmap

# crushmap = """
# {
#   "trees": [
#     {
#       "type": "root", "name": "dc1", "id": -1,
#       "children": [
#         {
#          "type": "host", "name": "host0", "id": -2,
#          "children": [
#           { "id": 0, "name": "device0", "weight": 65536 },
#           { "id": 1, "name": "device1", "weight": 131072 }
#          ]
#         },
#         {
#          "type": "host", "name": "host1", "id": -3,
#          "children": [
#           { "id": 2, "name": "device2", "weight": 65536 },
#           { "id": 3, "name": "device3", "weight": 131072 }
#          ]
#         },
#         {
#          "type": "host", "name": "host2", "id": -4,
#          "children": [
#           { "id": 4, "name": "device4", "weight": 65536 },
#           { "id": 5, "name": "device5", "weight": 131072 }
#          ]
#         }
#       ]
#     }
#   ],
#   "rules": {
#     "r": [
#       [ "take", "dc1" ],
#       [ "chooseleaf", "indep", 0, "type", "host" ],
#       [ "emit" ]
#     ]
#   }
# }

# """

crushmap = gen_crushmap(11, 1)

@with_goto

def main():
    c = Crush()

    str = "bnaio90hn.jfaf02%"

    # time1 = time.time()
    # print(utils.ceph_str_hash_rjenkins(str, len(str)))
    # time2 = time.time()
    # print(c.c.ceph_str_hash_rjenkins(str, len(str)))
    # time3 = time.time()
    # print(time2 - time1)
    # print(time3 - time2)

    # print(c.c.ceph_pool_pps(3, 1024, 1024))
    c.parse(crushmap)
    random_int = [348, 1874, 2102, 867, 542, 1838, 2064, 1142]
    print(random_int)
    for i in range (len(random_int)):
        print(c.map(rule="default_rule", value=random_int[i], replication_count=3))
    
    # print(c.map(rule="default_rule", value=1213, replication_count=3))
    # print(c.map(rule="default_rule", value=321, replication_count=3))
    # print(c.map(rule="default_rule", value=21312, replication_count=3))
    # goto .end
    # print(c.map(rule="r", value=1234, replication_count=3))
    # print(c.map(rule="r", value=1234, replication_count=3))
    # print(c.map(rule="r", value=124, replication_count=3))
    # label .end
    


main()
