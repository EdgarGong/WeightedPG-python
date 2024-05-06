import hashlib
import mmh3
import utils
from crush import Crush
class hashtest:
    def __init__(self):
        self.c = Crush()
        self.pg_num = 8192
        self.pg_num_mask = 8191

    def hash1(self, key):
        raw_pg_id = self.c.c.ceph_str_hash_rjenkins(key, len(key))
        return utils.ceph_stable_mod(raw_pg_id, self.pg_num, self.pg_num_mask)
    
    def hash2(self, key):
        return mmh3.hash(key) % self.pg_num
    
    def hash3(self, key):
        return hash(key) % self.pg_num
    
    def hash4(self, key):
        md5_hash = hashlib.md5(key).hexdigest()
        # hash_object = hashlib.md5()
        # hash_object.update(key)
        # md5_hash = hash_object.hexdigest()
        hashed_value_mod = int(md5_hash, 16) % self.pg_num
        return hashed_value_mod
    
    def hash5(self, key):
        hash = hashlib.sha1(key).hexdigest()
        hashed_value_mod = int(hash, 16) % self.pg_num
        return hashed_value_mod

    def hash6(self, key):
        hash = hashlib.sha256(key).hexdigest()
        hashed_value_mod = int(hash, 16) % self.pg_num
        return hashed_value_mod
    
h = hashtest()
key = "100" + str('%08x' % 1) + '.' + str('%08x' % 2)          
print(key)
print(h.hash1(key))
print(h.hash2(key))
print(h.hash3(key))
print(h.hash4(key))
print(h.hash5(key))
print(h.hash6(key))


# import json
# import time
# from goto import with_goto
# from crush import Crush
# import utils

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
#       [ "chooseleaf", "firstn", 0, "type", "host" ],
#       [ "emit" ]
#     ]
#   }
# }

# """


# @with_goto
# def main():
#     c = Crush()

#     str = "bnaio90hn.jfaf02%"

#     time1 = time.time()
#     print(utils.ceph_str_hash_rjenkins(str, len(str)))
#     time2 = time.time()
#     print(c.c.ceph_str_hash_rjenkins(str, len(str)))
#     time3 = time.time()
#     print(time2 - time1)
#     print(time3 - time2)

#     print(c.c.ceph_pool_pps(3, 1024, 1024))
#     c.parse(json.loads(crushmap))
#     print(c.map(rule="r", value=1234, replication_count=3))
#     # goto .end
#     # print(c.map(rule="r", value=1234, replication_count=3))
#     # print(c.map(rule="r", value=1234, replication_count=3))
#     # print(c.map(rule="r", value=124, replication_count=3))
#     # label .end


# main()
