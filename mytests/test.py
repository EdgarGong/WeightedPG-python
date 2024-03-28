import json
import time
from goto import with_goto
from crush import Crush
import utils

crushmap = """
{
  "trees": [
    {
      "type": "root", "name": "dc1", "id": -1,
      "children": [
        {
         "type": "host", "name": "host0", "id": -2,
         "children": [
          { "id": 0, "name": "device0", "weight": 65536 },
          { "id": 1, "name": "device1", "weight": 131072 }
         ]
        },
        {
         "type": "host", "name": "host1", "id": -3,
         "children": [
          { "id": 2, "name": "device2", "weight": 65536 },
          { "id": 3, "name": "device3", "weight": 131072 }
         ]
        },
        {
         "type": "host", "name": "host2", "id": -4,
         "children": [
          { "id": 4, "name": "device4", "weight": 65536 },
          { "id": 5, "name": "device5", "weight": 131072 }
         ]
        }
      ]
    }
  ],
  "rules": {
    "r": [
      [ "take", "dc1" ],
      [ "chooseleaf", "firstn", 0, "type", "host" ],
      [ "emit" ]
    ]
  }
}

"""


@with_goto
def main():
    c = Crush()

    str = "bnaio90hn.jfaf02%"

    time1 = time.time()
    print(utils.ceph_str_hash_rjenkins(str, len(str)))
    time2 = time.time()
    print(c.c.ceph_str_hash_rjenkins(str, len(str)))
    time3 = time.time()
    print(time2 - time1)
    print(time3 - time2)

    print(c.c.ceph_pool_pps(3, 1024, 1024))
    c.parse(json.loads(crushmap))
    print(c.map(rule="r", value=1234, replication_count=3))
    # goto .end
    # print(c.map(rule="r", value=1234, replication_count=3))
    # print(c.map(rule="r", value=1234, replication_count=3))
    # print(c.map(rule="r", value=124, replication_count=3))
    # label .end


main()
