import json
import time
from goto import with_goto
from crush import Crush
import utils
import numpy as np
import collections
import logging
import pandas as pd
import textwrap

log = logging.getLogger(__name__)

rep_type = "ec"

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

    if rep_type == "rep":
        rules["default_rule"] = default_rule_rep
    else:
        rules["default_rule"] = default_rule_rep

    types = [{"type_id": 2, "name": "root"}, {"type_id": 1, "name": "host"}]

    crushmap = {}
    crushmap["trees"] = trees
    crushmap["rules"] = rules
    crushmap["types"] = types
    return crushmap
  
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

  if rep_type == "rep":
        rules["default_rule"] = default_rule_rep
  else:
      rules["default_rule"] = default_rule_rep
  # rules["default_rule"] = default_rule_rep

  types = [{"type_id": 2, "name": "root"}, {"type_id": 1, "name": "host"}]

  crushmap = {}
  crushmap["trees"] = trees
  crushmap["rules"] = rules
  crushmap["types"] = types

  return crushmap
      
      
def crushmap_add_host(crushmap, add_host, disk_per_host):
  device_id = 0
  trees = crushmap["trees"]
  children = trees[0]["children"]
  
  device_id = 0
  host_id = -2
  host_no = 0
  
  for host_dict in children:
    host_dict_children = host_dict["children"]
    host_id = min(host_id, host_dict["id"])
    host_no += 1
    for disk_dict in host_dict_children:
      device_id = max(device_id, disk_dict["id"])
  
  print("device_id", device_id)
  device_id += 1
  host_id -= 1
  
    
  for host in range(add_host):
      host_dict = {}
      host_dict["type"] = "host"
      host_dict["name"] = "host" + str(host_no + host)
      host_dict["id"] = host_id
      host_id -= 1
      host_dict_children = []
      for i in range(disk_per_host):
          disk_dict = {}
          disk_dict["id"] = device_id
          disk_dict["name"] = "device" + str(device_id)
          disk_dict["weight"] = 1
          device_id += 1
          host_dict_children.append(disk_dict)
      host_dict["children"] = host_dict_children
      children.append(host_dict)
  
  trees[0]["children"] = children
  crushmap["trees"] = trees

  return crushmap

def compare():
  pg_num = 32
  pg_num_mask = 31
  pg_to_osds = {}
  pg_in_osd = {}
  replication_count = 3
  
  origin_d = collections.defaultdict(lambda: 0)
  destination_d = collections.defaultdict(lambda: 0)
  from_to = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))    
  am = gen_crushmap(3, 3)
  a = Crush()
  a.parse(am)
  
  bm = crushmap_add_host(am, 3, 3)
  b = Crush()
  b.parse(bm)
  for pg_no in range(pg_num):
    pps = np.int32(
        utils.crush_hash32_rjenkins1_2(utils.ceph_stable_mod(pg_no, pg_num, pg_num_mask), 3))  # 3 is pool no., not rep count
    am = a.map(rule="default_rule", value=pps, replication_count=replication_count)
    print("am {} == {} mapped to {}".format(pg_no, pps, am))
    assert len(am) == replication_count
    for d in am:
        origin_d[d] += 1
    bm = b.map(rule="default_rule", value=pps, replication_count=replication_count)
    print("bm {} == {} mapped to {}".format(pg_no, pps, bm))
    assert len(bm) == replication_count
    for d in bm:
        destination_d[d] += 1
    if rep_type == "ec":
        for i in range(len(am)):
            if am[i] != bm[i]:
                from_to[am[i]][bm[i]] += 1
    else:
        am = set(am)
        bm = set(bm)
        if am == bm:
            continue
        ar = list(am - bm)
        br = list(bm - am)
        for i in range(len(ar)):
            from_to[ar[i]][br[i]] += 1
    
  
  # print(from_to)
  # display

  out = ""
  o = pd.Series(origin_d)
  objects_count = o.sum()
  n = 'pgs'
  out += "There are {} pgs.\n".format(objects_count)
  m = pd.DataFrame.from_dict(from_to, dtype=int).fillna(0).T.astype(int)
  objects_moved = m.sum().sum()
  objects_moved_percent = objects_moved / objects_count * 100
  out += textwrap.dedent("""
  Replacing the crushmap specified with --origin with the crushmap
  specified with --destination will move {} pgs ({}% of the total)
  from one item to another.
  """.format(int(objects_moved),  objects_moved_percent))
  from_to_percent = m.sum(axis=1) / objects_count
  to_from_percent = m.sum() / objects_count
  m[n + '%'] = from_to_percent.apply(lambda v: "{:.2%}".format(v))
  mt = m.T
  mt[n + '%'] = to_from_percent.apply(lambda v: "{:.2%}".format(v))
  m = mt.T.fillna("{:.2f}%".format(objects_moved_percent))
  out += textwrap.dedent("""
  The rows below show the number of {name} moved from the given
  item to each item named in the columns. The {name}% at the
  end of the rows shows the percentage of the total number
  of {name} that is moved away from this particular item. The
  last row shows the percentage of the total number of {name}
  that is moved to the item named in the column.

  """.format(name=n))
  pd.set_option('display.max_rows', None)
  pd.set_option('display.width', 160)
  out += str(m)
  # return out
  print(out)
    
  
compare()
exit(0)
  


pg_num = 32
pg_num_mask = 31
pg_to_osds = {}
pg_in_osd = {}
replication_count = 1

crushmap = gen_crushmap(3, 3)
print("crushmap", crushmap)
c = Crush()
c.parse(crushmap)
for pg_no in range(pg_num):
  pps = np.int32(
      utils.crush_hash32_rjenkins1_2(utils.ceph_stable_mod(pg_no, pg_num, pg_num_mask), 3))  # 3 is pool no., not rep count
  osds = c.map(rule="default_rule", value=pps, replication_count=replication_count)
  pg_to_osds[pg_no] = osds
  for i in range(len(osds)):
      pgs = pg_in_osd.get(osds[i], [])
      pgs.append(pg_no)
      pg_in_osd[osds[i]] = pgs

# for(pg_no, osds) in pg_to_osds.items():
#   print(pg_no, osds)

new_crushmap = crushmap_add_host(crushmap, 3, 3)
print("new crushmap", new_crushmap)
c.parse(crushmap)
new_pg_to_osds = {}
new_pg_in_osd = {}
for pg_no in range(pg_num):
  pps = np.int32(
      utils.crush_hash32_rjenkins1_2(utils.ceph_stable_mod(pg_no, pg_num, pg_num_mask), 3))  # 3 is pool no., not rep count
  osds = c.map(rule="default_rule", value=pps, replication_count=replication_count)
  new_pg_to_osds[pg_no] = osds
  for i in range(len(osds)):
      pgs = new_pg_in_osd.get(osds[i], [])
      pgs.append(pg_no)
      new_pg_in_osd[osds[i]] = pgs
      
print("new crush")
diff_count_map = {}
for(pg_no, osds) in new_pg_to_osds.items():
  old_osds = pg_to_osds.get(pg_no, [])
  diff_count = 0
  for osd in osds:
    if osd not in old_osds:
      diff_count += 1
      if osd <= 8:
        print("new map to old osd! pg_no", pg_no, "old osd", osd)
  print("old", pg_no, old_osds, " new", pg_no, osds, " different count:", diff_count)
  diff_count_map[diff_count] = diff_count_map.get(diff_count, 0) + 1

print("diff_count_map", diff_count_map)
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