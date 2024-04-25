from __future__ import division

import copy
import json
import random
import numpy as np
from goto import with_goto
import time
import sys

from crush import Crush
import utils


class Logger:
    def __init__(self, path):
        self.fo = open(path, "a")
        msg = "========== start logging at " + time.asctime( time.localtime(time.time()) ) + " =========="
        self.log(msg)

    def __del__(self):
        msg = "========== stop logging at " + time.asctime( time.localtime(time.time()) ) + " =========="
        self.log(msg)
        self.fo.close()

    def log(self, msg):
        print(msg)
        msg = str(msg)
        msg += '\n'
        self.fo.write(msg)


class Incremental:
    def __init__(self):
        self.old_pg_upmap_items = []
        self.new_pg_upmap_items = {}


class OSDMap:
    def __init__(self):
        self.pg_upmap = {}
        self.pg_upmap_items = {}


class Balancer:
    def __init__(self):
#         self.crushmap = """
# {"rules": {"default_rule": [["take", "default"], ["chooseleaf", "firstn", 0, "type", "host"], ["emit"]]}, "types": [{"name": "root", "type_id": 2}, {"name": "host", "type_id": 1}], "trees": [{"children": [{"children": [{"id": 0, "weight": 1, "name": "device0"}, {"id": 1, "weight": 1, "name": "device1"}, {"id": 2, "weight": 1, "name": "device2"}], "type": "host", "name": "host0", "id": -2}, {"children": [{"id": 3, "weight": 1, "name": "device3"}, {"id": 4, "weight": 1, "name": "device4"}, {"id": 5, "weight": 1, "name": "device5"}], "type": "host", "name": "host1", "id": -3}, {"children": [{"id": 6, "weight": 1, "name": "device6"}, {"id": 7, "weight": 1, "name": "device7"}, {"id": 8, "weight": 1, "name": "device8"}], "type": "host", "name": "host2", "id": -4}, {"children": [{"id": 9, "weight": 1, "name": "device9"}, {"id": 10, "weight": 1, "name": "device10"}, {"id": 11, "weight": 1, "name": "device11"}], "type": "host", "name": "host3", "id": -5}], "type": "root", "name": "default", "id": -1}]}
# """
        self.n_host = 12
        self.n_disk = 32
        self.crushmap = self.gen_crushmap(self.n_host, self.n_disk)
        # print(self.crushmap)
        self.c = Crush()
        # self.crushmap_json = json.loads(self.crushmap)
        self.c.parse(self.crushmap)
        self.replication_count = 3
        # self.pg_pri_in_osd = {}
        # self.pg_rep_in_osd = {}
        self.pg_in_osd = {}
        self.pg_to_osds = {}
        self.osd_used_capacity = {}
        self.obj_in_pg = {}
        self.obj_num_in_pg = {}

        self.disk_num = self.n_host*self.n_disk
        # engine_num = disk_num, Each engine corresponds to one osd.
        # self.engine_num = self.disk_num
        # self.engine_id = [i for i in range(self.engine_num)]
        # self.obj_in_engine = {}

        self.obj_size = 4 #MB
        self.disk_size = 128 #GB

        self.overall_used_capacity = 0.7
        self.pg_num = int(100 * self.disk_num * self.overall_used_capacity / self.replication_count)  # Suggest PG Count = (Target PGs per OSD) x (OSD#) x (%Data) / (Size)
        self.pg_num = 1 << (self.pg_num.bit_length() - 1)
        # self.pg_num = 8192

        self.pg_num_mask = self.compute_pgp_num_mask(self.pg_num)

        self.pg_to_vpg_num = 8 # one pg to 4 virtual pgs
        self.vpg_num = self.pg_num * self.pg_to_vpg_num
        self.vpg_num_mask = self.compute_pgp_num_mask(self.vpg_num)

        # record the pg to vpg no, round robin
        self.pg_to_vpg_no = {}
        
        # print("pg num:", bin(self.pg_num), " pg num mask:", bin(self.pg_num_mask))

        self.max_optimizations = self.pg_num
        self.max_deviation = 1
        self.aggressive = True
        self.fast_aggressive = True
        self.local_fallback_retries = 100
        self.osdmap = OSDMap()

    def compute_pgp_num_mask(self, num):
        # Calculate the pgp_num_mask for given pgp_num
        return (1 << (num - 1).bit_length()) - 1
    
    def gen_crushmap(self, n_host, n_disk):
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

    def stripe_pg_to_osds(self):
        osd_id = 0
        for pg_no in range(self.pg_num):
            osds = []
            for i in range(self.replication_count):
                osds.append(osd_id)
                pgs = self.pg_in_osd.get(osd_id, [])
                pgs.append(pg_no)
                self.pg_in_osd[osd_id] = pgs
                osd_id = (osd_id + 1) % self.disk_num
            self.pg_to_osds[pg_no] = osds

    # map virtual pg to osd using CRUSH
    def crush_vpg_to_osds(self):
        for pg_no in range(self.vpg_num):
            pps = np.int32(
                utils.crush_hash32_rjenkins1_2(utils.ceph_stable_mod(pg_no, self.vpg_num, self.vpg_num_mask), 3))  # 3 is pool no., not rep count
            osds = self.c.map(rule="default_rule", value=pps, replication_count=self.replication_count)
            self.pg_to_osds[pg_no] = osds
            for i in range(len(osds)):
                pgs = self.pg_in_osd.get(osds[i], [])
                pgs.append(pg_no)
                self.pg_in_osd[osds[i]] = pgs
    
    def print_pg_in_osd(self):
        max_pg_num = 0
        max_osdid = 0
        min_pg_num = sys.maxsize
        min_osdid = 0
        avg_pg_num = float(self.vpg_num * self.replication_count) / self.disk_num
        for osd, pgs in self.pg_in_osd.items():
            pg_num = len(pgs)
            if pg_num >= max_pg_num:
                max_pg_num = pg_num
                max_osdid = osd
            if pg_num <= min_pg_num:
                min_pg_num = pg_num
                min_osdid = osd
            # logger.log("(osd, pg num):" + str(osd) + "," + str(len(pgs)))
        logger.log("virtual pg num in osd , MAX/MIN/AVG:" + str(max_pg_num) + "/" + str(min_pg_num) + "/" + str(avg_pg_num))
        logger.log("virtual pg num in osd deviation, MAX/MIN:" + str(max_pg_num/avg_pg_num) + "/" + str(min_pg_num/avg_pg_num))
        logger.log("virtual pg num in osd  MAX/MIN osd id:" + str(max_osdid) + "/" + str(min_osdid))
        return max_osdid, min_osdid

    def stripe_write_obj(self, inode_num, inode_size):
        pg_id = 0
        for inode_no in range(inode_num):
            # print(inode_num-inode_no)
            for block_no in range(inode_size):
                # key format: "10000000001.00000002"
                key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
                # append to obj_in_pg
                objs = self.obj_in_pg.get(pg_id, [])
                objs.append(key)
                self.obj_in_pg[pg_id] = objs
                # start crush
                osds = self.pg_to_osds.get(pg_id, [])
                for i in range(len(osds)):
                    used_capacity = self.osd_used_capacity.get(osds[i], 0)
                    used_capacity += self.obj_size
                    # if used_capacity >= self.disk_size*1024:
                        # print("overflow! osd id:", osds[i])
                    self.osd_used_capacity[osds[i]] = used_capacity
                
                pg_id = (pg_id + 1) % self.pg_num

    def write_obj(self, inode_num, inode_size):
        # object_num = 0
        for inode_no in range(inode_num):
            # print(inode_num-inode_no)
            for block_no in range(inode_size):
                # object_num = object_num + 1
                # key format: "10000000001.00000002"
                key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
                raw_pg_id = self.c.c.ceph_str_hash_rjenkins(key, len(key))
                pg_id = utils.ceph_stable_mod(raw_pg_id, self.pg_num, self.pg_num_mask)
                # append to obj_in_pg
                objs = self.obj_in_pg.get(pg_id, [])
                objs.append(key)
                self.obj_in_pg[pg_id] = objs
                # start crush
                # pg to vpg
                # round robin
                no = self.pg_to_vpg_no.get(pg_id, 0)
                vpg_id = pg_id + no * self.pg_num
                no = (no + 1) % self.pg_to_vpg_num
                self.pg_to_vpg_no[pg_id] = no
                osds = self.pg_to_osds.get(vpg_id, [])
                # make sure that every virtual pg map to {replication_count} osds
                assert len(osds) == self.replication_count

                # only record the capacity, but not execute the write operation
                for i in range(len(osds)):
                    used_capacity = self.osd_used_capacity.get(osds[i], 0)
                    used_capacity += self.obj_size
                    # if used_capacity >= self.disk_size*1024:
                        # print("overflow! osd id:", osds[i])
                    # print("osd id:", osds[i], " used capacity:", used_capacity)
                    self.osd_used_capacity[osds[i]] = used_capacity
        # for i in range(self.disk_num):
        #     print("osd id:", i, " used capacity:", self.osd_used_capacity.get(i, 0))
        # print("write object num", object_num)

    def write_obj_with_pg_num(self, inode_num, inode_size):
        object_num = inode_num*inode_size
        # Calculate the pg average of the osd corresponding to each pg
        pg_to_avg = {}
        for pg, osds in self.pg_to_osds.items():
            total_pg = 0
            for osd in osds:
                total_pg += len(self.pg_in_osd[osd])
            pg_to_avg[pg] = total_pg / len(osds)
        # Sort by average in ascending order
        sorted_pg = sorted(pg_to_avg.items(), key=lambda x: x[1])
        # print(sorted_pg)
        # Calculate the sum of all values
        total_value = sum([v for _, v in sorted_pg])
        print(total_value)
        # Then calculate the average value
        average_value = total_value / len(sorted_pg)
        # Divide each value in sorted_pg by the average value
        normalized_pg = [(pg, v/average_value) for pg, v in sorted_pg]
        # print(normalized_pg)

        pg_ids = [pg_id for pg_id, _ in normalized_pg]
        values = [value for _, value in normalized_pg]
        pg_ids.reverse()
        avg_obj_in_pg = object_num / self.pg_num

        max_obj = 0
        min_obj = sys.maxsize
        obj_sum = 0
        for i in range(len(pg_ids)):
            pg_id = pg_ids[i]
            value = values[i]
            # print(pg_id, value)
            obj_num = int(avg_obj_in_pg * value * value)
            max_obj = max(max_obj, obj_num)
            min_obj = min(min_obj, obj_num)
            obj_sum += obj_num
            self.obj_num_in_pg[pg_id] = obj_num
            # print("pgid:", pg_id, " object num:", obj_num)
            # start crush
            osds = self.pg_to_osds.get(pg_id, [])
            for i in range(len(osds)):
                used_capacity = self.osd_used_capacity.get(osds[i], 0)
                used_capacity += (self.obj_size * obj_num)
                # if used_capacity >= self.disk_size*1024:
                    # print("overflow! osd id:", osds[i])
                self.osd_used_capacity[osds[i]] = used_capacity

        print("object sum:", obj_sum)
        avg_obj = float(obj_sum) / self.pg_num
        logger.log("objects in PGs MIN/MAX/AVG objects number: " + str(min_obj) + "/" + str(max_obj) + "/" + str(avg_obj))
        
    def print_osd_used_capacity(self, max_osd_id, min_osd_id):
        max_used_capacity = 0
        max_osdid = 0
        min_used_capacity = sys.maxsize
        min_osdid = 0
        obj_sum = 0
        for osd, used in self.osd_used_capacity.items():
            obj_sum += used
            # used_capacity = used/(self.disk_size * 1024)
            # print("osd id:", osd, "used capacity:", used_capacity)
            if max_used_capacity <= used:
                max_used_capacity = used
                max_osdid = osd
            if used <= min_used_capacity:
                min_used_capacity = used
                min_osdid = osd
        avg_capacity = float(obj_sum) / self.disk_num
        logger.log("MAX/MIN used capacity:" + str(float(max_used_capacity)/(self.disk_size * 1024))+ "/" + str(float(min_used_capacity)/(self.disk_size * 1024)))
        # print("max used capacity:", float(max_used_capacity)/(self.disk_size * 1024), "min used capacity:", float(min_used_capacity)/(self.disk_size * 1024))
        logger.log("MAX/MIN used capacity deviation:" + str(max_used_capacity/avg_capacity)+ "/" + str(min_used_capacity/avg_capacity))
        # print("max used capacity deviation:", max_used_capacity/avg_capacity, "min used capacity deviation:", min_used_capacity/avg_capacity)
        logger.log("MAX/MIN used osd id:" + str(max_osdid)+ "/" + str(min_osdid))
        # print("max used osd id:", max_osdid, "min used osd id:", min_osdid)
        logger.log("MAX/MIN pg osd's used deviation:" + str(float(self.osd_used_capacity[max_osd_id]) / avg_capacity)+ "/" + str(float(self.osd_used_capacity[min_osd_id]) / avg_capacity))
        # print("max_pg_osd's used deviation:", float(self.osd_used_capacity[max_osd_id]) / avg_capacity)
        # print("min_pg_osd's used deviation:", float(self.osd_used_capacity[min_osd_id]) / avg_capacity)

    
    def print_obj_in_pg_number(self):
        max_obj = 0
        min_obj = len(self.obj_in_pg[0])
        obj_sum = 0
        for pgid, objs in self.obj_in_pg.items():
            obj_count = len(objs)
            max_obj = max(max_obj, obj_count)
            min_obj = min(min_obj, obj_count)
            obj_sum += obj_count
            # logger.log("pgid:" + str(pgid) + " object count:" + str(obj_count))
        avg_obj = float(obj_sum) / self.pg_num
        logger.log("objects in PGs MIN/MAX/AVG objects number: " + str(min_obj) + "/" + str(max_obj) + "/" + str(avg_obj))
        
        # pg_counts = {pg: len(objs) for pg, objs in self.obj_in_pg.items()}

        # max_objects = max(pg_counts.values())
        # scale_factor = 50.0 / max_objects  # Adjust scale to fit your console width

        # for pg, num_objs in pg_counts.items():
        #     bar_length = int(num_objs * scale_factor)
        #     logger.log("PG {}: {} ({} objects)".format(pg, '*' * bar_length, num_objs))

logger = Logger("raw_crush_log.txt")
b = Balancer()
print("pg num:", b.pg_num)
print("pg num mask:", b.pg_num_mask)
print("virtual pg num:", b.vpg_num)
# disk_load = 12288  # GB
# inode_size = 4096
inode_size = 4096
# inode_num = int(b.disk_num * disk_load / inode_size / b.replication_count * 256)
disk_capacity = b.disk_num*b.disk_size #GB
overall_utilization = b.overall_used_capacity
obj_load_num = int(disk_capacity*overall_utilization*1024/b.replication_count/b.obj_size)
# obj_load_num = 2936012

inode_num = int(obj_load_num/inode_size)
print("inode number:", inode_num)
print("object load number:", inode_num * inode_size)
# each inode has {inode_size} blocks
# each block is a object
time1 = time.time()
b.crush_vpg_to_osds()
# b.stripe_pg_to_osds()

time2 = time.time()
b.write_obj(inode_num, inode_size)
# b.write_obj_with_pg_num(inode_num, inode_size)
# b.stripe_write_obj(inode_num, inode_size)
time3 = time.time()
print("crush time:", time2-time1," write object time:", time3-time2)
max_osd_id, min_osd_id = b.print_pg_in_osd()
# b.print_obj_in_pg_number()
b.print_osd_used_capacity(max_osd_id, min_osd_id)