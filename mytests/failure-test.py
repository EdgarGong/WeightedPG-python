from __future__ import division

import mmh3
import math
import copy
import json
import random
import numpy as np
from goto import with_goto
import time
import sys

from crush import Crush
import utils

import hashlib

import pickle


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


class rendezvousNode:
    """Class representing a node that is assigned keys as part of a weighted rendezvous hash."""
    def __init__(self, id, weight):
        self.id = id
        self.weight = weight

    def compute_weighted_score(self, key):
        score = hash_to_unit_interval("%d: %s" % (self.id, key))
        log_score = 1.0 / -math.log(score)
        return self.weight * log_score

def hash_to_unit_interval(id):
    """Hashes a string onto the unit interval (0, 1]"""
    return (float(mmh3.hash128(id)) + 1) / 2**128

def determine_responsible_node(nodes, key):
    """Determines which node of a set of nodes of various weights is responsible for the provided key."""
    return max(nodes, key=lambda node: node.compute_weighted_score(key)) if nodes else None


class Balancer:
    def __init__(self):
#         self.crushmap = """
# {"rules": {"default_rule": [["take", "default"], ["chooseleaf", "firstn", 0, "type", "host"], ["emit"]]}, "types": [{"name": "root", "type_id": 2}, {"name": "host", "type_id": 1}], "trees": [{"children": [{"children": [{"id": 0, "weight": 1, "name": "device0"}, {"id": 1, "weight": 1, "name": "device1"}, {"id": 2, "weight": 1, "name": "device2"}], "type": "host", "name": "host0", "id": -2}, {"children": [{"id": 3, "weight": 1, "name": "device3"}, {"id": 4, "weight": 1, "name": "device4"}, {"id": 5, "weight": 1, "name": "device5"}], "type": "host", "name": "host1", "id": -3}, {"children": [{"id": 6, "weight": 1, "name": "device6"}, {"id": 7, "weight": 1, "name": "device7"}, {"id": 8, "weight": 1, "name": "device8"}], "type": "host", "name": "host2", "id": -4}, {"children": [{"id": 9, "weight": 1, "name": "device9"}, {"id": 10, "weight": 1, "name": "device10"}, {"id": 11, "weight": 1, "name": "device11"}], "type": "host", "name": "host3", "id": -5}], "type": "root", "name": "default", "id": -1}]}
# """
        with open("config/config.json") as f:
            config = json.load(f)
        self.n_host = config['host_num']
        self.n_disk = config['disk_num']

        self.crush_type = config['type']

        if self.crush_type == 'ec':
            self.k = config['k']
            self.m = config['m']
            self.replication_count = self.k + self.m
        elif self.crush_type == 'rep':
            self.replication_count = config['replicated_num']

        self.obj_size = config['obj_size'] #MB
        self.disk_size = config['disk_size'] #GB

        self.overall_used_capacity = config['overall_used_capacity']
        self.disk_num = self.n_host*self.n_disk
        # engine_num = disk_num, Each engine corresponds to one osd.
        # self.engine_num = self.disk_num
        # self.engine_id = [i for i in range(self.engine_num)]
        # self.obj_in_engine = {}

        
        # self.pg_num = int(100 * self.disk_num * self.overall_used_capacity / self.replication_count)  # Suggest PG Count = (Target PGs per OSD) x (OSD#) x (%Data) / (Size)
        # self.pg_num = 1 << (self.pg_num.bit_length() - 1)
        self.pg_num = config['pg_num']


        self.pg_num_mask = self.compute_pgp_num_mask()
        
        # print("pg num:", bin(self.pg_num), " pg num mask:", bin(self.pg_num_mask))
        
        # self.pg_pri_in_osd = {}
        # self.pg_rep_in_osd = {}
        self.pg_in_osd = {}
        self.new_pg_in_osd = {}
        self.pg_to_osds = {}
        self.new_pg_to_osds = {}
        self.osd_used_capacity = {}
        self.new_osd_used_capacity = {}

        self.obj_in_pg = {}
        self.new_obj_in_pg = {}
        self.obj_num_in_pg = {}

        self.pg_to_avg = {}
        self.new_pg_to_avg = {}

        
        self.max_optimizations = self.pg_num
        self.max_deviation = 1
        self.aggressive = True
        self.fast_aggressive = True
        self.local_fallback_retries = 100
        self.osdmap = OSDMap()

        self.pg_weight = {}
        self.new_pg_weight = {}

        self.pg_Rendezvous_node = {}
        self.new_pg_Rendezvous_node = {}

        self.grouped_data = {}
        self.group_Rendezvous_node = {}

    def compute_pgp_num_mask(self):
        # Calculate the pgp_num_mask for given pgp_num
        return (1 << (self.pg_num - 1).bit_length()) - 1
    
    def gen_crushmap(self, n_host, n_disk, fail_type):
        device_id = 0
        host_id = -2
        children = []
        for host in range(n_host):
            if fail_type == "host":
                if host == n_host-1:
                    continue
            host_dict = {}
            host_dict["type"] = "host"
            host_dict["name"] = "host" + str(host)
            host_dict["id"] = host_id
            host_id -= 1
            host_dict_children = []
            for i in range(n_disk):
                if fail_type == "disk":
                    if host == n_host-1 and i == n_disk-1:
                        continue
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

        if self.crush_type == 'ec':
            rules["default_rule"] = default_rule_ec
        elif self.crush_type == 'rep':
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

    def crush_pg_to_osds(self):
        self.pg_in_osd.clear()
        self.pg_to_osds.clear()
        for pg_no in range(self.pg_num):
            pps = np.int32(
                utils.crush_hash32_rjenkins1_2(utils.ceph_stable_mod(pg_no, self.pg_num, self.pg_num_mask), 3))  # 3 is pool no., not rep count
            osds = self.c.map(rule="default_rule", value=pps, replication_count=self.replication_count)
            self.pg_to_osds[pg_no] = osds
            for i in range(len(osds)):
                pgs = self.pg_in_osd.get(osds[i], [])
                pgs.append(pg_no)
                self.pg_in_osd[osds[i]] = pgs

    def are_lists_equal(self, list1, list2):
        return sorted(list1) == sorted(list2)

    def count_different_elements(list1, list2):
        # Convert lists to sets
        set1 = set(list1)
        set2 = set(list2)
        
        # Find different elements using symmetric difference operation
        different_elements_set = set1 ^ set2
        
        # Return the number of different elements
        return len(different_elements_set)
    
    def check_crush_pg_to_osds(self):
        equal_count = 0
        diff_map = {}
        for pg_no in range(self.pg_num):
            pps = np.int32(
                utils.crush_hash32_rjenkins1_2(utils.ceph_stable_mod(pg_no, self.pg_num, self.pg_num_mask), 3))  # 3 is pool no., not rep count
            osds = self.c.map(rule="default_rule", value=pps, replication_count=self.replication_count)
            self.new_pg_to_osds[pg_no] = osds
            if self.are_lists_equal(self.pg_to_osds[pg_no], osds):
                equal_count += 1
            for i in range(len(osds)):
                pgs = self.new_pg_in_osd.get(osds[i], [])
                pgs.append(pg_no)
                self.new_pg_in_osd[osds[i]] = pgs
        print("equal_count:", equal_count, " total pg count:", self.pg_num)

    def check_pg_in_osd(self):
        count = 0
        pg_num_change_count = {}
        for osd, pgs in self.new_pg_in_osd.items():
            before = len(self.pg_in_osd[osd])
            after = len(pgs)
            if before != after:
                count += 1
                diff = (before-after)
                tmp = pg_num_change_count.get(diff, 0)
                tmp += 1
                pg_num_change_count[diff] = tmp
                # print("before pg num:", before, " after pg num:", after)
        print("different pg num osd count:", count, " total osd num:", self.disk_num)
        for diff, tmp in pg_num_change_count.items():
            print("diff:", diff, " count:", tmp)

    def print_pg_in_osd(self):
        max_pg_num = 0
        max_osdid = 0
        min_pg_num = sys.maxsize
        min_osdid = 0
        avg_pg_num = float(self.pg_num * self.replication_count) / self.disk_num
        for osd, pgs in self.pg_in_osd.items():
            pg_num = len(pgs)
            if pg_num >= max_pg_num:
                max_pg_num = pg_num
                max_osdid = osd
            if pg_num <= min_pg_num:
                min_pg_num = pg_num
                min_osdid = osd
            # logger.log("(osd, pg num):" + str(osd) + "," + str(len(pgs)))
        logger.log("pg num in osd , MAX/MIN/AVG:" + str(max_pg_num) + "/" + str(min_pg_num) + "/" + str(avg_pg_num))
        logger.log("pg num in osd deviation, MAX/MIN:" + str(max_pg_num/avg_pg_num) + "/" + str(min_pg_num/avg_pg_num))
        logger.log("pg num in osd  MAX/MIN osd id:" + str(max_osdid) + "/" + str(min_osdid))
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
                    if self.crush_type == 'ec':
                        used_capacity += (self.obj_size / self.k)
                    elif self.crush_type == 'rep':
                        used_capacity += self.obj_size
                    # if used_capacity >= self.disk_size*1024:
                        # print("overflow! osd id:", osds[i])
                    self.osd_used_capacity[osds[i]] = used_capacity
                
                pg_id = (pg_id + 1) % self.pg_num

    def write_obj(self, inode_num, inode_size):
        for inode_no in range(inode_num):
            # print(inode_num-inode_no)
            for block_no in range(inode_size):
                # key format: "10000000001.00000002"
                key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
                raw_pg_id = self.c.c.ceph_str_hash_rjenkins(key, len(key))
                pg_id = utils.ceph_stable_mod(raw_pg_id, self.pg_num, self.pg_num_mask)
                # append to obj_in_pg
                objs = self.obj_in_pg.get(pg_id, [])
                objs.append(key)
                self.obj_in_pg[pg_id] = objs
                # start crush
                osds = self.pg_to_osds.get(pg_id, [])
                assert len(osds) == self.replication_count
                for i in range(len(osds)):
                    used_capacity = self.osd_used_capacity.get(osds[i], 0)
                    if self.crush_type == 'ec':
                        used_capacity += (self.obj_size / self.k)
                    elif self.crush_type == 'rep':
                        used_capacity += self.obj_size
                    # if used_capacity >= self.disk_size*1024:
                        # print("overflow! osd id:", osds[i])
                    self.osd_used_capacity[osds[i]] = used_capacity

    def group_data(self, data, num_groups):
        sorted_data = sorted(data.items(), key=lambda x: x[1])
        min_value = sorted_data[0][1]
        max_value = sorted_data[-1][1]
        interval_width = (max_value - min_value) / num_groups

        groups = {}
    
        for key, value in sorted_data:
            group_index = int((value - min_value) // interval_width)
            if group_index == num_groups:
                group_index -= 1
            group = groups.get(group_index, [])
            group.append((key, value))
            groups[group_index] = group
        
        # groups = [group for group in groups if len(group) != 0]
        
        return groups
    
    
    # calculate pg weight
    def cal_pg_weight(self, power):
        # Calculate the average pg num of the osd corresponding to each pg
        # pg_to_avg = {}
        for pg, osds in self.pg_to_osds.items():
            total_pg = 0
            for osd in osds:
                total_pg += len(self.pg_in_osd[osd])
            self.pg_to_avg[pg] = total_pg / len(osds)

        for pg, v in self.pg_to_avg.items():
            self.pg_weight[pg] = (1/v) ** power
        # # print(normalized_pg)
            
    # new calculate pg weight
    def new_cal_pg_weight(self, power):
        avg_pg_num_count = {}
        # Calculate the average pg num of the osd corresponding to each pg
        # pg_to_avg = {}
        for pg, osds in self.new_pg_to_osds.items():
            total_pg = 0
            for osd in osds:
                total_pg += len(self.new_pg_in_osd[osd])
            self.new_pg_to_avg[pg] = total_pg / len(osds)
            before = self.pg_to_avg[pg]
            after = self.new_pg_to_avg[pg]
            if before != after:
                diff = abs(before-after)
                if diff < 1:
                    tmp = avg_pg_num_count.get(1, 0)
                    tmp += 1
                    avg_pg_num_count[1] = tmp
                elif diff >=1 and diff < 2:
                    tmp = avg_pg_num_count.get(2, 0)
                    tmp += 1
                    avg_pg_num_count[2] = tmp
                elif diff >=2 and diff < 5:
                    tmp = avg_pg_num_count.get(5, 0)
                    tmp += 1
                    avg_pg_num_count[5] = tmp
                elif diff >=5 and diff < 10:
                    tmp = avg_pg_num_count.get(10, 0)
                    tmp += 1
                    avg_pg_num_count[10] = tmp
                else:
                    tmp = avg_pg_num_count.get(100, 0)
                    tmp += 1
                    avg_pg_num_count[100] = tmp
                # print("before avg pg num:", self.pg_to_avg[pg], " after avg pg num:", self.new_pg_to_avg[pg])
            else:
                tmp = avg_pg_num_count.get(0, 0)
                tmp += 1
                avg_pg_num_count[0] = tmp
        for avg, count in avg_pg_num_count.items():
            print("avg pg num diff:", avg, " count:", count)

        for pg, v in self.new_pg_to_avg.items():
            self.new_pg_weight[pg] = (1/v) ** power
        # # print(normalized_pg)
            
    # calculate pg weight
    def check_pg_weight(self, power):
        equal_count = 0
        # Calculate the average pg num of the osd corresponding to each pg
        pg_to_avg = {}
        for pg, osds in self.pg_to_osds.items():
            total_pg = 0
            for osd in osds:
                total_pg += len(self.pg_in_osd[osd])
            pg_to_avg[pg] = total_pg / len(osds)
        for pg, v in pg_to_avg.items():
            value = (1/v) ** power
            if self.pg_weight[pg] == value:
                equal_count += 1
            else:
                print("before:", self.pg_weight[pg]*100, " after:", value*100)
        # print(normalized_pg)
        print("equal_count:", equal_count)
            

    def init_pg_Rendezvous_node(self):
        assert len(self.pg_weight) > 0
        for pgid, v in self.pg_weight.items():
            self.pg_Rendezvous_node[pgid] = rendezvousNode(pgid, v)
    
    def new_init_pg_Rendezvous_node(self):
        assert len(self.new_pg_weight) > 0
        for pgid, v in self.new_pg_weight.items():
            self.new_pg_Rendezvous_node[pgid] = rendezvousNode(pgid, v)

    def init_pg_group_Rendezvous_node(self):
        assert len(self.pg_weight) > 0
        self.grouped_data = self.group_data(self.pg_weight, 100)
        for groupid, group in self.grouped_data.items():
            values = [pair[1] for pair in group]
            # all pgs in this group use the mean weight as their weight
            mean_weight = np.sum(values)
            self.group_Rendezvous_node[groupid] = rendezvousNode(groupid, mean_weight)

    # use Rendezvous hash. Each pg corresponds to a node
    def write_obj_with_Rendezvous(self, inode_num, inode_size):
        self.cal_pg_weight(3)
        self.init_pg_Rendezvous_node()
        count = 0
        for inode_no in range(inode_num):
            # print(inode_num-inode_no)
            for block_no in range(inode_size):
                count = count + 1
                if count % 10000 == 0:
                    sys.stdout.write("\r%d" % count)  
                    sys.stdout.flush() 

                # key format: "10000000001.00000002"
                key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
                pg_node = determine_responsible_node(self.pg_Rendezvous_node.values(), key)
                
                pg_id = pg_node.id

                # append to obj_in_pg
                objs = self.obj_in_pg.get(pg_id, [])
                objs.append(key)
                self.obj_in_pg[pg_id] = objs

                # start crush
                osds = self.pg_to_osds.get(pg_id, [])
                assert len(osds) == self.replication_count
                for i in range(len(osds)):
                    used_capacity = self.osd_used_capacity.get(osds[i], 0)
                    used_capacity += self.obj_size
                    # if used_capacity >= self.disk_size*1024:
                        # print("overflow! osd id:", osds[i])
                    self.osd_used_capacity[osds[i]] = used_capacity
    
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
    
    def hash7(self, key):
        hash = hashlib.sha384(key).hexdigest()
        hashed_value_mod = int(hash, 16) % self.pg_num
        return hashed_value_mod
    
    # use Rendezvous hash and "the Power of Two Choices". 
    # Each pg corresponds to a node
    def write_obj_with_Rendezvous_power_2(self, power, inode_num, inode_size):
        # power = 3.95
        self.cal_pg_weight(power)
        self.init_pg_Rendezvous_node()
        hash_funcs = [self.hash1, self.hash2, self.hash3, self.hash4, self.hash5]
        print("hash func num:", len(hash_funcs))
        print("weight power:", power)
        collision_num = 0
        count = 0
        for inode_no in range(inode_num):
            # print(inode_num-inode_no)
            for block_no in range(inode_size):
                count = count + 1
                if count % 10000 == 0:
                    sys.stdout.write("\r%d" % count)  
                    sys.stdout.flush()  

                # key format: "10000000001.00000002"
                key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)

                # use 2 different hash function to choose 2 pgs.
                pg_ids = set()
                for hash_func in hash_funcs:
                    elem = hash_func(key)
                    while elem in pg_ids:
                        # print("hash collision")
                        collision_num += 1
                        elem = (elem + 1) % self.pg_num
                    pg_ids.add(elem)
                
                pg_nodes = []
                for pg_id in pg_ids:
                    pg_nodes.append(self.pg_Rendezvous_node[pg_id])
                
                # choose the biggest
                max_pg_node = max(pg_nodes, key=lambda x: x.compute_weighted_score(key))
                pg_id = max_pg_node.id

                # append to obj_in_pg
                objs = self.obj_in_pg.get(pg_id, [])
                objs.append(key)
                self.obj_in_pg[pg_id] = objs

                # start crush
                osds = self.pg_to_osds.get(pg_id, [])
                assert len(osds) == self.replication_count
                for i in range(len(osds)):
                    used_capacity = self.osd_used_capacity.get(osds[i], 0)
                    if self.crush_type == 'ec':
                        used_capacity += (self.obj_size / self.k)
                    elif self.crush_type == 'rep':
                        used_capacity += self.obj_size
                    # if used_capacity >= self.disk_size*1024:
                        # print("overflow! osd id:", osds[i])
                    self.osd_used_capacity[osds[i]] = used_capacity
        print("collision num:", collision_num)

    # after failure
    # use Rendezvous hash and "the Power of Two Choices". 
    # Each pg corresponds to a node
    def new_write_obj_with_Rendezvous_power_2(self, power, inode_num, inode_size):
        diff_count = 0
        # power = 3.95
        self.new_cal_pg_weight(power)
        self.new_init_pg_Rendezvous_node()
        hash_funcs = [self.hash1, self.hash2, self.hash3, self.hash4, self.hash5]
        print("hash func num:", len(hash_funcs))
        print("weight power:", power)
        collision_num = 0
        count = 0
        for inode_no in range(inode_num):
            # print(inode_num-inode_no)
            for block_no in range(inode_size):
                count = count + 1
                if count % 10000 == 0:
                    sys.stdout.write("\r%d" % count)  
                    sys.stdout.flush()  

                # key format: "10000000001.00000002"
                key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)

                # use several different hash function to choose 2 pgs.
                pg_ids = set()
                for hash_func in hash_funcs:
                    elem = hash_func(key)
                    while elem in pg_ids:
                        # print("hash collision")
                        collision_num += 1
                        elem = (elem + 1) % self.pg_num
                    pg_ids.add(elem)
                
                pg_nodes = []
                for pg_id in pg_ids:
                    pg_nodes.append(self.new_pg_Rendezvous_node[pg_id])
                
                # choose the biggest
                max_pg_node = max(pg_nodes, key=lambda x: x.compute_weighted_score(key))
                pg_id = max_pg_node.id

                if key not in self.obj_in_pg[pg_id]:
                    diff_count += 1

                # append to obj_in_pg
                objs = self.new_obj_in_pg.get(pg_id, [])
                objs.append(key)
                self.new_obj_in_pg[pg_id] = objs

                # start crush
                osds = self.new_pg_to_osds.get(pg_id, [])
                assert len(osds) == self.replication_count
                for i in range(len(osds)):
                    used_capacity = self.new_osd_used_capacity.get(osds[i], 0)
                    if self.crush_type == 'ec':
                        used_capacity += (self.obj_size / self.k)
                    elif self.crush_type == 'rep':
                        used_capacity += self.obj_size
                    # if used_capacity >= self.disk_size*1024:
                        # print("overflow! osd id:", osds[i])
                    self.new_osd_used_capacity[osds[i]] = used_capacity
        print("collision num:", collision_num)
        print("obj in pg diff num:", diff_count)

    # devide pg into groups
    def write_obj_with_Rendezvous_group(self, inode_num, inode_size):
        self.init_pg_group_Rendezvous_node()
        count = 0
        for inode_no in range(inode_num):
            # print(inode_num-inode_no)
            for block_no in range(inode_size):
                count = count + 1
                if count % 10000 == 0:
                    print(count)

                # key format: "10000000001.00000002"
                key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
                group_node = determine_responsible_node(self.group_Rendezvous_node.values(), key)
                
                group = self.grouped_data[group_node.id]
                pg_id = group[hash(key) % len(group)][0]

                # append to obj_in_pg
                objs = self.obj_in_pg.get(pg_id, [])
                objs.append(key)
                self.obj_in_pg[pg_id] = objs

                # start crush
                osds = self.pg_to_osds.get(pg_id, [])
                assert len(osds) == self.replication_count
                for i in range(len(osds)):
                    used_capacity = self.osd_used_capacity.get(osds[i], 0)
                    used_capacity += self.obj_size
                    # if used_capacity >= self.disk_size*1024:
                        # print("overflow! osd id:", osds[i])
                    self.osd_used_capacity[osds[i]] = used_capacity

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

    def new_print_osd_used_capacity(self, max_osd_id, min_osd_id):
        max_used_capacity = 0
        max_osdid = 0
        min_used_capacity = sys.maxsize
        min_osdid = 0
        obj_sum = 0
        for osd, used in self.new_osd_used_capacity.items():
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
        logger.log("MAX/MIN pg osd's used deviation:" + str(float(self.new_osd_used_capacity[max_osd_id]) / avg_capacity)+ "/" + str(float(self.osd_used_capacity[min_osd_id]) / avg_capacity))
        # print("max_pg_osd's used deviation:", float(self.osd_used_capacity[max_osd_id]) / avg_capacity)
        # print("min_pg_osd's used deviation:", float(self.osd_used_capacity[min_osd_id]) / avg_capacity)

    
    def print_obj_in_pg_number(self):
        max_obj = 0
        min_obj = sys.maxsize
        obj_sum = 0
        for pgid, objs in self.obj_in_pg.items():
            obj_count = len(objs)
            obj_to_weight = obj_count / self.pg_weight[pgid]
            max_obj = max(max_obj, obj_to_weight)
            min_obj = min(min_obj, obj_to_weight)
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


# test failure

logger = Logger("raw_crush_log.txt")
b = Balancer()

print("pg num:", b.pg_num)
print("pg num mask:", b.pg_num_mask)
print("crush type:", b.crush_type)
print("replicated count:", b.replication_count)
# disk_load = 12288  # GB
# inode_size = 4096
inode_size = 4096
# inode_num = int(b.disk_num * disk_load / inode_size / b.replication_count * 256)
disk_capacity = b.disk_num*b.disk_size #GB
overall_utilization = b.overall_used_capacity

target_capacity = disk_capacity*overall_utilization*1024
if b.crush_type == 'rep':
    obj_load_num = int(target_capacity/b.replication_count/b.obj_size)
elif b.crush_type == 'ec':
    redun = (b.k + b.m)/b.k
    obj_load_num = int(target_capacity/redun/b.obj_size)

# obj_load_num = int(disk_capacity*overall_utilization*1024/b.replication_count/b.obj_size)
# obj_load_num = 2936012
print("object load number:", obj_load_num)
inode_num = int(obj_load_num/inode_size)
print("inode number:", inode_num)
# each inode has {inode_size} blocks
# each block is a object
if b.crush_type == 'rep' and b.replication_count == 3:
    power = 3.95
elif b.crush_type == 'ec'and b.k == 8 and b.m ==2:
    power = 12.2
power = 6
# time1 = time.time()

# entire cluster without failure
b.crushmap = b.gen_crushmap(b.n_host, b.n_disk, "")
# print(self.crushmap)
b.c = Crush()
# self.crushmap_json = json.loads(self.crushmap)
b.c.parse(b.crushmap)
b.crush_pg_to_osds()
# b.cal_pg_weight(power)
b.write_obj_with_Rendezvous_power_2(power, inode_num, inode_size)
# with open('obj_in_pg.pkl', 'wb') as f:
#     pickle.dump(b.obj_in_pg, f, pickle.HIGHEST_PROTOCOL)
# b.stripe_pg_to_osds()

# b.crushmap = b.gen_crushmap(b.n_host, b.n_disk, "disk")
# print(self.crushmap)
# b.c = Crush()
# self.crushmap_json = json.loads(self.crushmap)
# b.c.parse(b.crushmap)

# b.crush_pg_to_osds()
# b.check_pg_weight(power)

# b.cal_pg_weight()
# time2 = time.time()

#one disk failure
b.crushmap = b.gen_crushmap(b.n_host, b.n_disk, "disk")
# print(self.crushmap)
b.c = Crush()
# self.crushmap_json = json.loads(self.crushmap)
b.c.parse(b.crushmap)
b.check_crush_pg_to_osds()
b.check_pg_in_osd()
b.new_cal_pg_weight(power)
# b.crush_pg_to_osds()
b.new_write_obj_with_Rendezvous_power_2(power, inode_num, inode_size)
# b.stripe_write_obj(inode_num, inode_size)
time3 = time.time()
# print(time2-time1, time3-time2)
# max_osd_id, min_osd_id = b.print_pg_in_osd()
# b.print_obj_in_pg_number()
b.new_print_osd_used_capacity(0, 0)