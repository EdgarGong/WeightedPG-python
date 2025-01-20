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
        # print("pg id:", self.id, "score:", score, "log score:", log_score)
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

        
        self.crushmap = self.gen_crushmap(self.n_host, self.n_disk)
        # print(self.crushmap)
        self.c = Crush()
        # self.crushmap_json = json.loads(self.crushmap)
        self.c.parse(self.crushmap)
        
        # self.pg_pri_in_osd = {}
        # self.pg_rep_in_osd = {}
        self.pg_in_osd = {}
        self.pg_to_osds = {}
        self.osd_used_capacity = {}
        self.obj_in_pg = {}
        self.obj_num_in_pg = {}

        
        self.max_optimizations = self.pg_num
        self.max_deviation = 1
        self.aggressive = True
        self.fast_aggressive = True
        self.local_fallback_retries = 100
        self.osdmap = OSDMap()

        self.pg_weight = {}

        self.pg_Rendezvous_node = {}

        self.grouped_data = {}
        self.group_Rendezvous_node = {}

    def compute_pgp_num_mask(self):
        # Calculate the pgp_num_mask for given pgp_num
        return (1 << (self.pg_num - 1).bit_length()) - 1
    
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
        # if the failure domain is osd, change the "host" to 0
        default_rule_rep = [["take", "default"], ["chooseleaf", "firstn", 0, "type", 0], ["emit"]]
        # default_rule_ec = [["take", "default"], ["choose", "indep", 5, "type", "host"], ["chooseleaf", "indep", 2, "type", 0], ["emit"]]
        default_rule_ec = [["take", "default"], ["chooseleaf", "indep", 0, "type", 0], ["emit"]]

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
        for pg_no in range(self.pg_num):
            pps = np.int32(
                utils.crush_hash32_rjenkins1_2(utils.ceph_stable_mod(pg_no, self.pg_num, self.pg_num_mask), 3))  # 3 is pool no., not rep count
            osds = self.c.map(rule="default_rule", value=pps, replication_count=self.replication_count)
            self.pg_to_osds[pg_no] = osds
            for i in range(len(osds)):
                if osds[i] >= self.disk_num or osds[i] < 0:
                    print("osd id:", osds[i])
                pgs = self.pg_in_osd.get(osds[i], [])
                pgs.append(pg_no)
                self.pg_in_osd[osds[i]] = pgs
    
    def print_pg_in_osd(self):
        # print("pg in osd:", self.pg_in_osd)
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
        pg_to_avg = {}
        for pg, osds in self.pg_to_osds.items():
            total_pg = 0
            for osd in osds:
                total_pg += len(self.pg_in_osd[osd])
            pg_to_avg[pg] = total_pg / len(osds)
        # # Sort by average in ascending order
        # sorted_pg = sorted(pg_to_avg.items(), key=lambda x: x[1])
        # # print(sorted_pg)
        # Calculate the sum of all values
        # total_value = sum([v for _, v in pg_to_avg.items()])
        # print(total_value)
        # Then calculate the average value
        # average_value = total_value / len(pg_to_avg)
        # Divide each value in pg_to_avg by the average value
        k = 0.065
        min_value = min(pg_to_avg.values())
        max_value = max(pg_to_avg.values())
        print("min value:", min_value, "max value:", max_value)
        print("e^(-%.3f*v)" % (k))
        for pg, v in pg_to_avg.items():
            # print("pg:", pg, "v:", v)
            # self.pg_weight[pg] = math.exp((-1) * k * v)
            self.pg_weight[pg] = (1/v) ** power
        # # print(normalized_pg)
            

    def init_pg_Rendezvous_node(self):
        assert len(self.pg_weight) > 0
        for pgid, v in self.pg_weight.items():
            self.pg_Rendezvous_node[pgid] = rendezvousNode(pgid, v)

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
    def write_obj_with_Rendezvous_power_2(self, inode_num, inode_size):
        power = 6.8
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

                # use several different hash function to choose some pgs.
                pg_ids = set()
                for hash_func in hash_funcs:
                    elem = hash_func(key)
                    while elem in pg_ids:
                        # print("hash collision")
                        collision_num += 1
                        elem = (elem + 1) % self.pg_num
                    pg_ids.add(elem)
                
                # elem = self.hash3(key)
                # while elem in pg_ids:
                #     elem += 1
                # pg_ids.add(elem)
                # elem = self.hash4(key)
                # while elem in pg_ids:
                #     elem += 1
                # pg_ids.add(elem)
                    
                # raw_pg_id = self.c.c.ceph_str_hash_rjenkins(key, len(key))
                # pg_id1 = utils.ceph_stable_mod(raw_pg_id, self.pg_num, self.pg_num_mask)
                # pg_id1 = self.hash1(key)

                # pg_id1 = mmh3.hash(key) % self.pg_num
                # Eliminate pg1 to avoid pg1 and pg2 being the same.
                # pg_id2 = (pg_id1 + 1 + (self.hash2(key) % (self.pg_num - 1))) % self.pg_num
                # pg_id2 = (pg_id1 + 1 + mmh3.hash(key)) % (self.pg_num - 1)
                # pg_id2 = self.hash2(key)
                # if pg_id1 == pg_id2:
                #     pg_id2 = pg_id2 + 1

                # pg_id3 = self.hash3(key)
                # while pg_id1 == pg_id3 or pg_id2 == pg_id3:
                #     pg_id3 = pg_id3 + 1
                # assert pg_id1 != pg_id2



                # pgs = []
                # for i in range(self.pg_num):
                #     pgs.append(i)

                # pg_id1 = pgs[self.hash1(key) % len(pgs)]
                # pgs.remove(pg_id1)
                # pg_id2 = pgs[self.hash2(key) % len(pgs)]
                # assert pg_id1 != pg_id2
                

                # random_numbers = random.sample(range(0, self.pg_num), 3)
                # pg_id1, pg_id2, pg_id3 = random_numbers[0], random_numbers[1], random_numbers[2]

                pg_nodes = []
                for pg_id in pg_ids:
                    pg_nodes.append(self.pg_Rendezvous_node[pg_id])
                # pg_node1 = self.pg_Rendezvous_node[pg_id1]
                # pg_node2 = self.pg_Rendezvous_node[pg_id2]
                # pg_node3 = self.pg_Rendezvous_node[pg_id3]

                # if pg_node1.compute_weighted_score(key) > pg_node2.compute_weighted_score(key):
                #     pg_id = pg_id1
                # else:
                #     pg_id = pg_id2

                # choose the biggest
                max_pg_node = max(pg_nodes, key=lambda x: x.compute_weighted_score(key))
                pg_id = max_pg_node.id
                # if pg_node1.compute_weighted_score(key) > pg_node2.compute_weighted_score(key) \
                #  and pg_node1.compute_weighted_score(key) > pg_node3.compute_weighted_score(key):
                #     pg_id = pg_id1
                # elif pg_node2.compute_weighted_score(key) > pg_node3.compute_weighted_score(key):
                #     pg_id = pg_id2
                # else:
                #     pg_id = pg_id3


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

        std = -np.std(list(self.osd_used_capacity.values()))
        logger.log("standard deviation: "+ str(std))
    
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


    def calc_obj_deviation_in_osd_with_new_rep(self, pg_rep_in_osd):
        obj_sum = 0
        obj_count_in_osd = {}
        for osd in range(self.disk_num):
            obj_count = 0
            pgs = self.pg_in_osd.get(osd, [])
            for pg in pgs:
                objs = self.obj_in_pg.get(pg, [])
                obj_count += len(objs)
            # pri_pgs = self.pg_pri_in_osd.get(osd, [])
            # for pg in pri_pgs:
            #     objs = self.obj_in_pg.get(pg, [])
            #     obj_count += len(objs)
            # rep_pgs = pg_rep_in_osd.get(osd, [])
            # for pg in rep_pgs:
            #     objs = self.obj_in_pg.get(pg, [])
            #     obj_count += len(objs)

            obj_count_in_osd[osd] = obj_count
            obj_sum += obj_count

        avg_obj = float(obj_sum) / self.disk_num

        max_dev = 1.0
        min_dev = 1.0
        for _, obj_count in obj_count_in_osd.items():
            dev = float(obj_count) / avg_obj
            max_dev = max(max_dev, dev)
            min_dev = min(min_dev, dev)
        logger.log("Real Objects in OSD MIN/MAX deviation: " + str(min_dev) + "/" + str(max_dev))
        return max_dev - min_dev

    def calc_pri_obj_deviation_in_osd(self):
        obj_sum = 0
        obj_count_in_osd = {}
        for osd in self.engine_id:
            obj_count = 0
            pri_pgs = self.pg_pri_in_osd.get(osd, [])
            for pg in pri_pgs:
                objs = self.obj_in_pg.get(pg, [])
                obj_count += len(objs)

            obj_count_in_osd[osd] = obj_count
            obj_sum += obj_count

        avg_obj = float(obj_sum) / self.engine_num

        max_dev = 1.0
        min_dev = 1.0
        for _, obj_count in obj_count_in_osd.items():
            dev = float(obj_count) / avg_obj
            max_dev = max(max_dev, dev)
            min_dev = min(min_dev, dev)
        logger.log("Primary Objects in OSD MIN/MAX deviation: " + str(min_dev) + "/" + str(max_dev))

    def get_obj_count_in_osd(self):
        obj_count_in_osd = {}
        for osd, pgs in self.pg_in_osd.items():
            for pg in pgs:
                obj_count = obj_count_in_osd.get(osd, 0)
                obj_count += self.obj_count_in_pg.get(pg, 0)
                obj_count_in_osd[osd] = obj_count
        return obj_count_in_osd

    def do_upmap_by_pg(self):
        total_did = 0
        # left pg num to optimize
        left = self.max_optimizations
        osdmap = copy.deepcopy(self.osdmap)
        while True:
            available = min(left, self.pg_num)
            
            did, pending_inc, obj_dev = self.calc_pg_rep_upmaps_by_pg(available, total_did, osdmap)
            
            if did == 0:
                break

            logger.log("did:{}, old:{}, new:{}".format(did, pending_inc.old_pg_upmap_items, pending_inc.new_pg_upmap_items))
            
            # remove old upmap items
            for old_item in pending_inc.old_pg_upmap_items:
                if old_item in osdmap.pg_upmap_items:
                    osdmap.pg_upmap_items.pop(old_item)

            # add new upmap items
            for pg, um_pairs in pending_inc.new_pg_upmap_items.items():
                osdmap.pg_upmap_items[pg] = um_pairs
            total_did += did
            left -= did
            if left <= 0:
                break
        return total_did, obj_dev, osdmap

    def do_upmap_by_obj(self, pg_total_did, pg_obj_dev):
        #logger.log("pg_total_did:{}, pg_dev:{}".format(pg_total_did, pg_obj_dev))
        #cur_max_deviation, _, _, _, _, _ = self.calc_deviation_by_object(self.pg_in_osd)
        avg_obj_in_pg = self.get_avg_obj_in_pg()
        #logger.log(avg_obj_in_pg)
        step = avg_obj_in_pg / 4
        self.max_deviation = avg_obj_in_pg

        min_osdmap = None
        min_dev = 1.0
        total_did = 0
        min_total_did = sys.maxsize

        for count in range(10):
            logger.log("max_deviation:{}".format(self.max_deviation))

            total_did = 0
            left = self.max_optimizations
            osdmap = copy.deepcopy(self.osdmap)
            did = 0
            retried = 0
            while True:
                available = min(left, self.pg_num)
                did, pending_inc, dev, cur_min_max_deviation = self.calc_pg_rep_upmaps_by_obj(available, total_did, osdmap)
                logger.log("now_dev:{}".format(dev))

                if did == 0:
                    break

                logger.log("now_total_did:{}, did:{}, old:{}, new:{}".format(total_did + did, did, pending_inc.old_pg_upmap_items, pending_inc.new_pg_upmap_items))
                for old_item in pending_inc.old_pg_upmap_items:
                    if old_item in self.osdmap.pg_upmap_items:
                        osdmap.pg_upmap_items.pop(old_item)

                for pg, um_pairs in pending_inc.new_pg_upmap_items.items():
                    osdmap.pg_upmap_items[pg] = um_pairs
                total_did += did
                left -= did
                if left <= 0:
                    break
                if dev >= min_dev * 1.2:
                    break
                if dev <= min_dev * 0.9:
                    break

            if dev < min_dev:
                min_dev = dev
                min_osdmap = copy.deepcopy(osdmap)
                min_total_did = total_did
                # logger.log("min_dev:{}, min_osdmap:{}".format(min_dev, min_osdmap))
            else:
                retried += 1
                if retried > 3:
                    break

            if self.max_deviation - step <= cur_min_max_deviation <= self.max_deviation + step:
                self.max_deviation -= step
            elif cur_min_max_deviation > self.max_deviation:
                self.max_deviation = (self.max_deviation + cur_min_max_deviation) / 2
            elif cur_min_max_deviation < self.max_deviation:
                self.max_deviation = (self.max_deviation + cur_min_max_deviation) / 2

            self.max_deviation = max(self.max_deviation, 1)

        logger.log("min_dev:{}".format(min_dev))
        return min_total_did, min_osdmap

    def get_real_rep_pg_in_osd(self, osdmap):
        pg_in_osd = copy.deepcopy(self.pg_in_osd)
        # execute the upmap items
        for pg, um_pairs in osdmap.pg_upmap_items.items():
            for ump in um_pairs:
                osd_from = ump[0]
                osd_to = ump[1]

                from_pgs = pg_in_osd.get(osd_from, [])
                if pg in from_pgs:
                    from_pgs.remove(pg)
                pg_in_osd[osd_from] = from_pgs

                to_pgs = pg_in_osd.get(osd_to, [])
                if pg not in to_pgs:
                    to_pgs.append(pg)
                pg_in_osd[osd_to] = to_pgs
        self.check_pg_list(pg_in_osd)
        return pg_in_osd

    def calc_rep_deviations_by_obj(self, pg_in_osd):
        total_objects = 0
        obj_debug_sum = 0
        for osd, pgs in pg_in_osd.items():
            for pg in pgs:
                total_objects += len(self.obj_in_pg.get(pg, []))
            obj_debug_sum += len(pgs)
        # logger.log("obj_debug_sum:{}".format(obj_debug_sum))

        cur_max_deviation = 0.0
        stddev = 0.0
        max_dev_perc = 1.0
        min_dev_perc = 1.0
        osd_deviation = {}
        deviation_osd = {}

        for osd, pgs in pg_in_osd.items():
            objects = 0
            for pg in pgs:
                objects += len(self.obj_in_pg.get(pg, []))

            target = float(total_objects) / float(len(pg_in_osd))
            deviation = objects - target
            dev_perc = float(objects) / float(target)
            if dev_perc > max_dev_perc:
                max_dev_perc = dev_perc
            if dev_perc < min_dev_perc:
                min_dev_perc = dev_perc

            osd_deviation[osd] = deviation

            osds = deviation_osd.get(deviation, [])
            osds.append(osd)
            deviation_osd[deviation] = osds

            stddev += deviation * deviation

            if abs(deviation) > cur_max_deviation:
                cur_max_deviation = abs(deviation)

        #logger.log("object LB info:\tmax_deviation:{},\tstddev:{},\tmin/max:{}:{}".format(cur_max_deviation, stddev, min_dev_perc, max_dev_perc))
        self.calc_obj_deviation_in_osd_with_new_rep(pg_in_osd)
        return cur_max_deviation, stddev, osd_deviation, deviation_osd, min_dev_perc, max_dev_perc

    @with_goto
    # max: available pg num to optimize
    def calc_pg_rep_upmaps_by_pg(self, max, last_did, osdmap):
        pending_inc = Incremental()
        obj_dev = 1.0

        max_deviation = self.max_deviation
        if max_deviation < 1:
            max_deviation = 1
        tmp_osd_map = copy.deepcopy(osdmap)
        num_changed = 0
        osd_weight_total, osd_weight = self.get_osd_weights()
        if osd_weight_total == 0:
            return 0, pending_inc, obj_dev
        pgs_per_weight = float(self.pg_num) * (self.replication_count - 1) / osd_weight_total

        pg_in_osd = self.get_real_rep_pg_in_osd(osdmap)


        cur_max_deviation, stddev, osd_deviation, deviation_osd, obj_dev = self.calc_rep_deviations(pg_in_osd, osd_weight, pgs_per_weight)

        deviation_osd = sorted(deviation_osd.items())
        if cur_max_deviation <= max_deviation:
            return 0, pending_inc, obj_dev

        skip_overfull = False
        aggressive = self.aggressive
        fast_aggressive = aggressive and self.fast_aggressive
        local_fallback_retries = self.local_fallback_retries
        retried = False

        # max: available pg num to optimize
        while max > 0:
            print("max:{}".format(max))
            max -= 1
            using_more_overfull = False
            overfull, more_overfull, underfull, more_underfull = self.fill_overfull_underfull(deviation_osd,
                                                                                              max_deviation)
            if len(underfull) == 0 and len(overfull) == 0:
                break
            if len(overfull) == 0 and len(underfull) != 0:
                overfull = copy.deepcopy(more_overfull)
                using_more_overfull = True

            to_skip = []
            local_fallback_retried = 0

            n_changes = 0
            prev_n_changes = 0
            osd_to_skip = []

            label .retry

            to_unmap = []
            to_upmap = {}
            temp_pg_in_osd = copy.deepcopy(pg_in_osd)
            
            
            #
            for item in reversed(deviation_osd):
                if skip_overfull and len(underfull) != 0:
                    break
                deviation = item[0]
                osds = item[1]
                for osd in osds:
                    if fast_aggressive and (osd in osd_to_skip):
                        continue
                    if deviation < 0:
                        break
                    target = osd_weight.get(osd, 0) * pgs_per_weight
                    if (not using_more_overfull) and (deviation <= max_deviation):
                        break

                    pgs = []
                    for pg in pg_in_osd.get(osd, []):
                        if pg in to_skip:
                            continue
                        pgs.append(pg)
                    if aggressive:
                        random.shuffle(pgs)
                    ret = self.try_drop_remap_overfull(pgs, tmp_osd_map, osd, temp_pg_in_osd, to_unmap, to_upmap)

                    if ret:
                        goto .test_change

                    # try upmap
                    for pg in pgs:
                        temp_it = tmp_osd_map.pg_upmap.get(pg, [])
                        if temp_it:
                            continue
                        new_upmap_items = []
                        existing = []
                        it = osdmap.pg_upmap_items.get(pg, [])
                        if it:
                            if len(it) >= self.replication_count:
                                continue
                            else:
                                new_upmap_items = it
                                for p in it:
                                    existing.append(p[0])
                                    existing.append(p[1])
                        

                        # fall through
                        # to see if we can append more remapping pairs
                        raw = []
                        orig = []
                        out = []
                        raw, orig = self.pg_to_raw_upmap(tmp_osd_map, osd_weight, pg)
                        if not self.c.c.try_pg_upmap(pg, self.replication_count, "default_rule", overfull, underfull, more_underfull, orig, out):
                            continue
                        if len(orig) != len(out):
                            continue
                        
                        
                        pos = self.find_best_remap(orig, out, existing, osd_deviation)
                        if pos != -1:
                            self.add_remap_pair(orig[pos], out[pos], pg, osd, existing, temp_pg_in_osd, new_upmap_items, to_upmap)

                            goto .test_change
                    if fast_aggressive:
                        if prev_n_changes == n_changes:
                            osd_to_skip.append(osd)
                        else:
                            prev_n_changes = n_changes
           
            for item in deviation_osd:
                deviation = item[0]
                osd = item[1]

                if osd not in underfull:
                    break
                target = osd_weight[osd] * pgs_per_weight
                if abs(deviation) < max_deviation:
                    break
                candidates = self.build_candidates(tmp_osd_map, to_skip, aggressive)
                ret = self.try_drop_remap_underfull(candidates, osd, temp_pg_in_osd, to_unmap, to_upmap)

                if ret:
                    goto .test_change

            if not aggressive:
                break
            elif not skip_overfull:
                break
            skip_overfull = False
            continue

            label .test_change
            cur_max_deviation, new_stddev, temp_osd_deviation, temp_deviation_osd, obj_dev = self.calc_rep_deviations(temp_pg_in_osd, osd_weight, pgs_per_weight)

            if new_stddev >= stddev:
                if not aggressive:
                    break
                local_fallback_retried += 1
                if local_fallback_retried >= local_fallback_retries:
                    if retried:
                        break
                    else:
                        retried = True
                    skip_overfull = not skip_overfull
                    continue
                for i in to_unmap:
                    to_skip.append(i)
                for i in to_upmap:
                    to_skip.append(i)
                goto .retry
            stddev = new_stddev
            pg_in_osd = copy.deepcopy(temp_pg_in_osd)
            osd_deviation = copy.deepcopy(temp_osd_deviation)
            deviation_osd = copy.deepcopy(temp_deviation_osd)
            deviation_osd = sorted(deviation_osd.items())
            n_changes += 1

            num_changed += self.pack_upmap_results(to_unmap, to_upmap, tmp_osd_map, pending_inc)
            # logger.log("now total did:{}".format(last_did + num_changed))

            if cur_max_deviation <= max_deviation:
                break

        return num_changed, pending_inc, obj_dev

    def get_total_objects(self, pg_in_osd):
        total_objects = 0
        for osd, pgs in pg_in_osd.items():
            for pg in pgs:
                total_objects += len(self.obj_in_pg.get(pg, []))
        return total_objects

    @with_goto
    def calc_pg_rep_upmaps_by_obj(self, max, last_did, osdmap):
        pending_inc = Incremental()

        max_deviation = self.max_deviation
        if max_deviation < 1:
            max_deviation = 1
        tmp_osd_map = copy.deepcopy(osdmap)
        num_changed = 0
        osd_weight_total, osd_weight = self.get_osd_weights()
        if osd_weight_total == 0:
            return 0, pending_inc, 1.0, None

        total_objects = self.get_total_objects(self.pg_in_osd)

        objs_per_weight = total_objects / osd_weight_total

        pg_in_osd = self.get_real_rep_pg_in_osd(osdmap)

        cur_max_deviation, stddev, osd_deviation, deviation_osd, min_dev_perc, max_dev_perc = self.calc_rep_deviations_by_obj(pg_in_osd)
        cur_min_max_deviation = cur_max_deviation
        min_dev = max_dev_perc-min_dev_perc

        deviation_osd = sorted(deviation_osd.items())
        if cur_max_deviation <= max_deviation:
            return 0, pending_inc, min_dev, cur_min_max_deviation

        skip_overfull = False
        aggressive = self.aggressive
        fast_aggressive = aggressive and self.fast_aggressive
        local_fallback_retries = self.local_fallback_retries
        retried = False

        while max > 0:
            max -= 1
            using_more_overfull = False
            overfull, more_overfull, underfull, more_underfull = self.fill_overfull_underfull(deviation_osd,
                                                                                              max_deviation)
            if len(underfull) == 0 and len(overfull) == 0:
                break
            if len(overfull) == 0 and len(underfull) != 0:
                overfull = copy.deepcopy(more_overfull)
                using_more_overfull = True

            to_skip = []
            local_fallback_retried = 0

            n_changes = 0
            prev_n_changes = 0
            osd_to_skip = []

            label .retry

            to_unmap = []
            to_upmap = {}
            temp_pg_in_osd = copy.deepcopy(pg_in_osd)
            for item in reversed(deviation_osd):
                if skip_overfull and len(underfull) != 0:
                    break
                deviation = item[0]
                osds = item[1]
                for osd in osds:
                    if fast_aggressive and (osd in osd_to_skip):
                        continue
                    if deviation < 0:
                        break
                    target = osd_weight.get(osd, 0) * objs_per_weight
                    if (not using_more_overfull) and (deviation <= max_deviation):
                        break

                    pgs = []
                    for pg in pg_in_osd.get(osd, []):
                        if pg in to_skip:
                            continue
                        pgs.append(pg)
                    if aggressive:
                        random.shuffle(pgs)
                    if self.try_drop_remap_overfull(pgs, tmp_osd_map, osd, temp_pg_in_osd, to_unmap, to_upmap):
                        goto .test_change

                    # try upmap
                    for pg in pgs:
                        temp_it = tmp_osd_map.pg_upmap.get(pg, [])
                        if temp_it:
                            continue
                        new_upmap_items = []
                        existing = []
                        it = osdmap.pg_upmap_items.get(pg, [])
                        if it:
                            if len(it) >= self.replication_count:
                                continue
                            else:
                                new_upmap_items = it
                                for p in it:
                                    existing.append(p[0])
                                    existing.append(p[1])

                        # fall through
                        # to see if we can append more remapping pairs
                        raw = []
                        orig = []
                        out = []
                        raw, orig = self.pg_to_raw_upmap(tmp_osd_map, osd_weight, pg)
                        if not self.c.c.try_pg_upmap(pg, self.replication_count, "default_rule", overfull, underfull, more_underfull, orig, out):
                            continue
                        if len(orig) != len(out):
                            continue
                        pos = self.find_best_remap(orig, out, existing, osd_deviation)
                        if pos != -1:
                            self.add_remap_pair(orig[pos], out[pos], pg, osd, existing, temp_pg_in_osd, new_upmap_items, to_upmap)
                            goto .test_change
                    if fast_aggressive:
                        if prev_n_changes == n_changes:
                            osd_to_skip.append(osd)
                        else:
                            prev_n_changes = n_changes

            for item in deviation_osd:
                deviation = item[0]
                osd = item[1]

                if osd not in underfull:
                    break
                target = osd_weight[osd] * objs_per_weight
                if abs(deviation) < max_deviation:
                    break
                candidates = self.build_candidates(tmp_osd_map, to_skip, aggressive)
                if self.try_drop_remap_underfull(candidates, osd, temp_pg_in_osd, to_unmap, to_upmap):
                    goto .test_change

            if not aggressive:
                break
            elif not skip_overfull:
                break
            skip_overfull = False
            continue

            label .test_change
            cur_max_deviation, new_stddev, temp_osd_deviation, temp_deviation_osd, min_dev_perc, max_dev_perc = self.calc_rep_deviations_by_obj(temp_pg_in_osd)
            cur_min_max_deviation = min(cur_min_max_deviation, cur_max_deviation)
            min_dev = min(min_dev, max_dev_perc - min_dev_perc)

            if new_stddev >= stddev:
                if not aggressive:
                    break
                local_fallback_retried += 1
                if local_fallback_retried >= local_fallback_retries:
                    if retried:
                        break
                    else:
                        retried = True
                    skip_overfull = not skip_overfull
                    continue
                for i in to_unmap:
                    to_skip.append(i)
                for i in to_upmap:
                    to_skip.append(i)
                goto .retry
            stddev = new_stddev
            pg_in_osd = copy.deepcopy(temp_pg_in_osd)
            osd_deviation = copy.deepcopy(temp_osd_deviation)
            deviation_osd = copy.deepcopy(temp_deviation_osd)
            deviation_osd = sorted(deviation_osd.items())
            n_changes += 1

            num_changed += self.pack_upmap_results(to_unmap, to_upmap, tmp_osd_map, pending_inc)
            # logger.log("now total did:{}".format(last_did + num_changed))

            if cur_max_deviation <= max_deviation:
                break

        return num_changed, pending_inc, min_dev, cur_min_max_deviation

    def pack_upmap_results(self, to_unmap, to_upmap, tmp_osd_map, pending_inc):
        num_changed = 0
        for i in to_unmap:
            tmp_osd_map.pg_upmap_items.pop(i)
            pending_inc.old_pg_upmap_items.append(i)
            num_changed += 1
        for pg, um_items in to_upmap.items():
            tmp_osd_map.pg_upmap_items[pg] = um_items
            pending_inc.new_pg_upmap_items[pg] = um_items
            num_changed += 1
        return num_changed


    def try_drop_remap_underfull(self, candidates, osd, temp_pgs_in_osd, to_unmap, to_upmap):
        for item in candidates:
            pg = item[0]
            um_pairs = item[1]

            new_upmap_items = []
            for ump in um_pairs:
                um_from = ump[0]
                um_to = ump[1]
                if um_from == osd:
                    pgs = temp_pgs_in_osd.get(um_to, [])
                    pgs.remove(pg)
                    temp_pgs_in_osd[um_to] = pgs

                    pgs = temp_pgs_in_osd.get(um_from, [])
                    pgs.append(pg)
                    temp_pgs_in_osd[um_from] = pgs
                else:
                    new_upmap_items.append(ump)
            if len(new_upmap_items) == 0:
                to_unmap.append(pg)
                return True
            elif len(new_upmap_items) != len(um_pairs):
                to_upmap[pg] = new_upmap_items
                return True
        self.check_pg_list(temp_pgs_in_osd)
        return False

    def build_candidates(self, tmp_osd_map, to_skip, aggressive):
        candidates = []
        for pg, um_pair in tmp_osd_map.pg_upmap_items.items:
            if pg in to_skip:
                continue
            candidates.append([pg, um_pair])
        if aggressive:
            random.shuffle(candidates)
        return candidates


    def add_remap_pair(self, orig, out, pg, osd, existing, temp_pg_in_osd, new_upmap_items, to_upmap):
        orig_pgs = temp_pg_in_osd.get(orig, [])
        if pg not in orig_pgs:
            return

        orig_pgs.remove(pg)
        temp_pg_in_osd[orig] = orig_pgs
        out_pgs = temp_pg_in_osd.get(out, [])
        out_pgs.append(pg)
        temp_pg_in_osd[out] = out_pgs

        existing.append(orig)
        existing.append(out)

        item = [orig, out]
        new_upmap_items.append(item)
        to_upmap[pg] = new_upmap_items

        self.check_pg_list(temp_pg_in_osd)
        # print("")

    def find_best_remap(self, orig, out, existing, osd_deviation):
        best_pos = -1
        max_dev = 0.0
        for i in range(len(out)):
            if orig[i] == out[i]:
                continue
            if orig[i] in existing or out[i] in existing:
                continue
            dev = osd_deviation.get(orig[i], 0.0)
            if dev > max_dev:
                max_dev = dev
                best_pos = i
        return best_pos

    def pg_to_raw_upmap(self, osdmap, osd_weight, pg):
        pps = np.int32(
            utils.crush_hash32_rjenkins1_2(utils.ceph_stable_mod(pg, self.pg_num, self.pg_num_mask), 3))  # 3 is pool no.
        raw = self.c.map(rule="default_rule", value=pps, replication_count=self.replication_count)
        raw_upmap = copy.deepcopy(raw)
        self._apply_upmap(osdmap, osd_weight, pg, raw_upmap)
        return raw, raw_upmap

    def _apply_upmap(self, osdmap, osd_weight, pg, raw):
        p = osdmap.pg_upmap.get(pg, [])
        if p:
            for osd in p:
                if osd != None and osd_weight == 0:
                    return
            raw = copy.deepcopy(p)

        q = osdmap.pg_upmap_items.get(pg, [])
        if q:
            for item in q:
                osd_from = item[0]
                osd_to = item[1]
                exists = False
                pos = -1
                for i in range(len(raw)):
                    osd = raw[i]
                    if osd == osd_to:
                        exists = True
                        break
                    if osd == osd_from and pos < 0 and not (osd_to != None and osd_weight[osd_to] == 0):
                        pos = i
                if not exists and pos >= 0:
                    raw[pos] = osd_to


    def try_drop_remap_overfull(self, pgs, tmp_osd_map, osd, temp_pgs_in_osd, to_unmap, to_upmap):
        for pg in pgs:
            pg_upmap_items = tmp_osd_map.pg_upmap_items.get(pg, [])
            if len(pg_upmap_items) == 0:
                continue
            new_upmap_items = []
            for um_pair in pg_upmap_items:
                um_from = um_pair[0]
                um_to = um_pair[1]
                if um_to == osd:
                    pgs = temp_pgs_in_osd.get(um_to, [])
                    pgs.remove(pg)
                    temp_pgs_in_osd[um_to] = pgs

                    pgs = temp_pgs_in_osd.get(um_from, [])
                    pgs.append(pg)
                    temp_pgs_in_osd[um_from] = pgs
                else:
                    new_upmap_items.append(um_pair)
            if len(new_upmap_items) == 0:
                to_unmap.append(pg)
                return True
            elif len(new_upmap_items) != len(pg_upmap_items):
                to_upmap[pg] = new_upmap_items
                return True

        self.check_pg_list(temp_pgs_in_osd)
        return False

    def fill_overfull_underfull(self, deviation_osd, max_deviation):
        overfull = []
        more_overfull = []
        underfull = []
        more_underfull = []

        for item in reversed(deviation_osd):
            deviation = item[0]
            osds = item[1]
            if deviation <= 0:
                break
            if deviation > max_deviation:
                for osd in osds:
                    overfull.append(osd)
            else:
                for osd in osds:
                    more_overfull.append(osd)

        for item in deviation_osd:
            deviation = item[0]
            osds = item[1]
            if deviation >= 0:
                break
            if deviation < -max_deviation:
                for osd in osds:
                    underfull.append(osd)
            else:
                for osd in osds:
                    more_underfull.append(osd)

        return overfull, more_overfull, underfull, more_underfull

    def check_pg_list(self, pg_in_osd):
        pass
        # debug_sum = 0
        # for osd, pgs in pg_in_osd.items():
        #     debug_sum += len(pgs)
        # print(debug_sum)

    def calc_rep_deviations(self, pg_in_osd, osd_weight, pgs_per_weight):
        cur_max_deviation = 0.0
        stddev = 0.0
        osd_deviation = {}
        deviation_osd = {}
        max_dev_perc = 1.0
        min_dev_perc = 1.0
        debug_sum = 0
        for osd, pgs in pg_in_osd.items():
            # print("{}: len {}".format(osd, len(pgs)))
            debug_sum += len(pgs)

            target = osd_weight.get(osd, 0) * pgs_per_weight
            deviation = len(pgs) - target

            dev_perc = float(len(pgs)) / float(target)
            if dev_perc > max_dev_perc:
                max_dev_perc = dev_perc
            if dev_perc < min_dev_perc:
                min_dev_perc = dev_perc

            osd_deviation[osd] = deviation

            osds = deviation_osd.get(deviation, [])
            osds.append(osd)
            deviation_osd[deviation] = osds

            stddev += deviation * deviation

            if abs(deviation) > cur_max_deviation:
                cur_max_deviation = abs(deviation)

        # logger.log("debug_sum:{}".format(debug_sum))
        #logger.log("pg LB info:\t\tmax_deviation:{},\tstddev:{},\tmin/max:{}:{}".format(cur_max_deviation, stddev, min_dev_perc, max_dev_perc))
        #_, _, _, _, min_obj_dev_perc, max_obj_dev_perc = self.calc_real_deviation_by_object(pg_in_osd)
        #return cur_max_deviation, stddev, osd_deviation, deviation_osd, max_obj_dev_perc-min_obj_dev_perc
        return cur_max_deviation, stddev, osd_deviation, deviation_osd, self.calc_obj_deviation_in_osd_with_new_rep(pg_in_osd)

    def get_osd_weights(self):
        osd_weight_total = 0.0
        osd_weight = {}

        stack = []
        for child in self.c.crushmap.get('trees', [])[0].get('children', []):
            stack.append(child)

        while len(stack) > 0:
            bucket = stack.pop()
            if bucket.get('type', "") == "host":
                for child in bucket.get('children', []):
                    name = child.get("id", "")
                    weight = child.get("weight", 0)
                    if name != "":
                        ow = osd_weight.get(name, 0)
                        ow += weight
                        osd_weight[name] = ow

                        osd_weight_total += weight
            else:
                for child in bucket.get('children', []):
                    stack.append(child)

        return osd_weight_total, osd_weight

    def get_avg_obj_in_pg(self):
        avg = 0
        for v in self.obj_in_pg.values():
            avg += len(v)
        return float(avg) / len(self.obj_in_pg)
      
      
logger = Logger("weight-hash-upmap.txt")
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

# obj_load_num = i  nt(disk_capacity*overall_utilization*1024/b.replication_count/b.obj_size)
# obj_load_num = 2936012
print("object load number:", obj_load_num)
inode_num = int(obj_load_num/inode_size)
print("inode number:", inode_num)
# each inode has {inode_size} blocks
# each block is a object
time1 = time.time()
b.crush_pg_to_osds()
# b.stripe_pg_to_osds()

# 1: conv_ceph 
# 2:Rendezvous_power_2
hash_type = 1
# b.cal_pg_weight()
time2 = time.time()

if hash_type == 1:
  b.write_obj(inode_num, inode_size)
elif hash_type == 2:
  b.write_obj_with_Rendezvous_power_2(inode_num, inode_size)
else:
  print("hash type error")
# b.stripe_write_obj(inode_num, inode_size)
time3 = time.time()
# print(time2-time1, time3-time2)
max_osd_id, min_osd_id = b.print_pg_in_osd()
if hash_type == 0:
  b.print_obj_in_pg_number()
b.print_osd_used_capacity(max_osd_id, min_osd_id)

pg_did, obj_dev, _ = b.do_upmap_by_pg()
logger.log("pg upmap total_did:{}, obj_dev:{}".format(pg_did, obj_dev))

b.print_osd_used_capacity(max_osd_id, min_osd_id)

