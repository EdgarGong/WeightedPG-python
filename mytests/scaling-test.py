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
    def __init__(self, host_num, disk_num, pg_num, overall_used_capacity, power, type, k, m):
#         self.crushmap = """
# {"rules": {"default_rule": [["take", "default"], ["chooseleaf", "firstn", 0, "type", "host"], ["emit"]]}, "types": [{"name": "root", "type_id": 2}, {"name": "host", "type_id": 1}], "trees": [{"children": [{"children": [{"id": 0, "weight": 1, "name": "device0"}, {"id": 1, "weight": 1, "name": "device1"}, {"id": 2, "weight": 1, "name": "device2"}], "type": "host", "name": "host0", "id": -2}, {"children": [{"id": 3, "weight": 1, "name": "device3"}, {"id": 4, "weight": 1, "name": "device4"}, {"id": 5, "weight": 1, "name": "device5"}], "type": "host", "name": "host1", "id": -3}, {"children": [{"id": 6, "weight": 1, "name": "device6"}, {"id": 7, "weight": 1, "name": "device7"}, {"id": 8, "weight": 1, "name": "device8"}], "type": "host", "name": "host2", "id": -4}, {"children": [{"id": 9, "weight": 1, "name": "device9"}, {"id": 10, "weight": 1, "name": "device10"}, {"id": 11, "weight": 1, "name": "device11"}], "type": "host", "name": "host3", "id": -5}], "type": "root", "name": "default", "id": -1}]}
# """
        with open("config/config.json") as f:
            config = json.load(f)
        # self.n_host = config['host_num']
        # self.n_disk = config['disk_num']
        self.n_host = host_num
        self.n_disk = disk_num

        # self.crush_type = config['type']
        self.crush_type = type

        if self.crush_type == 'ec':
            # self.k = config['k']
            # self.m = config['m']
            self.k = k
            self.m = m
            self.replication_count = self.k + self.m
        elif self.crush_type == 'rep':
            # self.replication_count = config['replicated_num']
            self.replication_count = 3

        # self.obj_size = config['obj_size'] #MB
        # self.disk_size = config['disk_size'] #GB
        self.obj_size = 4 #MB
        self.disk_size = 256 #GB
        # self.power = config['power']
        self.power = power

        # self.overall_used_capacity = config['overall_used_capacity']
        self.overall_used_capacity = overall_used_capacity
        self.disk_num = self.n_host*self.n_disk
        
        self.osd_weight = {}
        for osd in range(self.disk_num):
            self.osd_weight[osd] = 1
        
        self.osd_to_capacity = {}

        # self.pg_num = int(100 * self.disk_num * self.overall_used_capacity / self.replication_count)  # Suggest PG Count = (Target PGs per OSD) x (OSD#) x (%Data) / (Size)
        # self.pg_num = 1 << (self.pg_num.bit_length() - 1)
        # self.pg_num = config['pg_num']
        self.pg_num = pg_num


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


    def compute_pgp_num_mask(self):
        # Calculate the pgp_num_mask for given pgp_num
        return (1 << (self.pg_num - 1).bit_length()) - 1
    
    def gen_crushmap(self, n_host, n_disk, add_a_disk):
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
            disk_num = n_disk
            if host == n_host - 1 and add_a_disk == True:
                disk_num += 1
            for i in range(disk_num):
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
        # take osd as failure domain
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
      
    def crushmap_add_host(self, crushmap, add_host, disk_per_host):
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
        max_osd_id = 0
        for pg_no in range(self.pg_num):
            pps = np.int32(
                utils.crush_hash32_rjenkins1_2(utils.ceph_stable_mod(pg_no, self.pg_num, self.pg_num_mask), 3))  # 3 is pool no., not rep count
            osds = self.c.map(rule="default_rule", value=pps, replication_count=self.replication_count)
            self.pg_to_osds[pg_no] = osds
            for i in range(len(osds)):
                # max_osd_id = max(max_osd_id, osds[i])
                pgs = self.pg_in_osd.get(osds[i], [])
                pgs.append(pg_no)
                self.pg_in_osd[osds[i]] = pgs
        # print("max osd id:", max_osd_id)
      

    def print_pg_in_osd(self):
        max_pg_num = 0
        max_osdid = 0
        min_pg_num = sys.maxsize
        min_osdid = 0
        # avg_pg_num = float(self.pg_num * self.replication_count) / self.disk_num
        for osd, pgs in self.pg_in_osd.items():
            normalized_pg_num = float(len(pgs)) / self.osd_weight[osd]
            if normalized_pg_num >= max_pg_num:
                max_pg_num = normalized_pg_num
                max_osdid = osd
            if normalized_pg_num <= min_pg_num:
                min_pg_num = normalized_pg_num
                min_osdid = osd
            # logger.log("(osd, pg num):" + str(osd) + "," + str(len(pgs)))
        logger.log("normalized pg num in osd , MAX/MIN:" + str(max_pg_num) + "/" + str(min_pg_num))
        # logger.log("pg num in osd deviation, MAX/MIN:" + str(max_pg_num/avg_pg_num) + "/" + str(min_pg_num/avg_pg_num))
        logger.log("pg num in osd MAX/MIN osd id:" + str(max_osdid) + "/" + str(min_osdid))
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

    
    
    # calculate pg weight
    def cal_pg_weight(self, power):
        # Calculate the average pg num of the osd corresponding to each pg
        # pg_to_avg = {}
        # print(self.osd_weight)
        self.pg_to_avg.clear()
        for pg, osds in self.pg_to_osds.items():
            total_pg = 0
            for osd in osds:
                # devide by the normalized capacity
                # if self.osd_weight.get(osd, 0) == 0:
                #     print("osd weight is zero, osd id:", osd)
                normalized_pg_num = (len(self.pg_in_osd[osd]) / (self.osd_weight[osd] ))
                # normalized_pg_num = len(self.pg_in_osd[osd])
                # print("pg:", pg, "osd:", osd, "normalized_pg_num:", normalized_pg_num)
                total_pg += normalized_pg_num
            self.pg_to_avg[pg] = total_pg / len(osds)

        # with open('pg_to_avg_non_normal.json', 'w') as f:
        #   json.dump(self.pg_to_avg, f)
        min_value = min(self.pg_to_avg.values())
        max_value = max(self.pg_to_avg.values())
        print("min value:", min_value, "max value:", max_value)
        
        for pg, v in self.pg_to_avg.items():
            self.pg_weight[pg] = (1/v) ** power
        # # print(normalized_pg)
            

    def init_pg_Rendezvous_node(self):
        assert len(self.pg_weight) > 0
        for pgid, v in self.pg_weight.items():
            self.pg_Rendezvous_node[pgid] = rendezvousNode(pgid, v)
    
    # # use Rendezvous hash. Each pg corresponds to a node
    # def write_obj_with_Rendezvous(self, inode_num, inode_size):

    #     self.cal_pg_weight(3)
    #     self.init_pg_Rendezvous_node()
    #     count = 0
    #     for inode_no in range(inode_num):
    #         # print(inode_num-inode_no)
    #         for block_no in range(inode_size):
    #             count = count + 1
    #             if count % 10000 == 0:
    #                 sys.stdout.write("\r%d" % count)  
    #                 sys.stdout.flush() 

    #             # key format: "10000000001.00000002"
    #             key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
    #             pg_node = determine_responsible_node(self.pg_Rendezvous_node.values(), key)
                
    #             pg_id = pg_node.id

    #             # append to obj_in_pg
    #             objs = self.obj_in_pg.get(pg_id, [])
    #             objs.append(key)
    #             self.obj_in_pg[pg_id] = objs

    #             # start crush
    #             osds = self.pg_to_osds.get(pg_id, [])
    #             assert len(osds) == self.replication_count
    #             for i in range(len(osds)):
    #                 used_capacity = self.osd_used_capacity.get(osds[i], 0)
    #                 used_capacity += self.obj_size
    #                 # if used_capacity >= self.disk_size*1024:
    #                     # print("overflow! osd id:", osds[i])
    #                 self.osd_used_capacity[osds[i]] = used_capacity
    
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
      
    def select_random_numbers_with_seed(self, lst, k, seed):
      random.seed(seed)
      return random.sample(lst, k)
    
    def select_pg(self, key):
        collision_num = 0
        hash_funcs = [self.hash1, self.hash2, self.hash3, self.hash4, self.hash5]
        pg_ids = set()
        for hash_func in hash_funcs:
            elem = hash_func(key)
            while elem in pg_ids:
                # print("hash collision")
                collision_num += 1
                elem = (elem + 1) % self.pg_num
            pg_ids.add(elem)
        
        # pg_ids = self.select_random_numbers_with_seed(list(range(self.pg_num)), 5, key)
        pg_nodes = []
        for pg_id in pg_ids:
            pg_nodes.append(self.pg_Rendezvous_node[pg_id])
        # choose the biggest
        max_score = float('-inf')
        max_pgid = 0
        # print(self.pg_Rendezvous_node)
        
        max_pg_node = max(pg_nodes, key=lambda x: x.compute_weighted_score(key))
        return max_pg_node.id
                
    
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


    def print_osd_used_capacity(self, osd_id_1, osd_id_2):
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
        logger.log("MAX/MIN/AVG used capacity:" + str(float(max_used_capacity)/(self.disk_size * 1024))+ "/" + str(float(min_used_capacity)/(self.disk_size * 1024)) + "/" + str(avg_capacity/(self.disk_size * 1024)))
        # print("max used capacity:", float(max_used_capacity)/(self.disk_size * 1024), "min used capacity:", float(min_used_capacity)/(self.disk_size * 1024))
        logger.log("MAX/MIN used capacity deviation:" + str(max_used_capacity/avg_capacity)+ "/" + str(min_used_capacity/avg_capacity))
        # print("max used capacity deviation:", max_used_capacity/avg_capacity, "min used capacity deviation:", min_used_capacity/avg_capacity)
        logger.log("MAX/MIN used osd id:" + str(max_osdid)+ "/" + str(min_osdid))
        # print("max used osd id:", max_osdid, "min used osd id:", min_osdid)
        logger.log("osd-1/2's used deviation:" + str(float(self.osd_used_capacity[osd_id_1]) / avg_capacity)+ "/" + str(float(self.osd_used_capacity[osd_id_2]) / avg_capacity))
        # print("max_pg_osd's used deviation:", float(self.osd_used_capacity[max_osd_id]) / avg_capacity)
        # print("min_pg_osd's used deviation:", float(self.osd_used_capacity[min_osd_id]) / avg_capacity)
    
        return max_osdid, min_osdid
      
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

def raw_crush_compare(b, new_b, inode_num, inode_size):
  diff_rep_cnt = 0
  remap_to_old_osd_cnt = 0
  for inode_no in range(inode_num):
      # print(inode_num-inode_no)
      for block_no in range(inode_size):
          # key format: "10000000001.00000002"
          key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
          raw_pg_id = b.c.c.ceph_str_hash_rjenkins(key, len(key))
          pg_id = utils.ceph_stable_mod(raw_pg_id, b.pg_num, b.pg_num_mask)
          # start crush
          b_osds = b.pg_to_osds.get(pg_id, [])
          new_b_osds = new_b.pg_to_osds.get(pg_id, [])
          
          for i in range(len(new_b_osds)):
              used_capacity = b.osd_used_capacity.get(b_osds[i], 0)
              if b.crush_type == 'ec':
                  used_capacity += (b.obj_size / b.k)
              elif b.crush_type == 'rep':
                  used_capacity += b.obj_size
              # if used_capacity >= self.disk_size*1024:
                  # print("overflow! osd id:", osds[i])
              b.osd_used_capacity[b_osds[i]] = used_capacity
              
              used_capacity = new_b.osd_used_capacity.get(new_b_osds[i], 0)
              if new_b.crush_type == 'ec':
                  used_capacity += (new_b.obj_size / new_b.k)
              elif new_b.crush_type == 'rep':
                  used_capacity += new_b.obj_size
              # if used_capacity >= self.disk_size*1024:
                  # print("overflow! osd id:", osds[i])
              new_b.osd_used_capacity[new_b_osds[i]] = used_capacity
        
              if new_b_osds[i] not in b_osds:
                  diff_rep_cnt += 1
                  if new_b_osds[i] < b.disk_num:
                    remap_to_old_osd_cnt += 1
                    
  # # save osd_used_capacity to file
  # with open('raw_crush_b_osd_used_capacity70.json', 'w') as f:
  #     json.dump(b.osd_used_capacity, f)
      
  # # save osd_used_capacity to file
  # with open('raw_crush_new_b_osd_used_capacity70.json', 'w') as f:
  #     json.dump(new_b.osd_used_capacity, f)

  # # read osd_used_capacity from file
  # b.osd_used_capacity.clear()
  # with open('raw_crush_b_osd_used_capacity70.json', 'r') as f:
  #     b.osd_used_capacity = json.load(f)
  #     b.osd_used_capacity = {int(k): v for k, v in b.osd_used_capacity.items()}
  #     # print(b.osd_used_capacity)
  # new_b.osd_used_capacity.clear()
  # with open('raw_crush_new_b_osd_used_capacity70.json', 'r') as f:
  #     new_b.osd_used_capacity = json.load(f)
  #     new_b.osd_used_capacity = {int(k): v for k, v in new_b.osd_used_capacity.items()}
  #     # print(new_b.osd_used_capacity)
  
                    
  print("different replication count:", diff_rep_cnt)
  print("total object number:", inode_num*inode_size)
  print("different replication count percentage:", diff_rep_cnt/(inode_num*inode_size*b.replication_count))
  print("remap to old osd replication count percentage:", remap_to_old_osd_cnt/(inode_num*inode_size*b.replication_count))
  print("osd used capacity before expansion" )
  b.print_osd_used_capacity(0, 0)
  print("osd used capacity after expansion" )
  new_b.print_osd_used_capacity(0, 0)
  
  # # 6 write the second batch of objects. Use the new PG weight
  # # write the same number of objects as the first batch
  # count = 0
  # div = float(new_b.disk_num / b.disk_num)
  # for inode_no in range(inode_num, int(div * inode_num)):
  #     for block_no in range(inode_size):
  #         count = count + 1
  #         if count % 10000 == 0:
  #             sys.stdout.write("\r%d" % count)  
  #             sys.stdout.flush()
  #         # key format: "10000000001.00000002"
  #         key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
  #         raw_pg_id = new_b.c.c.ceph_str_hash_rjenkins(key, len(key))
  #         pg_id = utils.ceph_stable_mod(raw_pg_id, new_b.pg_num, new_b.pg_num_mask)

  #         # start crush
  #         new_b_osds = new_b.pg_to_osds.get(pg_id, [])
          
  #         for i in range(len(new_b_osds)):
  #             used_capacity = new_b.osd_used_capacity.get(new_b_osds[i], 0)
  #             if new_b.crush_type == 'ec':
  #                 used_capacity += (new_b.obj_size / new_b.k)
  #             elif new_b.crush_type == 'rep':
  #                 used_capacity += new_b.obj_size
  #             # if used_capacity >= self.disk_size*1024:
  #                 # print("overflow! osd id:", osds[i])
  #             new_b.osd_used_capacity[new_b_osds[i]] = used_capacity
  
  # print("osd used capacity after writng the second batch of objects:")
  # max_osd_id, min_osd_id = new_b.print_pg_in_osd()
  # new_b.print_osd_used_capacity(max_osd_id, min_osd_id)
  
# Test the overall balance when the number of objects 
# in each pg perfectly matches the respective weights
# def test_ideal_weight(new_b, overall_obj_num, max_after_expan, min_after_expan):
#   new_b.osd_used_capacity.clear()
#   with open('new_b_osd_used_capacity70.json', 'r') as f:
#       new_b.osd_used_capacity = json.load(f)
#       new_b.osd_used_capacity = {int(k): v for k, v in new_b.osd_used_capacity.items()}
#   print("ideal weight test")
#   print("Test the overall balance when the number of objects in each pg perfectly matches the respective weights")
#   print("overall object number:", overall_obj_num)
#   overall_weight = sum(new_b.pg_weight.values())
#   write_obj_num = 0
#   for pg, weight in new_b.pg_weight.items():
#     obj_num = overall_obj_num * weight / overall_weight
#     write_obj_num += obj_num
#     # start crush
#     new_b_osds = new_b.pg_to_osds.get(pg, [])
    
#     for i in range(len(new_b_osds)):
#         used_capacity = new_b.osd_used_capacity.get(new_b_osds[i], 0)
#         if new_b.crush_type == 'ec':
#             used_capacity += (obj_num * new_b.obj_size / new_b.k)
#         elif new_b.crush_type == 'rep':
#             used_capacity += (obj_num *  new_b.obj_size)
#         # if used_capacity >= self.disk_size*1024:
#             # print("overflow! osd id:", osds[i])
#         new_b.osd_used_capacity[new_b_osds[i]] = used_capacity
  
  # print("write object number:", write_obj_num)
  # print("osd used capacity after writng the second batch of objects:")
  # max_osd_id, min_osd_id = new_b.print_pg_in_osd()
  # new_b.print_osd_used_capacity(max_after_expan, min_after_expan)
  
  
  
def weightedPG_compare(b, new_b, inode_num, inode_size):
  b.osd_used_capacity.clear()
  new_b.osd_used_capacity.clear()
  # power = 4.5
  b.cal_pg_weight(b.power)
  new_b.cal_pg_weight(new_b.power)
  
  # diff_pg_weight_cnt = 0
  # for pg in range (b.pg_num):
  #   if b.pg_weight[pg] != new_b.pg_weight[pg]:
  #     diff_pg_weight_cnt += 1
  #     # print("pg weight not equal!")
  # print("different pg weight count:", diff_pg_weight_cnt)
  
  b.init_pg_Rendezvous_node()
  new_b.init_pg_Rendezvous_node()
  hash_funcs = [b.hash1, b.hash2, b.hash3, b.hash4, b.hash5]
  print("hash func num:", len(hash_funcs))
  print("weight power:", b.power)
  collision_num = 0
  count = 0
  diff_rep_cnt = 0
  remap_to_old_osd_cnt = 0
  diff_pg_cnt = 0
  for inode_no in range(inode_num):
      # print(inode_num-inode_no)
      for block_no in range(inode_size):
          count = count + 1
          if count % 10000 == 0:
              sys.stdout.write("\r%d" % count)  
              sys.stdout.flush()
          # key format: "10000000001.00000002"
          key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
          b_pg_id = b.select_pg(key)
          # new_b_pg_id = new_b.select_pg(key)
          # don't update pg weight
          new_b_pg_id = b_pg_id
          # if b_pg_id != new_b_pg_id:
          #     diff_pg_cnt += 1

          # start crush
          b_osds = b.pg_to_osds.get(b_pg_id, [])
          new_b_osds = new_b.pg_to_osds.get(new_b_pg_id, [])
          
          for i in range(len(new_b_osds)):
              used_capacity = b.osd_used_capacity.get(b_osds[i], 0)
              if b.crush_type == 'ec':
                  used_capacity += (b.obj_size / b.k)
              elif b.crush_type == 'rep':
                  used_capacity += b.obj_size
              # if used_capacity >= self.disk_size*1024:
                  # print("overflow! osd id:", osds[i])
              b.osd_used_capacity[b_osds[i]] = used_capacity
              
              used_capacity = new_b.osd_used_capacity.get(new_b_osds[i], 0)
              if new_b.crush_type == 'ec':
                  used_capacity += (new_b.obj_size / new_b.k)
              elif new_b.crush_type == 'rep':
                  used_capacity += new_b.obj_size
              # if used_capacity >= self.disk_size*1024:
                  # print("overflow! osd id:", osds[i])
              new_b.osd_used_capacity[new_b_osds[i]] = used_capacity
        
              if new_b_osds[i] not in b_osds:
                  diff_rep_cnt += 1
                  if new_b_osds[i] < b.disk_num:
                    remap_to_old_osd_cnt += 1
  
  
  print("different replication count:", diff_rep_cnt)
  print("total object number:", inode_num*inode_size)
  print("different replication count percentage:", diff_rep_cnt/(inode_num*inode_size*b.replication_count))
  print("remap to old osd replication count percentage:", remap_to_old_osd_cnt/(inode_num*inode_size*b.replication_count))
  print("different pg count percentage:", diff_pg_cnt/(inode_num*inode_size))
  print("osd used capacity before expansion:")
  b.print_osd_used_capacity(0, 0)
  print("osd used capacity after expansion:")
  max_after_expan, min_after_expan = new_b.print_osd_used_capacity(0, 0)

  
  # 5.1 calculate the remaining capacity of each osd
  new_b_remaining_capacity = {}
  min_remaining_capacity = sys.maxsize
  max_remaining_capacity = 0
  for osd in new_b.osd_used_capacity.keys():
    new_b_remaining_capacity[osd] = new_b.disk_size*1024 - new_b.osd_used_capacity[osd]
    # if new_b_remaining_capacity[osd] < min_remaining_capacity:
    #   min_remaining_capacity = new_b_remaining_capacity[osd]
    if new_b_remaining_capacity[osd] < min_remaining_capacity:
      min_remaining_capacity = new_b_remaining_capacity[osd]
      
  # 5.2 calculate the weight of each osd(depending on the remaining capacity)
  # new_b_osd_weight = {}
  for osd in new_b.osd_used_capacity.keys():
    new_b.osd_weight[osd] = float(new_b_remaining_capacity[osd]) / min_remaining_capacity
    # new_b.osd_weight[osd] = float(new_b_remaining_capacity[osd]) / max_remaining_capacity
    # new_b.osd_weight[osd] = float(new_b_remaining_capacity[osd]) / new_b.disk_size/1024
    # new_b.osd_weight[osd] = 1
    # print("osd id:", osd, "weight:", new_b.osd_weight[osd])
  
  overall_obj_num = inode_num*inode_size * 2 * new_b.replication_count
  overall_capacity = new_b.disk_size*1024 * new_b.disk_num
  overall_utilization = overall_obj_num * new_b.obj_size / overall_capacity
  print("overall utilization:", overall_utilization)
  
  power = 4
  print("power:", power)
  # new_b.osd_used_capacity = new_b_osd_used_capacity
  new_b.cal_pg_weight(power)
  new_b.init_pg_Rendezvous_node()
  
  # 6 write the second batch of objects. Use the new PG weight
  # write the same number of objects as the first batch
  # obj_to_pg = {}
  div = float(new_b.disk_num / b.disk_num)
  for inode_no in range(inode_num, int(div * inode_num)):
      for block_no in range(inode_size):
          count = count + 1
          if count % 10000 == 0:
              sys.stdout.write("\r%d" % count)  
              sys.stdout.flush()
          # key format: "10000000001.00000002"
          key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
          new_b_pg_id = new_b.select_pg(key)
          # obj_to_pg[key] = new_b_pg_id
          # start crush
          new_b_osds = new_b.pg_to_osds.get(new_b_pg_id, [])
          
          for i in range(len(new_b_osds)):
              used_capacity = new_b.osd_used_capacity.get(new_b_osds[i], 0)
              if new_b.crush_type == 'ec':
                  used_capacity += (new_b.obj_size / new_b.k)
              elif new_b.crush_type == 'rep':
                  used_capacity += new_b.obj_size
              # if used_capacity >= self.disk_size*1024:
                  # print("overflow! osd id:", osds[i])
              new_b.osd_used_capacity[new_b_osds[i]] = used_capacity
  
  
  print("osd used capacity after writng the second batch of objects:")
  max_osd_id, min_osd_id = new_b.print_pg_in_osd()
  new_b.print_osd_used_capacity(max_after_expan, min_after_expan)
  
# def expansion_test(hash_type):
def expansion_test(host_num, disk_num, pg_num, overall_used_capacity, power, type, k, m):
  b = Balancer(host_num, disk_num, pg_num, overall_used_capacity, power, type, k, m)

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
  print("overall utilization:", overall_utilization)

  target_capacity = disk_capacity*overall_utilization*1024
  if b.crush_type == 'rep':
      obj_load_num = int(target_capacity/b.replication_count/b.obj_size)
  elif b.crush_type == 'ec':
      redun = (b.k + b.m)/b.k
      obj_load_num = int(target_capacity/redun/b.obj_size)

  # obj_load_num = int(disk_capacity*overall_utilization*1024/b.replication_count/b.obj_size)
  print("object load number:", obj_load_num)
  inode_num = int(obj_load_num/inode_size)
  print("inode number:", inode_num)
  # each inode has {inode_size} blocks
  # each block is a object

  # oringinal cluster
  crushmap = b.gen_crushmap(b.n_host, b.n_disk, add_a_disk=False)
  print("before expansion: host num:", b.n_host, " disk num:", b.n_disk)
  # print(self.crushmap)
  b.c = Crush()
  b.c.parse(crushmap)
  b.crush_pg_to_osds()
  # b.write_obj(inode_num, inode_size)

  # expansion
  host_add_num = 1
  new_b = Balancer(b.n_host + host_add_num, disk_num, pg_num, overall_used_capacity, power, type, k, m)
  new_b.n_host = b.n_host + host_add_num
  new_b.disk_num = new_b.n_host * new_b.n_disk
  for osd in range(b.disk_num, new_b.disk_num):
    new_b.osd_weight[osd] = 1
  print("after expansion: host num:", new_b.n_host, " disk num:", new_b.n_disk)
  
  # new_crushmap = new_b.crushmap_add_host(crushmap, host_add_num, new_b.n_disk)
  new_crushmap = new_b.gen_crushmap(new_b.n_host, new_b.n_disk, add_a_disk=False)
  new_b.c = Crush()
  new_b.c.parse(new_crushmap)
  new_b.crush_pg_to_osds()
  # new_b.write_obj(inode_num, inode_size)
  
  print("raw crush compare")
  raw_crush_compare(b, new_b, inode_num, inode_size)
  print("weighted PG compare")
  weightedPG_compare(b, new_b, inode_num, inode_size)

logger = Logger("scaling_test_log.txt")
test_cases = [
    (3, 1, 4, 'rep', 3, 0, 4.5),
    (3, 1, 4, 'ec', 4, 2, 10.5),
    (3, 1, 12, 'rep', 3, 0, 4),
    (3, 1, 12, 'ec', 4, 2, 8.5),
    (5, 1, 4, 'rep', 3, 0, 4.5),
    (5, 1, 4, 'ec', 8, 2, 19.5),
    (5, 1, 12, 'rep', 3, 0, 4),
    (5, 1, 12, 'ec', 8, 2, 13.5),
    (6, 2, 4, 'rep', 3, 0, 4),
    (6, 2, 4, 'ec', 4, 2, 8),
    (6, 2, 12, 'rep', 3, 0, 4),
    (6, 2, 12, 'ec', 4, 2, 8),
    (6, 2, 32, 'rep', 3, 0, 3.5),
    (6, 2, 32, 'ec', 4, 2, 7.5),
    (10, 2, 4, 'rep', 3, 0, 4),
    (10, 2, 4, 'ec', 8, 2, 13.5),
    (10, 2, 12, 'rep', 3, 0, 4),
    (10, 2, 12, 'ec', 8, 2, 13.7),
    (10, 2, 32, 'rep', 3, 0, 4),
    (10, 2, 32, 'ec', 8, 2, 12),
]
# host_num = 4
# disk_num = 12
# pg_num = 4096
# overall_used_capacity = 0.9
# power = 8
# type = 'ec'
# k = 4
# m = 2
for host_num, extra_hosts, disk_num, type, k, m, power in test_cases:
  # for overall_used_capacity in [0.9]:
    overall_used_capacity = 0.9
    print("-------------------------------------------------------------------")
    total_hosts = host_num + extra_hosts
    print("host_num:", total_hosts, "disk_num:", disk_num, "overall_used_capacity:", overall_used_capacity)
    print("type:", type, "k:", k, "m:", m)
    total_disk_num = total_hosts * disk_num
    pg_num = int(100 * total_disk_num * overall_used_capacity / (k+m))  # Suggest PG Count = (Target PGs per OSD) x (OSD#) x (%Data) / (Size)
    pg_num = (1 << (pg_num.bit_length())) * 2
    if total_hosts == 12 and disk_num == 32 and type == 'ec':
      pg_num *= 2
    
    expansion_test(total_hosts, disk_num, pg_num, overall_used_capacity, power, type, k, m)


