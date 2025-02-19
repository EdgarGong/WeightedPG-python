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

import csv


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
        with open("config/engine-config.json") as f:
            config = json.load(f)

        print("config:")
        print(config)

        # self.weigted = config['weighted']

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

        # config osd in host
        self.disk_num = self.n_host*self.n_disk
        # self.osd_in_host = {}
        osd_id = 0
        self.osd_to_host = {}
        for host_id in range(self.n_host):
            for j in range(self.n_disk):
                # self.osd_in_host[host_id] = osd_id
                self.osd_to_host[osd_id] = host_id
                osd_id += 1
        
        self.osd_pg_num = {}

        # config engine in host
        self.engine_per_host = config['engine_per_host']
        self.engine_num = self.engine_per_host * self.n_host
        # self.engine_in_host = {}
        self.engine_to_host = {}

        engine_id = 0
        for host_id in range(self.n_host):
            for j in range(self.engine_per_host):
                # self.engine_in_host[host_id] = engine_id
                self.engine_to_host[engine_id] = host_id
                engine_id += 1

        self.obj_in_engine = {}

        self.pg_pri_in_host = {}
        self.pg_rep_in_host = {}

        self.pg_to_osds = {}

        self.osd_used_capacity = {}

        self.obj_num_in_host = {}

        # engine_num = disk_num, Each engine corresponds to one osd.
        # self.engine_num = self.disk_num
        # self.engine_id = [i for i in range(self.engine_num)]
        # self.obj_in_engine = {}

        
        # self.pg_num = int(100 * self.disk_num * self.overall_used_capacity / self.replication_count)  # Suggest PG Count = (Target PGs per OSD) x (OSD#) x (%Data) / (Size)
        # self.pg_num = 1 << (self.pg_num.bit_length() - 1)
        self.pg_num = config['pg_num']

        self.pg_num_mask = self.compute_pgp_num_mask(self.pg_num)
        
        # print("pg num:", bin(self.pg_num), " pg num mask:", bin(self.pg_num_mask))

        
        self.crushmap = self.gen_crushmap(self.n_host, self.n_disk)
        # print(self.crushmap)
        self.c = Crush()
        # self.crushmap_json = json.loads(self.crushmap)
        self.c.parse(self.crushmap)
        
        # self.pg_pri_in_osd = {}
        # self.pg_rep_in_osd = {}
        self.pg_in_osd = {}
        self.osd_used_capacity = {}
        self.obj_in_pg = {}
        self.obj_num_in_pg = {}

        self.pg_weight = {}
        self.host_weight = {}

        self.pg_Rendezvous_node = {}
        self.host_Rendezvous_node = {}

        # self.grouped_data = {}
        # self.group_Rendezvous_node = {}

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

    def fill_engine(self, inode_num, inode_size):
        for inode_no in range(inode_num):
            # print(inode_num-inode_no)
            for block_no in range(inode_size):
                # key format: "10000000001.00000002"
                key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
                # print(key)
                hash_value = self.c.c.ceph_str_hash_rjenkins(key, len(key)) % self.engine_num
                engine_id = hash_value

                hostid = self.engine_to_host[engine_id]
                obj_num = self.obj_num_in_host.get(hostid, 0)
                obj_num += 1
                self.obj_num_in_host[hostid] = obj_num

                # print(engine_id)
                objs = self.obj_in_engine.get(engine_id, [])
                objs.append(key)
                self.obj_in_engine[engine_id] = objs

    def fill_engine_weight_hash(self, inode_num, inode_size, hash_func_num):
        print("start object Rendezvous hash to engine")
        # initial engine Rendezvous node
        assert len(self.host_weight) > 0
        engine_Rendezvous_node = {}
        for engine_id, host_id in self.engine_to_host.items():
            engine_weight = self.host_weight[host_id]
            engine_Rendezvous_node[engine_id] = rendezvousNode(engine_id, engine_weight)
        all_hash_funcs = [self.hash1, self.hash2, self.hash3, self.hash4, self.hash5, self.hash6, self.hash7, self.hash8]
        hash_funcs = []
        for i in range(hash_func_num):
            hash_funcs.append(all_hash_funcs[i])
        print("hash func num:", len(hash_funcs))
        collision_num = 0
        count = 0


        for inode_no in range(inode_num):
            # print(inode_num-inode_no)
            for block_no in range(inode_size):
                count = count + 1
                # if count % 10000 == 0:
                #     sys.stdout.write("\r%d" % count)  
                #     sys.stdout.flush()
                
                # key format: "10000000001.00000002"
                key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
                # print(key)

                # Rendevous hashing
                # use serveral different hash function to choose 2 pgs.
                engine_ids = set()
                for hash_func in hash_funcs:
                    hash_value = hash_func(key, self.engine_num)
                    elem = hash_value
                    while elem in engine_ids:
                        # print("hash collision")
                        collision_num += 1
                        hash_value = (hash_value + 1) % self.engine_num
                        elem = hash_value
                    engine_ids.add(elem)
                
                engine_nodes = []
                for engine_id in engine_ids:
                    engine_nodes.append(engine_Rendezvous_node[engine_id])
                
                max_engine_node = max(engine_nodes, key=lambda x: x.compute_weighted_score(key))
                engine_id = max_engine_node.id
                
                # hash_value = self.c.c.ceph_str_hash_rjenkins(key, len(key)) % self.engine_num
                # engine_id = hash_value

                hostid = self.engine_to_host[engine_id]
                obj_num = self.obj_num_in_host.get(hostid, 0)
                obj_num += 1
                self.obj_num_in_host[hostid] = obj_num

                # print(engine_id)
                objs = self.obj_in_engine.get(engine_id, [])
                objs.append(key)
                self.obj_in_engine[engine_id] = objs
        
        for hostid, obj_num in self.obj_num_in_host.items():
            print("host id:", hostid, " obj num:", obj_num, " host weight:", self.host_weight[hostid])

    def calc_engine_deviation(self):
        s = 0
        for _, objs in self.obj_in_engine.items():
            s += len(objs)
        avg = float(s) / self.engine_num

        max_dev = 1.0
        min_dev = 1.0
        max_obj = 0
        min_obj = len(self.obj_in_engine[0])
        for _, objs in self.obj_in_engine.items():
            dev = float(len(objs)) / avg
            max_dev = max(max_dev, dev)
            min_dev = min(min_dev, dev)
            max_obj = max(max_obj, len(objs))
            min_obj = min(min_obj, len(objs))
        logger.log("Objects in Engines MIN/MAX objects number: " + str(min_obj) + "/" + str(max_obj))
        logger.log("Objects in Engines MIN/MAX deviation: " + str(min_dev) + "/" + str(max_dev))


    # CRUSH PG to OSDs
    def fill_osd_with_pg(self):
        for pg_no in range(self.pg_num):
            pps = np.int32(
                utils.crush_hash32_rjenkins1_2(utils.ceph_stable_mod(pg_no, self.pg_num, self.pg_num_mask), 3))  # 3 is pool no., not rep count
            osds = self.c.map(rule="default_rule", value=pps, replication_count=self.replication_count)
            self.pg_to_osds[pg_no] = osds
            for i in range(len(osds)):
                osd_id = osds[i]
                pg_num = self.osd_pg_num.get(osd_id, 0)
                pg_num += 1
                self.osd_pg_num[osd_id] = pg_num

                # pg_in_osd
                pgs = self.pg_in_osd.get(osds[i], [])
                pgs.append(pg_no)
                self.pg_in_osd[osds[i]] = pgs

                host_id = self.osd_to_host[osd_id]
                if i == 0:
                    pgs = self.pg_pri_in_host.get(host_id, [])
                    pgs.append(pg_no)
                    self.pg_pri_in_host[host_id] = pgs
                else:
                    pgs = self.pg_rep_in_host.get(host_id, [])
                    pgs.append(pg_no)
                    self.pg_rep_in_host[host_id] = pgs
        # print(self.pg_pri_in_osd)
        # print(self.pg_rep_in_osd)
                    
    def calc_pg_in_osd_deviation(self):
        max_pg = 0
        min_pg = self.osd_pg_num[0]
        pg_sum = 0
        # max_pg_osdid = 0
        # max_pri_pg = 0
        # min_pri_pg = len(self.pg_pri_in_osd.get(0, []))
        # pri_pg_sum = 0
        # max_pri_pg_osdid = 0
        for osd in range(self.disk_num):
            # pri_pg_count = len(self.pg_pri_in_osd.get(osd, []))
            pg_count = self.osd_pg_num[osd]
            if pg_count > max_pg:
                max_pg = pg_count
                max_pg_osdid = osd
            min_pg = min(min_pg, pg_count)
            # if pri_pg_count > max_pri_pg:
            #     max_pri_pg = pri_pg_count
            #     max_pri_pg_osdid = osd
            # min_pri_pg = min(min_pg, pri_pg_count)
            pg_sum += pg_count
            # pri_pg_sum += pri_pg_count

        avg_pg = float(pg_sum) / self.disk_num
        logger.log("PGs in OSDs MIN/MAX/AVG PGs number: " + str(min_pg) + "/" + str(max_pg) + "/" + str(avg_pg))
        
        # avg_pri_pg = float(pri_pg_sum) / self.disk_num
        # logger.log("pri_PGs in OSDs MIN/MAX/AVG pri_PGs number: " + str(min_pri_pg) + "/" + str(max_pri_pg) + "/" + str(avg_pri_pg))
        # logger.log("PGS/pri_PGs in OSDs MAX OSD id: " + str(max_pg_osdid) + "/" + str(max_pri_pg_osdid))

    def engine_to_pg_naive_hash(self):
        for engine_id, objs in self.obj_in_engine.items():
            # the pgs that take the osd that share the same host with this engine as the primary osd
            # osd_id = engine_id
            host_id = self.engine_to_host[engine_id]
            pg_pri = self.pg_pri_in_host.get(host_id, [])
            pg_dict = {pg: False for pg in pg_pri}
            for obj in objs:
                # ?????
                # hash to one of the pgs that take the osd corresponding to this engine as the primary osd
                hash_value = self.c.c.ceph_str_hash_rjenkins(obj, len(obj)) % len(pg_pri)
                pg_id = pg_pri[hash_value]
                pg_dict[pg_id] = True
                tmp_objs = self.obj_in_pg.get(pg_id, [])
                tmp_objs.append(obj)
                self.obj_in_pg[pg_id] = tmp_objs

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



            # for debug
            missing_pgs = [pg for pg, value in pg_dict.items() if not value]
            # logger.log("pg_pri number:" + str(len(pg_pri)) + " miss pgs number:" + str(len(missing_pgs)))
            if len(pg_pri) - len(missing_pgs) <= 3:
                logger.log("pg_pri number:" + str(len(pg_pri)) + " miss pgs number:" + str(len(missing_pgs)))
                # print("pg_pri", pg_pri)
                # print("missing pgs", missing_pgs)
                hash_values = {}
                print(objs)
                for obj in objs:
                    hash_value = self.c.c.ceph_str_hash_rjenkins(obj, len(obj)) % len(pg_pri)
                    hash_values[hash_value] = True
                print(hash_values, len(pg_pri))

            obj_counts = [len(objs) for pg_id, objs in self.obj_in_pg.items() if pg_id in pg_pri]
            max_count = max(obj_counts) if obj_counts else 0
            min_count = min(obj_counts) if obj_counts else 0
            # if len(missing_pgs) != 0:
            #     min_count = 0
            avg_count = sum(obj_counts) / len(obj_counts) if obj_counts else 0
            # logger.log("max/min/avg obj:" + str(max_count) + "/" + str(min_count) + "/" + str(avg_count))

    def gen_pg_crushmap(self, pg_pri_num):
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
        return crushmap

  
    def compute_pgp_num_mask(self, num):
        # Calculate the pgp_num_mask for given pgp_num
        return (1 << (num - 1).bit_length()) - 1
    
    
    def engine_to_pg_crush(self):
        for engine_id, objs in self.obj_in_engine.items():
            # the pgs that take the osd corresponding to this engine as the primary osd
            host_id = self.engine_to_host[engine_id]
            pg_pri = self.pg_pri_in_host.get(host_id, [])
            pg_pri_num = len(pg_pri)
            # pg_pri_num_mask = pg_pri_num - 1
            pg_dict = {pg: False for pg in pg_pri}
            crushmap = self.gen_pg_crushmap(pg_pri_num)
            c = Crush()
            c.parse(crushmap)
            for obj in objs:
                # hash to one of the pgs that take the osd corresponding to this engine as the primary osd
                # hash_value = self.c.c.ceph_str_hash_rjenkins(obj, len(obj)) % len(pg_pri)
                # ????
                # s_mod = hash(obj) % len(objs)
                s_mod = utils.ceph_stable_mod(hash(obj), len(objs), self.compute_pgp_num_mask(len(objs)))
                # if s_mod != s_mod2:
                #     print("!=, s_mod/s_mod2:", s_mod, s_mod2)
                pps = np.int32(
                utils.crush_hash32_rjenkins1_2(s_mod, engine_id))  # replace the pool no(original 3) with engine id
                
                osds = c.map(rule="default_rule", value=pps, replication_count=1)
                pg_id = pg_pri[osds[0]]
                pg_dict[pg_id] = True
                tmp_objs = self.obj_in_pg.get(pg_id, [])
                tmp_objs.append(obj)
                self.obj_in_pg[pg_id] = tmp_objs

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


            # max_objs = 0
            # min_objs = len(self.obj_in_pg[pg_pri[0]])

            # for pgid in pg_pri:
            #     len_objs = len(self.obj_in_pg[pgid])
            #     max_objs = max(max_objs, len_objs)
            #     min_objs = min(min_objs, len_objs)
            # avg_objs = float(len(objs)) / len(pg_pri)
            # logger.log("engine_to_pg_crush min/max/avg objs in pg:" + str(min_objs) + "/" + str(max_objs) + "/" + str(avg_objs))
            
            missing_pgs = [pg for pg, value in pg_dict.items() if not value]
            # logger.log("pg_pri number:" + str(len(pg_pri)) + " miss pgs number:" + str(len(missing_pgs)))
            if len(missing_pgs) >= 3:
                logger.log("pg_pri number:" + str(len(pg_pri)) + " miss pgs number:" + str(len(missing_pgs)))
                # print("pg_pri", pg_pri)
                # print("missing pgs", missing_pgs)
                s_mods = {}
                ppss = {}
                pg_ids = {}
                # print(objs)
                # logger.log(pg_pri)
                # logger.log(engine_id)
                # logger.log(objs)
                for obj in objs:
                    s_mod = hash(obj) % len(pg_pri)
                    s_mods[s_mod] = True
                    pps = np.int32(
                    utils.crush_hash32_rjenkins1_2(s_mod, engine_id))  # replace the pool no(original 3) with engine id
                    ppss[pps] = True
                    osds = c.map(rule="default_rule", value=pps, replication_count=1)
                    pg_id = pg_pri[osds[0]]   
                    pg_ids[pg_id] = True
                print("pg_pri_num == len(s_mods) == len(ppss), pg_ids", pg_pri_num == len(s_mods) == len(ppss), len(pg_ids))

            obj_counts = [len(objs) for pg_id, objs in self.obj_in_pg.items() if pg_id in pg_pri]
            max_count = max(obj_counts) if obj_counts else 0
            min_count = min(obj_counts) if obj_counts else 0
            # if len(missing_pgs) != 0:
            #     min_count = 0
            avg_count = sum(obj_counts) / len(obj_counts) if obj_counts else 0
            # logger.log("max/min/avg obj:" + str(max_count) + "/" + str(min_count) + "/" + str(avg_count))


    def engine_to_pg_strip(self):
        for engine_id, objs in self.obj_in_engine.items():
            # the pgs that take the osd corresponding to this engine as the primary osd
            pg_pri = self.pg_pri_in_osd.get(engine_id, [])
            current_pg_idx = 0
            for obj in objs:
                pg_id = pg_pri[current_pg_idx]
                tmp_objs = self.obj_in_pg.get(pg_id, [])
                tmp_objs.append(obj)
                self.obj_in_pg[pg_id] = tmp_objs
                current_pg_idx = (current_pg_idx + 1) % len(pg_pri)

        # calculate pg weight
    def cal_pg_weight(self, power):
        # Calculate the average pg num of the osd corresponding to each pg
        pg_to_avg = {}
        for pg, osds in self.pg_to_osds.items():
            total_pg = 0
            for osd in osds:
                total_pg += len(self.pg_in_osd[osd])
            pg_to_avg[pg] = total_pg / len(osds)

        for pg, v in pg_to_avg.items():
            self.pg_weight[pg] = (1/v) ** power
        # # print(normalized_pg)

        # calculate pg weight
    def cal_pg_host_weight(self, power):
        # Calculate the average pg num of the osd corresponding to each pg
        pg_to_avg = {}
        for pg, osds in self.pg_to_osds.items():
            total_pg = 0
            for osd in osds:
                total_pg += len(self.pg_in_osd[osd])
            pg_to_avg[pg] = total_pg / len(osds)

        for pg, v in pg_to_avg.items():
            self.pg_weight[pg] = (1/v) ** power
        
        for hostid, pg_pris in self.pg_pri_in_host.items():
            host_weight = 0
            for pg_pri in pg_pris:
                host_weight += self.pg_weight[pg_pri]
            self.host_weight[hostid] = host_weight
        print(self.host_weight)
        # # print(normalized_pg)
            

    def init_pg_Rendezvous_node(self):
        assert len(self.pg_weight) > 0
        for pgid, v in self.pg_weight.items():
            self.pg_Rendezvous_node[pgid] = rendezvousNode(pgid, v)

    def hash1(self, key, mod):
        raw_pg_id = self.c.c.ceph_str_hash_rjenkins(key, len(key))
        mask = self.compute_pgp_num_mask(mod)
        return utils.ceph_stable_mod(raw_pg_id, mod, mask)
    
    def hash2(self, key, mod):
        return mmh3.hash(key) % mod
    
    def hash3(self, key, mod):
        return hash(key) % mod
    
    def hash4(self, key, mod):
        md5_hash = hashlib.md5(key).hexdigest()
        # hash_object = hashlib.md5()
        # hash_object.update(key)
        # md5_hash = hash_object.hexdigest()
        hashed_value_mod = int(md5_hash, 16) % mod
        return hashed_value_mod
    
    def hash5(self, key, mod):
        hash = hashlib.sha1(key).hexdigest()
        hashed_value_mod = int(hash, 16) % mod
        return hashed_value_mod

    def hash6(self, key, mod):
        hash = hashlib.sha256(key).hexdigest()
        hashed_value_mod = int(hash, 16) % mod
        return hashed_value_mod
    
    def hash7(self, key, mod):
        hash = hashlib.sha384(key).hexdigest()
        hashed_value_mod = int(hash, 16) % mod
        return hashed_value_mod
    
    def hash8(self, key, mod):
        hash = hashlib.sha512(key).hexdigest()
        hashed_value_mod = int(hash, 16) % mod
        return hashed_value_mod
    
    # use Rendezvous hash and "the Power of Two Choices". 
    # Each pg corresponds to a node
    def write_obj_with_Rendezvous_power_2(self, power, hash_func_num):
        self.cal_pg_weight(power)
        self.init_pg_Rendezvous_node()
        all_hash_funcs = [self.hash1, self.hash2, self.hash3, self.hash4, self.hash5, self.hash6, self.hash7, self.hash8]
        hash_funcs = []
        for i in range(hash_func_num):
            hash_funcs.append(all_hash_funcs[i])

        print("hash func num:", len(hash_funcs))
        print("weight power:", power)
        collision_num = 0
        count = 0

        for engine_id, objs in self.obj_in_engine.items():
            # the pgs that take the osd that share the same host with this engine as the primary osd
            # osd_id = engine_id
            host_id = self.engine_to_host[engine_id]
            pg_pri = self.pg_pri_in_host.get(host_id, [])
            # pg_dict = {pg: False for pg in pg_pri}
            for obj in objs:
                # count = count + 1
                # if count % 10000 == 0:
                #     sys.stdout.write("\r%d" % count)  
                #     sys.stdout.flush()

                # use serveral different hash function to choose 2 pgs.
                pg_ids = set()
                for hash_func in hash_funcs:
                    hash_value = hash_func(obj, len(pg_pri))
                    elem = pg_pri[hash_value]
                    while elem in pg_ids:
                        # print("hash collision")
                        collision_num += 1
                        hash_value = (hash_value + 1) % len(pg_pri)
                        elem = pg_pri[hash_value]
                    pg_ids.add(elem)

                pg_nodes = []
                for pg_id in pg_ids:
                    pg_nodes.append(self.pg_Rendezvous_node[pg_id])
                
                max_pg_node = max(pg_nodes, key=lambda x: x.compute_weighted_score(obj))
                pg_id = max_pg_node.id
                

                # pg_dict[pg_id] = True
                tmp_objs = self.obj_in_pg.get(pg_id, [])
                tmp_objs.append(obj)
                self.obj_in_pg[pg_id] = tmp_objs

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

    
    def print_pg_in_osd(self):
        max_pg_num = 0
        max_osdid = 0
        min_pg_num = sys.maxsize
        min_osdid = 0
        avg_pg_num = float(self.pg_num * self.replication_count) / self.disk_num
        for osd in range(self.disk_num):
            pg_num = self.osd_pg_num[osd]
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

    def calc_obj_in_pg_deviation(self):
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
        
        pg_counts = {pg: len(objs) for pg, objs in self.obj_in_pg.items()}
        # filename = "./obj_in_pg_engine.csv"
        # with open(filename, 'wb') as csvfile:
        #     writer = csv.writer(csvfile)
        #     writer.writerow(['PG id', 'Object Count'])
        #     for pgid, obj_count in pg_counts.items():
        #         writer.writerow([pgid, obj_count])
        # max_objects = max(pg_counts.values())
        # scale_factor = 50.0 / max_objects  # Adjust scale to fit your console width

        # for pg, num_objs in pg_counts.items():
        #     bar_length = int(num_objs * scale_factor)
        #     logger.log("PG {}: {} ({} objects)".format(pg, '*' * bar_length, num_objs))

    def calc_used_capacity_each_osd(self, max_osd_id, min_osd_id):
        obj_sum = 0
        obj_count_in_osd = {}
        for osd in self.disk_num:
            obj_count = 0
            pri_pgs = self.pg_pri_in_osd.get(osd, [])
            for pg in pri_pgs:
                objs = self.obj_in_pg.get(pg, [])
                obj_count += len(objs)
            rep_pgs = self.pg_rep_in_osd.get(osd, [])
            for pg in rep_pgs:
                objs = self.obj_in_pg.get(pg, [])
                obj_count += len(objs)

            obj_count_in_osd[osd] = obj_count
            obj_sum += obj_count
        # print("overall object sum:", obj_sum)
        overall_used_capacity = obj_sum*self.obj_size/(self.disk_num*self.disk_size*1024)
        print("overall used capacity:", overall_used_capacity)
        max_used_capacity = 0.0
        min_used_capacity = 1.0
        avg_used_capacity = float(obj_sum) / self.disk_num*self.obj_size/(self.disk_size*1024)
        
        for osd_id, obj_count in obj_count_in_osd.items():
            used_capacity = obj_count*self.obj_size/(self.disk_size*1024)
            # print("osd id:", osd_id, "used capacity:", used_capacity)
            max_used_capacity = max(max_used_capacity, used_capacity)
            min_used_capacity = min(min_used_capacity, used_capacity)
        print("max used capacity:", max_used_capacity, "min used capacity:", min_used_capacity)
        print("max used capacity deviation:", max_used_capacity/avg_used_capacity, "min used capacity deviation:", min_used_capacity/avg_used_capacity)
        
        print("max_pg_osd's used deviation:", float(obj_count_in_osd[max_osd_id]*self.obj_size/(self.disk_size*1024)) / avg_used_capacity)
        print("min_pg_osd's used deviation:", float(obj_count_in_osd[min_osd_id]*self.obj_size/(self.disk_size*1024)) / avg_used_capacity)
    
    
    
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
        logger.log("MAX/MIN used capacity deviation: " + str(max_used_capacity/avg_capacity)+ "  " + str(min_used_capacity/avg_capacity) + " difference:" + str(max_used_capacity/avg_capacity - min_used_capacity/avg_capacity))
        # print("max used capacity deviation:", max_used_capacity/avg_capacity, "min used capacity deviation:", min_used_capacity/avg_capacity)
        logger.log("MAX/MIN used osd id:" + str(max_osdid)+ "/" + str(min_osdid))
        # print("max used osd id:", max_osdid, "min used osd id:", min_osdid)
        logger.log("MAX/MIN pg osd's used deviation:" + str(float(self.osd_used_capacity[max_osd_id]) / avg_capacity)+ "/" + str(float(self.osd_used_capacity[min_osd_id]) / avg_capacity))
        # print("max_pg_osd's used deviation:", float(self.osd_used_capacity[max_osd_id]) / avg_capacity)
        # print("min_pg_osd's used deviation:", float(self.osd_used_capacity[min_osd_id]) / avg_capacity)



    def calc_obj_deviation_in_osd(self):
        obj_sum = 0
        obj_count_in_osd = {}
        for osd in self.engine_id:
            obj_count = 0
            pri_pgs = self.pg_pri_in_osd.get(osd, [])
            for pg in pri_pgs:
                objs = self.obj_in_pg.get(pg, [])
                obj_count += len(objs)
            rep_pgs = self.pg_rep_in_osd.get(osd, [])
            for pg in rep_pgs:
                objs = self.obj_in_pg.get(pg, [])
                obj_count += len(objs)

            obj_count_in_osd[osd] = obj_count
            obj_sum += obj_count

        print("overall object sum:", obj_sum)
        avg_obj = float(obj_sum) / self.engine_num

        max_dev = 1.0
        min_dev = 1.0
        for _, obj_count in obj_count_in_osd.items():
            dev = float(obj_count) / avg_obj
            max_dev = max(max_dev, dev)
            min_dev = min(min_dev, dev)
        logger.log("Objects in OSD MIN/MAX deviation: " + str(min_dev) + "/" + str(max_dev))

    def calc_obj_deviation_in_osd_with_new_rep(self, pg_rep_in_osd):
        obj_sum = 0
        obj_count_in_osd = {}
        for osd in self.engine_id:
            obj_count = 0
            pri_pgs = self.pg_pri_in_osd.get(osd, [])
            for pg in pri_pgs:
                objs = self.obj_in_pg.get(pg, [])
                obj_count += len(objs)
            rep_pgs = pg_rep_in_osd.get(osd, [])
            for pg in rep_pgs:
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


logger = Logger("log-new.txt")

b = Balancer()
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



print("object load number:", obj_load_num)
inode_num = int(obj_load_num/inode_size)
print("inode number:", inode_num)
# each inode has {inode_size} blocks
# each block is a object
b.fill_osd_with_pg()

host_weight_power = 5
fill_engine_hash_func_num = 8
b.cal_pg_host_weight(host_weight_power)
b.fill_engine_weight_hash(inode_num, inode_size, fill_engine_hash_func_num)
# b.fill_engine(inode_num, inode_size)

# b.calc_engine_deviation()

print("start engine to pg crush")
# b.engine_to_pg_naive_hash()
# b.engine_to_pg_crush()

for hostid, pg_pri in b.pg_pri_in_host.items():
    print("host id:" , hostid, " pg pri num:", len(pg_pri))


pg_weight_power = 4
write_obj_hash_func_num = 7
b.write_obj_with_Rendezvous_power_2(pg_weight_power, write_obj_hash_func_num)

# b.engine_to_pg_naive_hash()
# b.engine_to_pg_strip()

max_osdid, min_osd_id = b.print_pg_in_osd()
b.calc_engine_deviation()
# b.calc_pg_in_osd_deviation()
b.calc_obj_in_pg_deviation()
# b.calc_used_capacity_each_osd(max_osdid, min_osd_id)
b.print_osd_used_capacity(max_osdid, min_osd_id)
# b.calc_obj_deviation_in_osd()
# b.calc_pri_obj_deviation_in_osd()

# logger.log("obj upmap total_did:{}".format(total_did))


 