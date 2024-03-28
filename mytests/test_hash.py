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

import csv

c = Crush()
length = 10000
inode_num = 4096
inode_size = 512
engine_num = 12*32
engine_id = [i for i in range(engine_num)]
objs = []
# print(engine_id)
for inode_no in range(inode_num):
    # print(inode_num-inode_no)
    for block_no in range(inode_size):
        key = "100" + str('%08x' % inode_no) + '.' + str('%08x' % block_no)
        # print(key)
        engine_id = c.c.ceph_str_hash_rjenkins(key, len(key)) % engine_num
        # print(engine_id)
        if engine_id == 12:
            objs.append(key)
                
# for i in range(length):
#     key = "100" + str('%08x' % 454) + '.' + str('%08x' % (3760+i))
#     engine_id = self.engine_id[self.c.c.ceph_str_hash_rjenkins(key, len(key)) % self.engine_num]
#     key = "100" + str('%08x' % 454) + '.' + str('%08x' % (3760+i))
#     print(c.c.ceph_str_hash_rjenkins(key, len(key)) % 16)

# objs = ['100000001c6.00000eb0', '100000001c6.00000f9d', '100000001c7.000001a0', '100000001c7.000003ed', '100000001c7.000004d3', '100000001c7.000004f2', '100000001c7.00000627', '100000001c7.00000741', '100000001c7.00000746', '100000001c7.00000bfe', '100000001c7.00000c50', '100000001c7.00000c78', '100000001c8.00000579', '100000001c8.000008c5', '100000001c8.00000d8d', '100000001c8.00000e48', '100000001c8.00000f2d', '100000001c9.0000011c', '100000001c9.0000015c', '100000001c9.00000629', '100000001c9.0000064a', '100000001c9.000008ef', '100000001c9.00000920', '100000001c9.00000a16', '100000001c9.00000b91', '100000001c9.00000c1f', '100000001c9.00000cb8', '100000001c9.00000f21', '100000001c9.00000f27', '100000001ca.00000077', '100000001ca.0000015c', '100000001ca.0000053d', '100000001ca.00000543', '100000001ca.00000630', '100000001ca.000006d0', '100000001ca.00000bb3', '100000001ca.00000c00', '100000001ca.00000c7c', '100000001ca.00000cef', '100000001ca.00000e72', '100000001cb.0000010d', '100000001cb.00000930', '100000001cb.00000952', '100000001cb.00000ac3', '100000001cb.00000b51', '100000001cb.00000c1b', '100000001cb.00000c9b', '100000001cb.00000f94']
for obj in objs:
    print((c.c.ceph_str_hash_rjenkins(obj, len(obj))) % 24)