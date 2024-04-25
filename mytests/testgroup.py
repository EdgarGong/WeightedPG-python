import numpy as np
import math

def calculate_stats(data):
    return np.mean(data), np.max(data), np.min(data)

def group_and_stats(data, num_groups):
    sorted_data = sorted(data.items(), key=lambda x: x[1])
    chunk_size = math.ceil(len(sorted_data) / float(num_groups))
    print(chunk_size)
    grouped_data = [sorted_data[int(i*chunk_size):int((i+1)*chunk_size)] for i in range(num_groups)]
    print(grouped_data)
    stats = [[np.mean(group), np.max(group), np.min(group)] for group in grouped_data]
    return stats

pg_weight = {
    1: 0.8,
    2:0.9,
    3:1.3,
    4:0.3,
    5:1.4,
    6:1.1,
    # [3, 1.3],
}
group_stats = group_and_stats(pg_weight, 3)

for i, stats in enumerate(group_stats):
    print("Group {}: Mean={}, Max={}, Min={}".format(i+1, stats[0], stats[1], stats[2]))
