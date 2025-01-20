import json
with open('osd_used_capacity.json', 'r') as f:
    osd_used_capacity = json.load(f)
    print(osd_used_capacity)