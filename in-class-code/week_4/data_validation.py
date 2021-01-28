import pandas as pd


class Crash:
    def __init__(self, crash_id, record_type):
        self.crash_id = crash_id
        self.record_type = record_type
        self.contained_data = []


csv = pd.read_csv(r'crashes.csv')
v = csv.values
d = []
'''
x = dict()
for i in range(len(v)):
    #v[i][0] gives us our crash IDs
    if v[i][0] not in d:
        d.append(v[i][0])
        p = Crash(v[i][0], v[i][1])
        p.contained_data.append(v[i][2:])
        x[p.crash_id] = p
    else:
        x[v[i][0]].contained_data.append(v[i][1:])

for id in x:
    if (x[id].record_type) is 0:
        
'''
class record:
    def __init__(self, crash_id, record_type):
        self.crash_id = crash_id
        self.record_type = record_type
        self.contained_data = []


for i in range(len(v)):
    record_type = v[i][1]
    if record_type is 1:
        pass
    elif record_type is 2:
        pass
    elif record_type is 3:
        pass




