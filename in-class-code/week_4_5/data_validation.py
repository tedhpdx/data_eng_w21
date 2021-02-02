import pandas as pd

'''
class Crash:
    def __init__(self, crash_id, record_type):
        self.crash_id = crash_id
        self.record_type = record_type
        self.contained_data = []


csv = pd.read_csv(r'crashes.csv')
v = csv.values
d = []

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
        

class record:
    def __init__(self, crash_id, record_type):
        self.crash_id = crash_id
        self.record_type = record_type
        self.contained_data = []

type1 = []
type2 = []
type3 = []
for i in range(len(v)):
    record_type = v[i][1]
    if record_type is 1:
        type1.append(v[i])
        pass
    elif record_type is 2:
        type2.append(v[i])
        pass
    elif record_type is 3:
        type3.append(v[i])
        pass
#clear nan indicies (1,6) from type 1
for i in range(len(type1)):
    d = []
    d = type1[i].tolist()
    for j in range(6):
        d.pop(1)
    type1[i] = d

for i in range(len(type1)):
    #d = []
    #d = type1[i].tolist()
    for j in range(55):
        type1[i].pop(96)
    type1[i] = d

for i in range(len(type1)):
    #assert month is in range
    if type1[i][3] > 12:
        print("month out of range")
    #assert day is in range
    if type1[i][4] > 31:
        print("day out of range")

pass
'''

#ok so after lots of struggling I see that I need to stay within pandas,
#which I realize now I wasn't even using...I think through so many different
#attempts I ended up trying to reinvent the wheel

df = pd.read_csv('crashes.csv')
crashes = df[df['Record Type'] == 1]
vehicle = df[df['Record Type'] == 2]
participants = df[df['Record Type'] == 3]
df = []

crashes = crashes.dropna(axis=1, how='all')
vehicle = vehicle.dropna(axis=1, how='all')
participants = participants.dropna(axis=1, how='all')

for i in crashes:
    if i == 'Crash Hour':
        for j in crashes[i]:
            if j > 23:
                print ("hour error")
                print(j)



