import pandas as pd



csv = pd.read_csv(r'crashes.csv')
v = csv.values
c = []
for i in range(len(v)):
    #v[i][0] gives us our crash IDs
    if v[i][0] not in c:
        c.append(v[i][0])
print (c)




