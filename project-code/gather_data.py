import requests
import json

r = requests.get('http://rbi.ddns.net/getBreadCrumbData')
rj = r.json()
with open('/home/herring/data_eng_w21/project-code/breadcrumb_data.json', 'w') as outfile:
    json.dump(rj, outfile)
