import requests
import json

r = requests.get('http://rbi.ddns.net/getBreadCrumbData')
rj = r.json()
with open('breadcrumb_data.json', 'w') as outfile:
    json.dump(rj, outfile)
