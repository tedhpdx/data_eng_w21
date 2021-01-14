import requests
import json

#json_file_path = '/home/herring/data_eng_w21/project-code/breadcrumb_data.json'
json_file_path = 'breadcrumb_data.json'

r = requests.get('http://rbi.ddns.net/getBreadCrumbData')
rj = r.json()
first = rj[0]
second = first['EVENT_NO_TRIP']
with open(json_file_path, 'w') as outfile:
        json.dump(rj, outfile)



