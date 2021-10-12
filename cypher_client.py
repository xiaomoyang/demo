import requests
incident_id = '0cb53adb-2645-4e4e-93a2-ccd48278c8c2'
url = 'http://127.0.0.1:5200/get_related_incidents?incident_id={}'.format(incident_id)
re = requests.get(url)
print(re.content)
