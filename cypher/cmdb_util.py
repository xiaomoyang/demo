# encoding=utf8
import requests
import json
from cypher.conf import config 
cmdb_service = config.get('cmdb_config', 'cmdb_service')
def check_cmdb_level(s_id, t_id):
    url = cmdb_service + '/api/v1/cmdb/ci/relation'
    data = {
        "fromCiId": s_id,
        "toCiId": t_id
    }
    try:
        re = requests.post(url, params=data)
        rets = json.loads(re.content)
        s_name = rets['data']['fromCi']['ciName']
        s_model = rets['data']['fromCi']['modelId']
        t_name = rets['data']['toCi']['ciName']
        t_model = rets['data']['toCi']['modelId']
        relation = rets['data']['relations']
        return ('{}:{}'.format(s_model, s_name), '{}:{}'.format(t_model, t_name), relation) 
    except Exception as e:
        print(e)
    return None 
def check_same_level(s_id, t_id):
    return True
def check_downer_level(s_id, t_id):
    return True
def check_upper_level(s_id, t_id):
    return True
if __name__ == '__main__':
    print(check_cmdb_level('5B3B0B1B10EE4F3887C0A8FEF660155F', 'D43D71C7C8D942808A601D2C143D211D'))
    print(check_cmdb_level('4EBF6B2E1470468B8BEE556AFF29BA3F', '2D474D9FACAA4D559D8945BAE6FB298B'))