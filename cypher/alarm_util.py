# encoding=utf-8
import requests
import json
from cypher.conf import config

es_http = config.get('incident_config', 'es_http')
incident_url = config.get('incident_config', 'incident_url')

class Msg(object):
    def __init__(self, msg_id):
        self.msg_id = msg_id
        return


class Alarm(object):
    def __init__(self, alarm_id):
        self.alarm_id = alarm_id
        return


class Incident(object):
    def __init__(self, incident_id):
        self.incident_id = incident_id
        return


def get_incident_by_id(incident_id):
    incident = Incident(incident_id)
    url = es_http + '/artemis_incident/_search'
    data = {
        "query":
        {
            "term": {
                "id.keyword": incident_id
            }
        },
        "_source": ["createdTime"]
    }
    re = requests.post(url, json=data)
    rets = json.loads(re.content)
    for hit in rets['hits']['hits']:
        incident.created_time = hit['_source']['createdTime']
        break
    url = es_http + '/artemis_incident/_search'
    data = {
        "query": {
            "bool": {
                "must": [
                    {
                        "has_parent": {
                            "parent_type": "incidentId",
                            "query": {
                                "term": {
                                    "id.keyword": incident_id
                                }
                            }
                        }
                    },
                    {
                        "exists": {
                            "field": "messageGroup"
                        }
                    }
                ]
            }
        },
        "_source": ["messageGroup.groupIdentity", "messageGroup.group.targetname", "ciIdList", "description"]
    }
    re = requests.post(url, json=data)
    rets = json.loads(re.content)
    alarms = []
    for hit in rets['hits']['hits']:
        alarm = Alarm(hit['_source']['messageGroup']['groupIdentity'])
        alarm.targetname = hit['_source']['messageGroup']['group']['targetname']
        if 'ciIdList' in hit['_source']:
            alarm.ci_ids = hit['_source']['ciIdList']
        else:
            alarm.ci_ids = []
        if 'description' in hit['_source']:
            alarm.description = hit['_source']['description']
        else:
            alarm.description = ''
        alarms.append(alarm)
    incident.alarms = alarms
    return incident


def get_first_msg_by_alarm_id(alarm_id, begin_time):
    msg = None
    url = es_http + '/artemis_alert_msg/_search'
    data = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "md5value": alarm_id
                        }
                    },
                    {
                        "range": {
                            "timestamp": {
                                "gte": begin_time
                            }
                        }
                    }
                ]
            }
        },
        "_source": ["timestamp", "level", "md5value", "id"],
        "sort": [{"timestamp": 'asc'}],
        "size": 1000
    }
    re = requests.post(url, json=data)
    rets = json.loads(re.content)
    for hit in rets['hits']['hits']:
        msg = Msg(hit['_source']['id'])
        msg.timestamp = hit['_source']['timestamp']
        break
    return msg


def get_alarms_by_time(start, end):
    url = es_http + '/artemis_alert_msg/_search'
    data = {
        'size': 1000,
        'query': {
            "range": {
                "timestamp": {
                    "gte": start,
                    "lte": end
                }
            }
        }
    }
    alarms = []
    re = requests.post(url, json=data)
    rets = json.loads(re.content)
    s = set()
    for hit in rets['hits']['hits']:
        alarm = Alarm(hit['_source']['md5value'])
        alarm.ci_ids = hit['_source']['ciIdList']
        alarm.description = hit['_source']['description']
        alarm.targetname = hit['_source']['targetname']
        if alarm.alarm_id in s:
            continue
        alarms.append(alarm)
        s.add(alarm.alarm_id)
    return alarms


def get_incident_by_alarm_id(alarm_id):
    incident = None
    url = es_http + '/artemis_incident/_search'
    data = {
        "query": {
            "term": {
                "messageGroup.groupIdentity.keyword": alarm_id
            }
        },
        "_source": ["incidentId"]
    }
    re = requests.post(url, json=data)
    rets = json.loads(re.content)
    for hit in rets['hits']['hits']:
        tmp_incident = Incident(hit['_source']['incidentId'])
        data = {
            "query": {
                "term": {
                    "id.keyword": tmp_incident.incident_id
                }
            }
        }
        for hit2 in json.loads(requests.post(url, json=data).content)['hits']['hits']:
            if hit2['_source']['processStatus'].upper() != 'CLOSED':
                tmp_incident.created_time = hit2['_source']['createdTime']
                tmp_incident.name = hit2['_source']['incidentNameText']
                incident = tmp_incident
                break

    return incident


def get_incidents_by_name(name):
    incidents = []
    url = es_http + '/artemis_incident/_search'
    data = {
        'query': {
            'term': {
                'incidentNameText': name
            }
        }
    }
    re = requests.post(url, json=data)
    rets = json.loads(re.content)
    for hit in rets['hits']['hits']:
        incident = Incident(hit['_source']['id'])
        incident.create_time = hit['_source']['createdTime']
        incident.name = hit['_source']['incidentNameText']
        incidents.append(incident)
    return incidents


def get_all_docs(index='artemis_alert_msg', cache_time=1, cols=[]):
    url = es_http + '/{}/_search?scroll={}m'.format(index, cache_time)
    data = {
        'size': 1000
    }
    if len(cols) > 0:
        data['_source'] = cols
    scroll_id = ''
    while True:
        re = requests.post(url, json=data)
        rets = json.loads(re.content)
        scroll_id = rets['_scroll_id']
        if len(rets['hits']['hits']) == 0:
            break
        for hit in rets['hits']['hits']:
            yield hit['_source']
        url = es_http + '/_search/scroll'
        data = {
            "scroll_id": scroll_id,
            "scroll": "{}m".format(cache_time)
        }
    return


if __name__ == "__main__":
    incident_id = '9276a531-58e3-4359-a3dd-1e7fcf3e3fde'
    print(get_incident_by_id(incident_id))
