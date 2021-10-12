#!/usr/bin/env python
# --coding:utf-8--

from http.server import BaseHTTPRequestHandler, HTTPServer
from os import path
from urllib.parse import urlparse
import json
import math
import logging
from cypher.alarm_util import *
from cypher.cmdb_util import *
from cypher.trace_util import *
from cypher.log_util import get_log_ano
import time
from cypher.conf import config

logger = logging.getLogger()
logger.addHandler(logging.FileHandler('cypher_logs'))
logger.setLevel(logging.INFO)

# config
# (t,)->t1 prob,surport
history_map = {}

def calc_willson(num_click, num_pv, z=1.96):
    p = num_click * 1.0 / num_pv
    n = num_pv
    A = p + z**2 / (2*n)
    T = p * (1-p) / n + z**2 / (4*(n**2))
    if T <= 0:
        return 0.0
    B = math.sqrt(T)
    C = z * B
    D = 1 + z**2 / n
    ctr = (A - C) / D
    return ctr

curdir = path.dirname(path.realpath(__file__))
sep = '/'

test_data = {
    "code": 0,
    "status": "success",
    "data": [
        {
            "name": "test",
            "create_time": "2020-12-21 14:00:33",
            "type": "拓扑",
            "detail": "上游出现问题",
            "url": "http://xxx/xxx/id=xx",
            "relate_score": "30%"
        }
    ]
}

class RelatedIncident(object):
    def __init__(self, incident_id, name, create_time, type, detail, url, relate_score):
        self.incident_id = incident_id
        self.name = name
        self.create_time = create_time
        self.type = type
        self.detail = detail
        self.url = url
        self.relate_score = relate_score
        return

    def to_map(self):
        return {
            "name": self.name,
            "create_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.create_time/1000)),
            "type": self.type,
            "detail": self.detail,
            "url": self.url,
            "relate_score": self.relate_score
        }


def alarm_behaviour_relation(source_alarm, target_alarm):
    global history_map
    k1 = (source_alarm.alarm_id, target_alarm.alarm_id)
    k2 = (target_alarm.alarm_id, source_alarm.alarm_id)
    chosen = 0
    willson1 = 0
    willson2 = 0
    if k1 in history_map:
        support1, prob1 = history_map[k1]
        willson1 = calc_willson(support1, int(support1/prob1))
        chosen = 1
    if k2 in history_map:
        support2, prob2 = history_map[k2]
        willson2 = calc_willson(support2, int(support2/prob2))
        if willson2 > willson1:
            chosen = 2
    if chosen == 1:
        return (True, support1, prob1)
    elif chosen == 2:
        return (True, support2, prob2)
    else:
        return (False, None, None)

def alarm_ci_relation(source_alarm, target_alarm):
    s_name = None
    t_name = None
    relation = None
    source_ci_ids = source_alarm.ci_ids
    target_ci_ids = target_alarm.ci_ids
    # TODO relation privity
    for source_ci_id in source_ci_ids:
        for target_ci_id in target_ci_ids:
            re = check_cmdb_level(source_ci_id, target_ci_id)
            if re:
                s_name = re[0]
                t_name = re[1]
                relations = re[2]
                # TODO 多个关系
                if len(relations) > 0:
                    relation = relations[0]['relationship']
                if source_ci_id == target_ci_id:
                    relation = 'same ci {}'.format(source_ci_id)
                if relation and relation != 'null':
                    break
        if relation and relation != 'null':
            break
    if relation and relation != 'null':
        return True, s_name, t_name, relation
    return False, s_name, t_name, relation

def get_related_incidents(get_params):
    related_incidents = []
    interval = 5 * 60 * 1000
    incident_id = get_params['incident_id']
    incident = get_incident_by_id(incident_id)
    logger.info("incident:{} created_time:{}, alarms:{}".format(incident_id, incident.created_time,
                                                                len(incident.alarms)))
    for alarm in incident.alarms:
        # 获取警报的事件发生时间
        msg = get_first_msg_by_alarm_id(alarm.alarm_id, incident.created_time)
        if not msg:
            logger.error("alarm_id: {}, after create_time{} no msg".format(
                alarm.alarm_id, incident.created_time))
            continue
        logger.info("alarm_id: {} msg.timestamp {}".format(
            alarm.alarm_id, msg.timestamp))
        # 时间召回相关警报
        time_alarms = get_alarms_by_time(
            start=(msg.timestamp - interval), end=(msg.timestamp + interval))
        logger.info('alarm:{}, time recall alarms {}'.format(
            alarm.alarm_id, len(time_alarms)))
        # 相关类型判别
        for time_alarm in time_alarms:
            types = []
            details = []
            scores = []
            ret, support, prob = alarm_behaviour_relation(alarm, time_alarm)
            if ret:
                types.append("共现")
                details.append('警报行为相关, 置信度:{},相关概率:{}'.format(
                    alarm.alarm_id, time_alarm.alarm_id, support, prob))
                scores.append(prob)
            ret, s_name, t_name, relation = alarm_ci_relation(
                alarm, time_alarm)
            if ret:
                types.append("拓扑")
                details.append('警报ci拓扑相关, source_model_name:{},target_model_name:{}, relation:{}'
                               .format(s_name, t_name, relation))
                scores.append(0.5)
            ret, type, relation = alarm_call_relation(
                alarm.targetname, time_alarm.targetname)
            if ret:
                types.append("调用")
                details.append(
                    '警报调用链相关, type:{}, relation:{}'.format(type, relation))
                scores.append(0.5)
            # 不相关
            if len(types) == 0:
                continue
            score = 0
            for s in scores:
                score += s
            rincident = get_incident_by_alarm_id(time_alarm.alarm_id)
            if not rincident:
                logger.error("alarm_id {} not found incident".format(
                    time_alarm.alarm_id))
                continue
            url = incident_url.format(rincident.incident_id)
            related_incidents.append(RelatedIncident(rincident.incident_id, rincident.name, rincident.created_time, '\n'.join(types),
                                                     '\n'.join(details), url, score))
    # 历史相关
    '''
    history_incidents = get_incidents_by_name(incident.name)
    for rincident in history_incidents:
        url = incident_url.format(rincident.id)
        related_incidents.append(Related_Incident(rincident.name, rincident.created_time, '历史', 
            '之前同样报警事件', url, '0.5'))
    '''
    ret_data = {
        "code": 0,
        "status": "success",
        "data": []
    }
    # filter
    fs = set()
    for ri in related_incidents:
        if ri.incident_id == incident_id:
            continue
        if ri.incident_id in fs:
            continue
        fs.add(ri.incident_id)
        ret_data["data"].append(ri.to_map())
    return json.dumps(ret_data)

def get_related_logs(get_params):
    related_log_anos = []
    interval = 5 * 60 * 1000
    incident_id = get_params['incident_id']
    incident = get_incident_by_id(incident_id)
    logger.info("related log incident:{} created_time:{}, alarms:{}".format(incident_id, incident.created_time,
                                                                len(incident.alarms)))
    log_anos = get_log_ano(incident.created_time-interval, incident.created_time+interval)
    ret_data = {
        "code": 0,
        "status": "success",
        "data": []
    }
    if not log_anos:
        return json.dump(ret_data)
    for log_ano in log_anos:
        ret_data['data'].append(log_ano.to_map())
    return json.dumps(ret_data)

def get_related_metrics(get_params):
    return json.dumps(test_data)

# TODO
def get_related_onlines(get_params):
    return json.dumps(test_data)


class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):
    # GET
    def do_GET(self):
        api_map = {
            "/get_related_incidents": get_related_incidents,
            "/get_related_logs": get_related_logs,
            "/get_related_metrics": get_related_metrics,
            "/get_related_onlines": get_related_onlines
        }
        querypath = urlparse(self.path)
        path, query = querypath.path, querypath.query
        get_params = {}
        items = query.split('&')
        for item in items:
            k, v = item.split('=')
            get_params[k] = v
        if path in api_map:
            content = api_map[path](get_params).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_error(404, 'File Not Found: %s' % self.path)

def load_event_file(path):
    global history_map
    history_map.clear()
    for line in open(path):
        depend = json.loads(line.strip())
        pair = (','.join(depend['source']), ','.join(depend['target']))
        if pair not in history_map:
            history_map[pair] = (depend['source_support'], depend['conf'])
    return

def run():
    load_event_file('result.data')
    port = 5200
    print('starting server, port', port)
    # Server settings
    server_address = ('10.0.12.208', port)
    httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
    print('running server...')
    httpd.serve_forever()

if __name__ == '__main__':
    run()
