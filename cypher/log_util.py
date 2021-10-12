# encoding=utf-8
import copy
from queue import deque
import subprocess
import time
from cypher.conf import config
import json
import requests
ck_http = config.get('dodb_config', 'ck_http')
user = config.get('dodb_config', 'ck_user')
password = config.get('dodb_config', 'ck_password')
docp_url = config.get('docp_config', 'front_url')


class LogAno(object):
    def __init__(self, start, end, pattern, pattern_id, log_num, ano_type, ano_des, ano_value, rule_id, group_field, source_ids):
        global docp_url
        self.start = int(start)
        self.end = int(end)
        self.pattern = pattern
        self.pattern_id = pattern_id
        self.log_num = log_num
        self.ano_type = ano_type
        self.ano_des = ano_des
        self.ano_value = ano_value
        self.rule_id = rule_id
        self.group_field = json.loads(group_field)['_cw_biz']
        self.source_ids = source_ids
        self.url = docp_url + '/#/dola/logMonitor/patternAnalysis?pattern={}&groupFieldValue={}&ruleId={}&anomalyType={}&beginTime={}&endTime={}'.format(
            self.pattern_id,self.group_field,self.rule_id,self.ano_type,self.start,self.end
        )

    def to_map(self):
        m = {'4': '历史新增'} 
        return {
            "name": self.pattern,
            "create_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.start/1000)),
            "type": '时间相关',
            "detail": '发生了{}， {}'.format(m.get(self.ano_type, 'unknown类型:{}'.format(self.ano_type)), self.ano_des),
            "url": self.url,
            "relate_score": 0.5 
        }

def get_log_ano(start, end):
    global ck_http
    global user
    global password
    url = ck_http
    rets = []
    sql = 'select begin_time,end_time,pattern,pattern_id ,count,anomaly_type,anomaly_description,anomaly_value,rule_id,group_field,source_ids from cw_db.log_anomaly_detection_pid where anomaly_type !=0  and begin_time>= {} and  begin_time  < {};'.format(
        start, end)
    try:
        re = requests.post(url, auth=(user, password), data=sql)
        for line in re.content.decode('utf-8').split('\n'):
            items = line.strip().split('\t')
            if len(items) != 11:
                continue
            la = LogAno(*items)
            rets.append(la)
    except Exception as e:
        print(e)
    return rets

if __name__ == '__main__':
    get_log_ano(1625322255000, 1633962255000)
