# encoding=utf8
import json
import sys
from trouble import TroubleGenerator

import random
import uuid
import datetime
import time
import logging

logger = logging.getLogger()
logger.addHandler(logging.FileHandler('logs'))
logger.setLevel(logging.INFO)


class TrainAlertDataJob():

    def __init__(self, start_time, end_time, troubles, random_alarm_id=False, random_alarm_num=1000,
                 shared_alarm_toubles=None, conflict_time_num=10, conflict_trouble_num=10):
        self.start_time = start_time
        self.end_time = end_time
        self.troubles = troubles
        self.random_alarm_id = random_alarm_id
        self.random_alarm_num = random_alarm_num
        self.random_alarm_ids = []
        if self.random_alarm_id and self.random_alarm_num > 0:
            for i in range(random_alarm_num):
                self.random_alarm_ids.append(str(uuid.uuid1()))
        self.shared_alarm_toubles = shared_alarm_toubles
        self.trouble_map = {}
        self.period_troubles = []
        self.non_period_troubles = []
        for trouble in self.troubles:
            self.trouble_map[trouble.name] = trouble
            if trouble.is_period:
                self.period_troubles.append(trouble)
            else:
                self.non_period_troubles.append(trouble)
        self.conflict_time_num = conflict_time_num
        self.conflict_trouble_num = conflict_trouble_num
        return

    def arrange_alarm_id(self):
        if self.random_alarm_id:
            for trouble in self.troubles:
                alarm_ids = random.sample(self.random_alarm_ids, trouble.rank)
                trouble.set_alarm_ids(alarm_ids)
        elif self.shared_alarm_toubles:
            for tuple in self.shared_alarm_toubles:
                tn1 = tuple[0]
                tn2 = tuple[1]
                k = tuple[2]
                if tn1 not in self.trouble_map or tn2 not in self.trouble_map:
                    continue
                t1 = self.trouble_map[tn1]
                t2 = self.trouble_map[tn2]
                if t1.left_rank() <= k or t2.left_rank() <= k:
                    continue
                for i in range(k):
                    alarm_id = str(uuid.uuid1())
                    t1.alarm_ids.append(alarm_id)
                    t2.alarm_ids.append(alarm_id)
                    continue
            for trouble in self.troubles:
                for i in range(trouble.left_rank()):
                    alarm_id = str(uuid.uuid1())
                    trouble.append(alarm_id)
        return

    def start(self):
        self.arrange_alarm_id()
        # peroid trouble
        for trouble in self.period_troubles:
            trouble.trigger_all_period()
        # trigger conflict trouble
        for i in range(self.conflict_trouble_num):
            trigger_time = random.randint(self.start_time, self.end_time)
            for j in range(random.randint(2, self.conflict_trouble_num)):
                trouble = random.choice(self.non_period_troubles)
                trouble.trigger_one_with_time(trigger_time)
        # trigger left trouble
        for trouble in self.non_period_troubles:
            tss = []
            for i in range(trouble.left_num()):
                tss.append(random.randint(self.start_time, self.end_time))
            trouble.trigger_all_with_time(tss)
        return


def date2ts(date_str, format):
    ts = time.mktime(datetime.datetime.strptime(date_str, format).timetuple())
    return ts


if __name__ == '__main__':
    job_file = open(sys.argv[1])
    content = job_file.read()
    job_config = json.loads(content.replace('\n', ''))
    job_file.close()
    st = time.mktime(datetime.datetime.strptime(
        job_config['start_time'], "%Y/%m/%d").timetuple())
    et = time.mktime(datetime.datetime.strptime(
        job_config['end_time'], "%Y/%m/%d").timetuple())
    out = job_config['out']
    if out == 'stdout':
        out = sys.stdout
    else:
        out = open(out, 'w+')
    random_alarm_id = job_config.get('random_alarm_id', None)
    random_alarm_num = job_config.get('random_alarm_num', None)
    conflict_time_num = job_config.get('conflict_time_num', 0)
    conflict_trouble_num = job_config.get('conflict_trouble_num', 0)
    trs = job_config['troubles']
    troubles = []
    for tr in trs:
        name = tr.pop('name')
        num = tr.pop('num')
        rank = tr.pop('rank')
        range_start = tr.pop('range_start', None)
        if range_start:
            its = range_start.split('-')
            range_start = (
                date2ts(its[0], '%Y/%m/%d %H:%M:%S'), date2ts(its[1], '%Y/%m/%d %H:%M:%S'))
        repeat = tr.pop('repeat')
        for i in range(repeat):
            tg = TroubleGenerator(name+'%s' % i, num, rank, **tr)
            troubles.append(tg)
    logger.info('trouble_num: %d', len(troubles))
    # parse job_config to job
    j = TrainAlertDataJob(st, et, troubles, random_alarm_id=random_alarm_id, random_alarm_num=random_alarm_num,
                          conflict_time_num=conflict_time_num, conflict_trouble_num=conflict_trouble_num)
    j.start()
    out.flush()
    out.close()
