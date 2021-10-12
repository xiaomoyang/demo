# encoding=utf8
# 事件发生器, 基本属性
# 1. rank=2 该事件发生的话，会发送几个警报
# 2. time_range=60*10 该事件发生，警报的持续时间
# 3. is_seq_pro=0 该事件发生时，警报发生是否保证顺序的概率
# 4. prob=0.01 该事件发生的概率
# 5. num=3 该事件发生的次数
# 6. 
import random
import uuid
import sys
import json
import logging
import datetime
logger=logging.getLogger()

class TroubleGenerator(object):

    def __init__(self, name, num, rank, out=sys.stdout, range_start=None, period_len=None,
                range_len=10*60, order_seq_num=0, dup_alarm_msg=0):
        self.name = name
        self.num = num
        self.triggered_num = 0
        self.rank = rank
        self.range_start = range_start
        self.is_period = range_start is not None
        self.preriod_len = period_len
        self.range_len = range_len
        self.order_seq_num = min(order_seq_num, rank)
        self.dup_alarm_msg = dup_alarm_msg
        self.uuid = None
        self.alarm_ids = []
        self.msgs = []
        self.out = out
        return

    def left_num(self):
        return self.num - self.triggered_num

    def left_rank(self):
        return self.rank - len(self.alarm_ids)

    def set_alarm_ids(self, alarm_ids):
        self.alarm_ids = alarm_ids
        if self.order_seq_num > 0:
            random.shuffle(self.alarm_ids)
        return

    def trigger_msg(self, alarm_id, ts):
        dt = datetime.datetime.fromtimestamp(ts)
        #trouble_name = (self.name+':%s') % self.triggered_num 
        trouble_name = self.name 
        msg = {'timestamp': ts, 'alarm_id': alarm_id, 'trouble_id': self.uuid, 
               'trouble_name': trouble_name, 'targetName': alarm_id,
               'date': dt.strftime('%Y/%m/%d %H:%M:%S'), 'rank': self.rank}
        msg_str = json.dumps(msg)
        # self.msgs.append({'timestamp': ts, 'alarm_id': alarm_id, 'trouble_id': self.uuid, 'trouble_name': self.name})
        self.out.write(msg_str + '\n')
        return

    def generate_seq(self, st, et):
        for alarm_id in self.alarm_ids:
            if self.dup_alarm_msg is None or self.dup_alarm_msg == 0:
                ts = random.randint(st, et)
                self.trigger_msg(alarm_id, ts)
            else:
                for i in range(self.dup_alarm_msg):
                    ts = random.randint(st, et)
                    self.trigger_msg(alarm_id, ts)
        return
    
    # 在时间周期内生成
    def generate_order_seq(self, st, et):
        tss = []
        for i in range(len(self.alarm_ids)):
            tss.append(random.randint(st, et))
        tss = sorted(tss)
        ordered_alarm_ids = self.alarm_ids[:self.order_seq_num]
        for alarm_id in self.alarm_ids[self.order_seq_num:]:
            pos = random.randint(0, len(ordered_alarm_ids))
            ordered_alarm_ids.insert(pos, alarm_id)
        for i in range(len(tss)):
            self.trigger_msg(ordered_alarm_ids[i], tss[i])
        return

    def trigger_one_period(self):
        if self.range_start is None or len(self.range_start) != 2:
            raise Exception('error: %s no range_start' % self.name)
        if self.left_num() <= 0:
            return
        self.uuid = str(uuid.uuid1())
        st = random.randint(self.range_start[0], self.range_start[1])
        et = st + self.range_len
        if self.order_seq_num == 0:
            self.generate_seq(st, et)
        else:
            self.generate_order_seq(st, et)
        self.triggered_num += 1
        return
    
    def trigger_all_period(self):
        if not self.is_period:
            return
        for i in range(self.left_num()):
            self.trigger_one_period()
            self.range_start[0] += self.preriod_len
            self.range_start[1] += self.preriod_len
        return
    
    def trigger_one_with_time(self, st):
        if self.left_num() <= 0:
            return
        self.uuid = str(uuid.uuid1())
        et = st + self.range_len
        if self.order_seq_num == 0:
            self.generate_seq(st, et)
        else:
            self.generate_order_seq(st, et)
        self.triggered_num += 1
        return

    def trigger_all_with_time(self, sts):
        if len(sts) < self.left_num():
            raise Exception('error: trigger_all ts num < left_num')
        sts = sts[:self.left_num()]
        for st in sts:
            self.trigger_one_with_time(st)