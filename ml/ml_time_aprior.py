"""
# Python 2.7
"""

import sys
import json
import argparse

class TimeDataSeries(object):

    def __init__(self, interval=600):
        self.interval = interval
        self.l_data = []
        self.I = {}
        return

    def append(self, data):
        self.l_data.append(data)
        return
    
    def bi_search(self, ts):
        s, e = 0, len(self.l_data) - 1
        while s <= e:
            mid = (s + e) / 2
            vmid = self.l_data[mid]
            if vmid == ts:
                return vmid
            elif vmid < ts:
                s = vmid + 1
            else:
                e = vmid - 1
        if e < 0:
            return 0
        return e

    def search(self, st, et, alarm_id):
        if (et - st) > self.interval or et < st:
            raise Exception('error st: %s, et: %s' % (st, et))
        t_st = et - self.interval
        t_et = st + self.interval
        i_st = self.bi_search(t_st)
        i_et = self.bi_search(t_et+1)
        for i in range(i_st, i_et):
            if self.l_data[i]['md5value'] == alarm_id:
                return (min(i, st), max(i, et))
        return None

def load_time_data_set(path):
    time_data_set = TimeDataSeries()
    #for line in sys.stdin:
    for line in open(path):
        j = json.loads(line.strip())
        time_data_set.append(j)
    return time_data_set

def create_C1(data_set):
    """
    Create frequent candidate 1-itemset C1 by scaning data set.
    Args:
        data_set: A list of transactions. Each transaction contains several items.
    Returns:
        C1: A set which contains all frequent candidate 1-itemsets
    """
    C1 = set()
    for i in range(len(data_set.l_data)):
        t = data_set.l_data[i]
        alarm_id = t['md5value']
        ts = t['timestamp']
        item_set = frozenset([alarm_id])
        C1.add(item_set)
        index = data_set.I.get(item_set, [])
        index.append((ts,ts))
        data_set.I[item_set] = index
    return C1

def is_apriori(data_set, ck, c1):
    """
    Judge whether a frequent candidate k-itemset satisfy Apriori property.
    Args:
        Ck_item: a frequent candidate k-itemset in Ck which contains all frequent
                 candidate k-itemsets.
        Lksub1: Lk-1, a set which contains all frequent candidate (k-1)-itemsets.
    Returns:
        True: satisfying Apriori property.
        False: Not satisfying Apriori property.
    """
    ret = False
    index = data_set.I.get(ck|c1, [])
    for (p1, p2) in data_set.I[c1]:
        p = p1
        for (st, et) in data_set.I[ck]: 
            if p >= st and p <= et:
                index.append((st, et))
                ret = True
            elif p < st and (et - p) <= data_set.interval:
                index.append((p, et))
                ret = True
            elif p > et and (p - st) <= data_set.interval:
                index.append((st, p))
                ret = True
    if len(index) > 0:
        data_set.I[ck|c1] = index
    return ret 

def create_Ck(data_set, Lksub1, k):
    """
    Create Ck, a set which contains all all frequent candidate k-itemsets
    by Lk-1's own connection operation.
    Args:
        Lksub1: Lk-1, a set which contains all frequent candidate (k-1)-itemsets.
        k: the item number of a frequent itemset.
    Return:
        Ck: a set which contains all all frequent candidate k-itemsets.
    """
    Ck = set()
    len_Lksub1 = len(Lksub1)
    list_Lksub1 = list(Lksub1)
    for i in range(len_Lksub1):
        for j in range(i+1, len_Lksub1):
            s1 = list_Lksub1[i]
            s2 = list_Lksub1[j]
            l1 = list(s1)
            l2 = list(s2)
            l1.sort()
            l2.sort()
            if l1[0:k-2] == l2[0:k-2]:
                # merge
                Ck_item = s1 | s2 
                if len(data_set.I[s1]) < len(data_set.I[s2]):
                    if is_apriori(data_set, s1, Ck_item-s1):
                        Ck.add(Ck_item)
                else:
                    if is_apriori(data_set, s2, Ck_item-s2):
                        Ck.add(Ck_item)
    return Ck

def generate_Lk_by_Ck(data_set, Ck, min_support, support_data):
    """
    Generate Lk by executing a delete policy from Ck.
    Args:
        data_set: A list of transactions. Each transaction contains several items.
        Ck: A set which contains all all frequent candidate k-itemsets.
        min_support: The minimum support.
        support_data: A dictionary. The key is frequent itemset and the value is support.
    Returns:
        Lk: A set which contains all all frequent k-itemsets.
    """
    Lk = set()
    for ck in Ck:
        support = len(data_set.I[ck])
        if support >= min_support:
            Lk.add(ck)
            support_data[ck] = support
    return Lk
    
def generate_L(data_set, k, min_support):
    """
    Generate all frequent itemsets.
    Args:
        data_set: A list of transactions. Each transaction contains several items.
        k: Maximum number of items for all frequent itemsets.
        min_support: The minimum support.
    Returns:
        L: The list of Lk.
        support_data: A dictionary. The key is frequent itemset and the value is support.
    """
    support_data = {}
    C1 = create_C1(data_set)
    print('generate c1:%s' % len(C1))
    L1 = generate_Lk_by_Ck(data_set, C1, min_support, support_data)
    print('generate l1:%s' % len(L1))
    Lksub1 = L1.copy()
    L = []
    L.append(Lksub1)
    for i in range(2, k+1):
        Ci = create_Ck(data_set, Lksub1, i)
        print('generate c%s:%s' % (i, len(Ci)))
        Li = generate_Lk_by_Ck(data_set, Ci, min_support, support_data)
        print('generate l%s:%s' % (i, len(Li)))
        Lksub1 = Li.copy()
        L.append(Lksub1)
    return L, support_data

def generate_big_rules(L, support_data, min_conf):
    """
    Generate big rules from frequent itemsets.
    Args:
        L: The list of Lk.
        support_data: A dictionary. The key is frequent itemset and the value is support.
        min_conf: Minimal confidence.
    Returns:
        big_rule_list: A list which contains all big rules. Each big rule is represented
                       as a 3-tuple.
    """
    big_rule_list = []
    sub_set_list = []
    for i in range(0, len(L)):
        for freq_set in L[i]:
            for sub_set in sub_set_list:
                if sub_set.issubset(freq_set):
                    conf = support_data[freq_set] * 1.0 / support_data[sub_set]
                    if conf > 1:
                        print(support_data[sub_set], support_data[freq_set], sub_set, freq_set)
                    big_rule = (sub_set, freq_set - sub_set, conf, support_data[sub_set], support_data[freq_set])
                    if conf >= min_conf and big_rule not in big_rule_list:
                        # print freq_set-sub_set, " => ", sub_set, "conf: ", conf
                        big_rule_list.append(big_rule)
            sub_set_list.append(freq_set)
    return big_rule_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ml_time_aprior args.')
    parser.add_argument('--input_file', default='train_alarm.data')
    parser.add_argument('--rank', type=int, default=2)
    parser.add_argument('--min_support', type=int, default=3)
    parser.add_argument('--output_file', default='alarm.data')
    args = parser.parse_args(sys.argv[1:])
    data_set = load_time_data_set(args.input_file)
    L, support_data = generate_L(data_set, k=args.rank, min_support=args.min_support)
    big_rules_list = generate_big_rules(L, support_data, min_conf=0.8)
    for Lk in L:
        print("="*50)
        print("frequent " + str(len(list(Lk)[0])) + "-itemsets\t\tsupport")
        print("="*50)
        for freq_set in Lk:
            print(freq_set, support_data[freq_set])
    f = open(args.output_file, 'w+')
    for item in big_rules_list:
        f.write(json.dumps({"source":list(item[0]), "target":list(item[1]), "conf": item[2], "source_support": item[3], "target_support": item[4]}) + '\n')
        # print item[0], "=>", item[1], "conf: ", item[2], " = ", item[4], "/", item[3]
    f.flush()
    f.close()
