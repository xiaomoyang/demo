import json

class Checker(object):

    def __init__(self, source_file, res_file):

        self.source_trouble = []
        self.alarm2trouble = {}
        self.trouble2alarm = {}
        self.res = []
        self.a_valids = set([])

        for line in open(source_file):
            tr = json.loads(line.strip())
            self.source_trouble.append(tr)
            alarm_id = tr['targetName']
            self.alarm2trouble[alarm_id] = tr
            trouble_name = tr['trouble_name']
            if trouble_name in self.trouble2alarm:
                self.trouble2alarm[trouble_name].add(alarm_id)
            else:
                self.trouble2alarm[trouble_name] = set([alarm_id])

        for trouble in self.trouble2alarm:
            if len(self.trouble2alarm[trouble]) <= 1:
                continue
            self.a_valids.add((trouble, frozenset(self.trouble2alarm[trouble])))

        for line in open(res_file):
            re = json.loads(line.strip())
            self.res.append(re)

        return
    
    def check(self):
        all = len(self.res)
        pos = 0
        apos = 0
        test_a_valids = set([]) 
        for re in self.res:
            # get alarm_ids
            sl = frozenset(re['source'])
            tl = frozenset(re['target'])
            als = sl|tl
            # if alarm_ids same name
            tr_names = set([])
            for s in als:
                tr_names.add(self.alarm2trouble[s]['trouble_name'])
            if len(tr_names) != 1:
                continue
            pos += 1
            tr_name = list(tr_names)[0]
            # rank 
            alarm_id = list(als)[0]
            rank =self.alarm2trouble[alarm_id]['rank']
            if rank == len(als):
                apos += 1
                test_a_valids.add((tr_name, als)) 
        print len(self.a_valids)
        print 'pos acc:%s, allpos acc:%s, trouble recall:%s' % (pos*1.0/all, apos*1.0/all, len(test_a_valids&self.a_valids)*1.0/len(self.a_valids))

ch = Checker('test.data', 'result.data')
ch.check()