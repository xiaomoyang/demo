#encoding=utf-8
import copy
from queue import deque
import subprocess
import time
from threading import Thread, Lock
from cypher.conf import config
call_data_path=config.get('trace_config', 'trace_file')
class Node(object):
    def __init__(self, node_id):
        self.node_id = node_id
        self.pres = {} 
        self.nexts = {} 
        return
class SimpleGraph(object):
    def __init__(self):
        self.nodes = {}
        return
    def clear(self):
        self.nodes = {}
    def add_edge(self, source, target):
        if source not in self.nodes:
            self.nodes[source] = Node(source)
        if target not in self.nodes:
            self.nodes[target] = Node(target)
        if target in self.nodes[source].nexts:
            self.nodes[source].nexts[target] += 1
        else:
            self.nodes[source].nexts[target] = 1
        if source in self.nodes[target].pres:
            self.nodes[target].pres[source] += 1
        else:
            self.nodes[target].pres[source] = 1
        return

    def bfs_find(self, source, target, distance=3):
        if source not in self.nodes or target not in self.nodes:
            return False, None
        parents = {} 
        level = {}
        colored = {}
        q = deque() 
        i = 0
        q.append(source) 
        colored[source] = 1
        level[source] = 0
        while len(q) > 0:
            c = q.popleft()
            l = level[c]
            # visit
            if c == target:
                path = deque()
                p = c 
                while p != source:
                    path.appendleft(p)
                    p = parents[p] 
                path.appendleft(p)
                return True, list(path) 
            # child
            if l >= distance:
                return False, None
            for n in self.nodes[c].nexts:
                if n not in colored:
                    level[n] = l + 1
                    q.append(n) 
                    colored[n] = 1
                    parents[n] = c
        return False, None 

    def dfs_find(self, source, target, distance=3):
        if source not in self.nodes or target not in self.nodes:
            return False, None
        stack = []
        path = []
        colored = {} 
        stack.append(source)
        colored[source] = 1
        while len(stack) > 0:
            c = stack[-1]
            path.append(c)
            if self.nodes[c].node_id == target:
                #paths.append(copy.deepcopy(stack))
                return True, path
            if len(path) >= distance:
                stack.pop()
                path.pop()
            else:
                has_next = False
                for n in self.nodes[c].nexts:
                    if n not in colored:
                        has_next = True 
                        stack.append(n)
                        colored[n] = 1
                if not has_next:
                    stack.pop()
                    path.pop()
        return False, None

def load_call_data(path):
    global sg
    global sg1
    global sg2
    global l

    while True:
        tmp_sg = None
        if sg == sg1:
            tmp_sg = sg2
        else:
            tmp_sg = sg1
        tmp_sg.clear()
        f = open(path)
        for line in f:
            items = line.strip().split('\t')
            source = items[7]
            target = items[5]
            tmp_sg.add_edge(source, target)
        f.close()
        l.acquire()
        sg = tmp_sg
        l.release()
        time.sleep(60*60)
    return

def alarm_call_relation(source_alarm, target_alarm):
    global sg
    global l
    l.acquire()
    ret1, path1 = sg.bfs_find(source_alarm, target_alarm)
    l.release()
    if ret1:
        return (True, '调用', '->'.join(path1))
    l.acquire()
    ret2, path2 = sg.bfs_find(target_alarm, source_alarm)
    l.release()
    if ret2:
        return (True, '被调用', '->'.join(path2))
    return False, None, None

sg1 = SimpleGraph()
sg2 = SimpleGraph()
l = Lock()
sg = sg1
t=Thread(target=load_call_data, args=(call_data_path,))
t.start()

if __name__ == '__main__':
    sg = SimpleGraph()
    sg.add_edge('A', 'B')
    sg.add_edge('A', 'C')
    sg.add_edge('B', 'C')
    sg.add_edge('C', 'D')
    sg.add_edge('D', 'E')
    print(sg.bfs_find('A', 'D'))
    print(sg.dfs_find('A', 'D'))
    