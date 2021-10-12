import sys
import json
res = []
for line in sys.stdin:
    ts = json.loads(line.strip())['timestamp']
    res.append((ts, line.strip()))
res_s = sorted(res, key=lambda x: x[0])
for r in res_s:
    print r[1]

    
    