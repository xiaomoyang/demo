import sys
import json
from cypher import alarm_util 
for msg in alarm_util.get_all_docs(cols=['md5value', 'timestamp', 'targetName']):
    print(json.dumps(msg))