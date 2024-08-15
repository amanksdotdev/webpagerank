"""
Extract pagerank data into a JSON file
"""

import sqlite3
import json

from constants import DATABASE_NAME

conn = sqlite3.connect(DATABASE_NAME)
cur = conn.cursor()

print('Creating JSON output on spider.js...')
num = int(input("How many nodes: "))

cur.execute('''SELECT COUNT(from_id) AS inbound, old_rank, new_rank, id, url 
    FROM Pages JOIN Links ON Pages.id = Links.to_id
    WHERE html IS NOT NULL AND ERROR IS NULL
    GROUP BY id ORDER BY id,inbound''')

nodes = list()
maxrank = None
minrank = None
for row in cur:
    nodes.append(row)
    rank = row[2]
    if maxrank is None or maxrank < rank: maxrank = rank
    if minrank is None or minrank > rank: minrank = rank
    if len(nodes) > num: break

if maxrank == minrank or maxrank is None or minrank is None:
    print("Error - please run sprank.py to compute page rank")
    quit()

# construct dict to convert into json
count = 0
map = dict()
ranks = dict()
result = {"nodes": list(), "links": list()}
for row in nodes:
    weight, old_rank, new_rank, id, url = row
    rank = 15 * ((rank - minrank) / (maxrank - minrank))
    result['nodes'].append({"weight": weight, "rank": rank, 'id': id, 'url': url})
    map[id] = count
    ranks[id] = rank
    count += 1

cur.execute('SELECT DISTINCT from_id, to_id FROM Links')

count = 0
for row in cur:
    from_id, to_id = row
    if from_id not in map or to_id not in map:
        continue
    result['links'].append({'source': map[from_id], 'target': map[to_id], "value": 3})
    count += 1

json_str = json.dumps(result, indent=4)
fh = open('view/spider.js', 'w')
fh.write(f'var spiderJson = {json_str}')
cur.close()

print('Open index.html in a browser to view the visualization')