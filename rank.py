"""
Ranks the webpage in Pages table with relation to how many other page points to it
"""
import sqlite3

from constants import DATABASE_NAME

conn = sqlite3.connect(DATABASE_NAME)
cur = conn.cursor()

# Find the ids that send out page rank
cur.execute('SELECT DISTINCT from_id FROM Links')
from_ids = list()
for row in cur:
    from_ids.append(row[0])

# Find ids that receive page rank
to_ids = list()
links = list()
cur.execute('SELECT DISTINCT from_id, to_id FROM Links')
for row in cur:
    from_id = row[0]
    to_id = row[1]
    # skipping unwanted ids
    if from_id == to_id:
        continue
    if from_id not in from_ids:
        continue
    # if to_id doesn't point to any other page, skip it from rank calculation
    if to_id not in from_ids:
        continue
    links.append(row)
    if to_id not in to_ids:
        to_ids.append(to_id)

# Get latest page ranks for strongly connected components (i.e. there is a path b/w two pages)
prev_ranks = dict()
for node in from_ids:
    cur.execute('SELECT new_rank FROM Pages WHERE id=?',(node,))
    row = cur.fetchone()
    prev_ranks[node] = row[0]

iteration_count = 0
while True:
    try:
        iteration_count = int(input('How many iteration: '))
        break
    except ValueError:
        print('error: Invalid number input')
        continue
    except KeyboardInterrupt:
        print('Program interrupted by user...')
        quit()

if len(prev_ranks) < 1:
    print('Nothing to page rank. Check data.')
    quit()

# Page ranking in memory
for i in range(iteration_count):
    # calc total of old_rank and initialize next_ranks
    next_ranks = dict()
    total = 0.0
    for node, old_rank in prev_ranks.items():
        total += old_rank
        next_ranks[node] = 0.0

    # Find the number of outbound links
    for node, old_rank in prev_ranks.items():
        give_ids = list()
        for from_id, to_id in links:
            if from_id != node:
                continue
            if to_id not in to_ids:
                continue
            give_ids.append(to_id)

        if len(give_ids) < 1:
            continue

        amount = old_rank / len(give_ids)

        for id in give_ids:
            next_ranks[id] += amount

    new_total = 0.0
    for node, next_rank in next_ranks.items():
        new_total += next_rank

    """
    total is the sum of the PageRank values from the previous iteration (old_rank).
    new_total is the sum of the PageRank values after redistribution in the current iteration (new_ranks).
    evaporate represents the average difference between total and new_total per page. This difference arises
    because the PageRank might not distribute perfectly due to factors like pages without outbound links.
    """
    evaporate = (total - new_total) / len(next_ranks)

    """
    To correct the discrepancy (total - new_total) and maintain consistency, evaporate is added to each page's rank.
    This adjustment helps ensure that the sum of PageRank values remains the same across iterations, 
    even though individual ranks might change.
    """
    for node in next_ranks:
        next_ranks[node] += evaporate

    new_total = 0.0
    for node, next_rank in next_ranks.items():
        new_total += next_rank

    # Compute per page average chagne from old rank to new rank
    # As indication of convergence of algorithm
    totaldiff = 0
    for node, old_rank in prev_ranks.items():
        new_rank = next_ranks[node]
        diff = abs(old_rank - new_rank)
        totaldiff += diff

    avgdiff = totaldiff / len(prev_ranks)
    print(i + 1, avgdiff)

    # rotate
    prev_ranks = next_ranks

# Commit final ranks to database
print(list(next_ranks.items())[:5])
cur.execute('UPDATE Pages SET old_rank=new_rank')
for id , new_rank in next_ranks.items():
    print(id, new_rank)
    cur.execute('UPDATE Pages SET new_rank=? WHERE id=?', (new_rank, id))

conn.commit()
cur.close()

