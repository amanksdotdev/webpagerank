import sqlite3

from constants import DATABASE_NAME

conn = sqlite3.connect(DATABASE_NAME)
cur = conn.cursor()

cur.execute('''UPDATE Pages SET new_rank=1.0, old_rank=0.0''')
conn.commit()

cur.close()

print("All pages set to a rank of 1.0")
