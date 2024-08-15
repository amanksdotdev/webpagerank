"""
Resets the database file by DELETING all table data
pass argument --drop to DROP the tables
"""
import sqlite3
import sys

from constants import DATABASE_NAME

conn = sqlite3.connect(DATABASE_NAME)
cur = conn.cursor()

argv = sys.argv[1:]

drop = True if len(argv) > 0 and argv[0] == '--drop' else False

inp = input(f'Are you sure you want to reset (DROP={drop})? (Y/n): ').lower()

if inp == 'n':
    print('Aborted')
    quit()


if drop:
    cur.execute('DROP TABLE IF EXISTS Pages, Websites, Links')
    print('All tables dropped')
else:
    cur.execute('DELETE FROM Pages')
    cur.execute('DELETE FROM Websites')
    cur.execute('DELETE FROM Links')
    print('All tables data cleared')

conn.commit()

cur.close()






