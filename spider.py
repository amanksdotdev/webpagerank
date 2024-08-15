"""
Spider the webpage and store data in a database
"""
import sqlite3
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup

from constants import DATABASE_NAME
from helper import is_valid_url

# Database setup: 3 tables needed 1) Pages 2) Links 3) Websites (if we crawled more than one website)
conn = sqlite3.connect(DATABASE_NAME)
cur = conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS Pages 
            (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
            error INTEGER, old_rank REAL, new_rank REAL)
            """)

cur.execute("""CREATE TABLE IF NOT EXISTS Websites
            (url TEXT UNIQUE)
            """)

cur.execute("""CREATE TABLE IF NOT EXISTS Links
            (from_id INTEGER, to_id INTEGER, UNIQUE(from_id, to_id))
            """)

# Randomly pick a random page url from Pages whose data hasn't been fetched yet
# if we get a None then do initial spidering of the website by asking URL from the user
cur.execute('SELECT id, url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
row = cur.fetchone()

print('randomly selected row (page):', row)

if row is not None:
    print('Restarting existing crawl. Remove webpagerank.sqlite to start a fresh crawl.')
else:
    while True:
        starturl = input('Enter web url to crawl: ')
        if not is_valid_url(starturl):
            print('error: Invalid url')
            continue
        break
    # remove / from end of url
    if starturl.endswith('/'):
        starturl = starturl[:-1]

    website = starturl
    # remove .html or .htm extension from end of url
    if starturl.endswith('.htm') or starturl.endswith('.html'):
        idx = starturl.rfind('/')
        website = starturl[:idx]

    # if url is valid then add it to Websites table and Pages table with default data
    if len(starturl) > 1:
        cur.execute('INSERT OR IGNORE INTO Websites (url) VALUES (?)', (website,))
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES (?, NULL, 1.0)',(starturl,))
        conn.commit()


# create a list of websites present in Websites database, used to skip anchor tags with outside links
cur.execute('SELECT url FROM Websites')
websites = list()
for row in cur:
    websites.append(row[0])

print('Websites:',websites)

# Infinite while loop: ask user how many links to retrieve
page_count = 0
while True:
    if page_count < 1:
        try:
            page_count = int(input('How many pages to crawl and retrieve html and links: '))
        except ValueError:
            print('error: Invalid number')
            continue
        except KeyboardInterrupt:
            print('\nProgram interrupted by user...')
            quit()

    page_count -= 1

    # select random page from table
    cur.execute('SELECT id, url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
    try:
        row = cur.fetchone()
        fromid = row[0]
        url = row[1]
    except:
        print('No un-retrieved HTML pages found')
        page_count = 0
        break

    print(f'id={fromid}, url={url}', end=' ')

    # Delete the retrieved page from Links table becuase we are going to fill it
    cur.execute('DELETE FROM Links WHERE from_id=?',(fromid,))
    try:
        response = requests.get(url)
        html = response.text

        if response.status_code != 200:
            print('Error on page: ', response.status_code)
            cur.execute('UPDATE Pages SET error=? WHERE url=?',(response.status_code, url))

        if 'text/html' not in response.headers['content-type']:
            print('Ignore non text/html pages')
            cur.execute('DELETE FROM Pages WHERE url=?',(url,))
            conn.commit()
            continue

        print(f'len(html)=({str(len(html))})', end=' ')

        soup = BeautifulSoup(html, 'html.parser')
    except KeyboardInterrupt:
        print('\nProgram interrupted by user...')
        break
    except:
        print('Unable to retrieve or parse page')
        cur.execute('UPDATE Pages SET error=-1 WHERE url=?',(url,))
        conn.commit()
        continue

    cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES (?, NULL, 1.0)', (url,))
    cur.execute('UPDATE Pages SET html=? WHERE url=?', (html, url))
    conn.commit()

    # Retrieve all the anchor tags
    tags = soup('a')
    count = 0
    for tag in tags:
        href = tag.get('href', None)
        if href is None:
            continue

        parsed_url = urlparse(href)
        if len(parsed_url.scheme) < 1:
            href = urljoin(url, href)

        poundpos = href.find('#')
        if poundpos > 1:
            href = href[:poundpos]
        if href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif'):
            continue
        if href.endswith('/'):
            href = href[:-1]

        if len(href) < 1:
            continue

        # check if url is in any of the websites
        found = False
        for website in websites:
            if href.startswith(website):
                found = True
                break

        if not found:
            continue

        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES (?, NULL, 1.0)', (href,))
        count = count + 1
        conn.commit()

        cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', (href,))
        try:
            row = cur.fetchone()
            toid = row[0]
        except:
            print('Could not retrieve id')
            continue
        cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES (?, ?)', (fromid, toid))
        conn.commit()

    print("Total links found:",count)

cur.close()


