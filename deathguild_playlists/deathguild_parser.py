from collections import defaultdict
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
import logging
import asyncio
import aiohttp
from urllib.parse import urljoin

from db_handler import DBHandler
from playlist_service import PlaylistService
from async_playlist_service import AsyncPlaylistService
from async_db_handler import AsyncDBHandler
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import os
import time

MAX_CONSUMERS = 100
BATCH_SIZE = 10

executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONSUMERS)
sem = asyncio.Semaphore(25)  # throttle concurrent fetches
logging.basicConfig(filename=os.path.abspath('deathguild_playlist_generator.log'), level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def parse_urls(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=lambda x: x and re.fullmatch(r'/playlist/\d{4}-\d{2}-\d{2}', x))
    absoluteLinks = [urljoin("http://www.deathguild.com", link['href']) for link in links]
    return absoluteLinks

def validateSong(song):
    if not song.get('title') or not song.get('artist'):
        raise ValueError("Critical error: Song must have both title and artist")

def parse_playlist(html):
    soup = BeautifulSoup(html, 'html.parser')
    date_text = soup.find('span', class_='date').get_text(strip=True)
    date = datetime.strptime(date_text, "%B %d, %Y").strftime("%Y-%m-%d")
    
    songs = soup.find_all('em')
    position = 1
    playlist = defaultdict(list)
    for song in songs:
        artist = song.get_text(strip=True)
        if song.next_sibling and '-' in song.next_sibling:
            title = song.next_sibling.replace('-', '').strip()
        is_request = False
        request_tag = song.find_next_sibling()
        if 'request' in request_tag.get('class', []):
            is_request = True
        song_data = {
            'artist': artist,
            'title': title,
            'position': position,
            'is_request': is_request
        }
        validateSong(song_data)
        logging.info(f"Appending parsed song: {title} by {artist} at position {position} is requested {is_request}")
        playlist[date].append(song_data)
        position += 1
    #print(f"Parsed {len(playlist[date])} songs for date: {date}")
    return playlist, date

async def fetch_and_parse(session, url, queue):
    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                html = await resp.text()
                playlist, date = parse_playlist(html)
                if playlist and date:
                    await queue.put((playlist, date))
                    print(f"Queued playlist for {date} from {url}")
        except asyncio.TimeoutError:
            print(f"Timeout while fetching {url}")
            logging.error(f"Timeout while fetching {url}")
        except Exception as e:
            logging.error(f"Failed to fetch or parse {url}: {e}")

async def db_consumer(queue: asyncio.Queue, db_path="deathguild_playlist_generator/db/deathguild.db"):
    db = AsyncDBHandler(db_path)
    await db.connect()
    playlist_service = AsyncPlaylistService(db)

    try:
        while True:
            batch = []

            while len(batch) < BATCH_SIZE:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=3)
                    if item is None:
                        break
                    batch.append(item)
                    queue.task_done()
                except asyncio.TimeoutError:
                    break

            if batch:
                for playlist, date in batch:
                    pid = await playlist_service.add_or_get_playlist_id(date)

                    bulk = playlist.get(date, [])
                    for song in bulk:
                        sid = await playlist_service.add_or_get_song_id(song['title'], song['artist'])
                        await db.insert_playlist_song(pid, sid, song['position'], song['is_request'])

                print(f"🔹 Inserted {len(batch)} playlists to DB")
            else:
                await asyncio.sleep(0.2)
            if item is None:
                break

    finally:
        await db.close()

async def main(urls):
    start = time.time()
    queue = asyncio.Queue()

    CONSUMER_COUNT = min(MAX_CONSUMERS, max(10, len(urls) // 25))
    consumers = [asyncio.create_task(db_consumer(queue)) for _ in range(CONSUMER_COUNT)]

    async with aiohttp.ClientSession() as session:
        fetchers = [fetch_and_parse(session, url, queue) for url in urls]
        await asyncio.gather(*fetchers)
    
    print("Fetch complete — inserting sentinels to shut down consumers")
    for _ in range(CONSUMER_COUNT):
        await queue.put(None)

    await asyncio.gather(*consumers)
    elapsed = time.time() - start
    print(f"Total elapsed time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    try:
        main_url = "http://www.deathguild.com/playdates/"
        urls = parse_urls(main_url)
        logging.info(f"Found playlist URLs: {urls}")
        print(len(urls), " playlists found")
        start = time.time()
        asyncio.run(main(urls[:25]))
        print(f"Elapsed: {time.time() - start:.2f} seconds")
    except Exception as e:
        logging.exception(f"Unhandled exception: {e}")