from collections import defaultdict
from datetime import datetime
import time
import requests
from bs4 import BeautifulSoup
import re
import logging
import asyncio
import aiohttp
from urllib.parse import urljoin

from db_handler import DBHandler
from playlist_service import PlaylistService

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
    return playlist, date

def main(urls):
    db = DBHandler()
    playlist_service = PlaylistService(db)
    try:
        for url in urls:
            playlist, date = parse_playlist(requests.get(url).text)
            print(f"Parsed playlist for date: {date} with {len(playlist[date])} songs")
            playlist_id = playlist_service.get_or_create_playlist(playlist, date)
            for song in playlist[date]:
                song_id = playlist_service.get_or_create_song(song)
                playlist_service.create_if_not_exists_playlist_song(playlist_id, song_id, song['position'], song['is_request'])
    finally:
        db.close()

if __name__ == "__main__":
    try:
        main_url = "http://www.deathguild.com/playdates/"
        urls = parse_urls(main_url)
        logging.info(f"Found playlist URLs: {urls}")
        print(len(urls), " playlists found")
        start = time.time()
        main(urls)
        print(f"Elapsed: {time.time() - start:.2f} seconds")
    except Exception as e:
        logging.exception(f"Unhandled exception: {e}")