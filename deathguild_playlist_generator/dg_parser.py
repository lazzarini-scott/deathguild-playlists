from collections import defaultdict
import datetime
import re

from bs4 import BeautifulSoup
import requests


def parse_urls(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    pattern = re.compile(r'playlist/\d{4}-\d{2}-\d{2}/')
    links = soup.find_all('a', href=pattern)
    return links

def parse_playlist(html):
    soup = BeautifulSoup(html, 'html.parser')
    # Find the div with id "playlist"
    date_text = soup.find('span', class_='date').get_text(strip=True)
    date = datetime.strptime(date_text, "%B %d, %Y").strftime("%Y-%m-%d")

    songs = soup.find_all('em')
    position = 1
    playlist = defaultdict(list)
    for song in songs:
        artist = song.get_text(strip=True)
        if song.next_sibling and '-' in song.next_sibling:
            title = song.next_sibling.replace('-', '').strip()
        print(artist, title)
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
        playlist[date].append(song_data)
        position += 1

    
    return playlist, date