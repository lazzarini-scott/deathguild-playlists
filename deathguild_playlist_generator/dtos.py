from dataclasses import dataclass
import datetime

@dataclass
class Playlist:
    id: int
    date: str
    spotify_id: str = None

@dataclass
class PlaylistSong:
    id: int
    playlist_id: int
    song_id: int
    position: int
    is_request: bool

@dataclass
class Song:
    id: int
    title: str
    artist: str
    spotify_id: str = None