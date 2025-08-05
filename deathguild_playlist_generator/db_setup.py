import sqlite3

conn = sqlite3.connect('deathguild_playlist_generator/db/deathguild.db')
c = conn.cursor()

c.execute("""
DROP TABLE IF EXISTS playlist_songs
""")

c.execute("""
DROP TABLE IF EXISTS songs
""")

c.execute("""
CREATE TABLE IF NOT EXISTS songs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        artist TEXT NOT NULL,
        spotify_id TEXT)
""")

c.execute("""
CREATE UNIQUE INDEX idx_unique_songs ON songs (artist, title)
""")

c.execute("""
DROP TABLE IF EXISTS playlists
""")

c.execute(""" 
CREATE TABLE IF NOT EXISTS playlists (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date date NOT NULL UNIQUE,
        spotify_id TEXT)
""")

c.execute(""" 
CREATE TABLE IF NOT EXISTS playlist_songs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        playlist_id INTEGER NOT NULL REFERENCES playlists(id),
        song_id INTEGER NOT NULL REFERENCES songs(id),
        position INTEGER NOT NULL,
        is_request BOOLEAN NOT NULL DEFAULT FALSE)
""")


conn.commit