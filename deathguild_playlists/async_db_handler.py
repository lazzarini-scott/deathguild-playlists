import aiosqlite
import logging

from dtos import Song, Playlist

class AsyncDBHandler:
    def __init__(self, db_path="deathguild_playlist_generator/db/deathguild.db"):
        self.db_path = db_path
        self.conn = None

    async def connect(self):
        self.conn = await aiosqlite.connect(self.db_path)
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.conn.execute("PRAGMA synchronous=NORMAL;")
        await self.conn.commit()

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def insert_playlist(self, date):
        try:
            await self.conn.execute("INSERT OR IGNORE INTO playlists (date) VALUES (?)", (date,))
            await self.conn.commit()
            cursor = await self.conn.execute("SELECT last_insert_rowid()")
            row = await cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            logging.error(f"Error inserting playlist: {e}")
            return None

    async def insert_song(self, title, artist):
        try:
            await self.conn.execute(
                "INSERT OR IGNORE INTO songs (title, artist) VALUES (?, ?)", (title, artist)
            )
            await self.conn.commit()
            cursor = await self.conn.execute("SELECT last_insert_rowid()")
            row = await cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            logging.error(f"Error inserting song: {e}")
            return None

    async def insert_playlist_song(self, playlist_id, song_id, position, is_request):
        try:
            await self.conn.execute(
                "INSERT OR IGNORE INTO playlist_songs (playlist_id, song_id, position, is_request) VALUES (?, ?, ?, ?)",
                (playlist_id, song_id, position, is_request)
            )
            await self.conn.commit()
        except Exception as e:
            logging.error(f"Error inserting playlist_song: {e}")
            return None

    async def get_playlist(self, date):
        try:
            cursor = await self.conn.execute(
                "SELECT id, date FROM playlists WHERE date = ?", (date,)
            )
            row = await cursor.fetchone()
            return Playlist(*row) if row else None
        except Exception as e:
            logging.error(f"Error fetching playlist: {e}")
            return None

    async def get_song(self, title, artist):
        try:
            cursor = await self.conn.execute(
                "SELECT id, artist, title FROM songs WHERE title = ? AND artist = ?",
                (title, artist)
            )
            row = await cursor.fetchone()
            return Song(*row) if row else None
        except Exception as e:
            logging.error(f"Error fetching song: {e}")
            return None