from async_db_handler import AsyncDBHandler
import logging

logging.basicConfig(filename='deathguild_playlist_generator/deathguild_playlist_generator.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class AsyncPlaylistService:
    def __init__(self, db_handler: AsyncDBHandler):
        self.db = db_handler

    async def add_song_to_playlist(self, date, title, artist, position, is_request=False):
        playlist = await self.db.get_playlist(date)
        if not playlist:
            playlist_id = await self.db.insert_playlist(date)
        else:
            playlist_id = playlist.id

        song = await self.db.get_song(title, artist)
        if not song:
            song_id = await self.db.insert_song(title, artist)
        else:
            song_id = song.id

        await self.db.insert_playlist_song(playlist_id, song_id, position, is_request)

    async def create_playlist_with_songs(self, date, songs):
        """
        songs: list of dicts like [{'title': 'Song A', 'artist': 'Artist A', 'position': 1, 'is_request': False}, ...]
        """
        playlist_id = await self.db.insert_playlist(date)
        for s in songs:
            song = await self.db.get_song(s['title'], s['artist'])
            if not song:
                song_id = await self.db.insert_song(s['title'], s['artist'])
            else:
                song_id = song.id
            await self.db.insert_playlist_song(playlist_id, song_id, s['position'], s.get('is_request', False))

    async def add_or_get_playlist_id(self, date):
        playlist = await self.db.get_playlist(date)
        if playlist:
            return playlist.id
        return await self.db.insert_playlist(date)

    async def add_or_get_song_id(self, title, artist):
        song = await self.db.get_song(title, artist)
        if song:
            return song.id
        return await self.db.insert_song(title, artist)