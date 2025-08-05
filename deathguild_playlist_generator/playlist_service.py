from db_handler import DBHandler
import logging

logging.basicConfig(filename='deathguild_playlist_generator/deathguild_playlist_generator.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class PlaylistService:
    def __init__(self, db: DBHandler):
        self.db = db

    def get_or_create_song(self, song):
        title = song['title']
        artist = song['artist']
        song = self.db.get_song(title, artist)
        if not song:
            logging.info(f"Song not found, inserting new song into db: {title} by {artist}")
            song_id = self.db.insert_song(title, artist)
        else:
            logging.info(f"Song found in db, returning song id for: {title} by {artist} with id {song.id}")
            song_id = song.id
        return song_id

    def get_or_create_playlist(self, playlist, date):
        playlist = self.db.get_playlist(date)
        if not playlist:
            logging.info(f"Playlist not found, inserting new playlist into db on date: {date}")
            print("Inserting new playlist into db: ", date)
            playlist_id = self.db.insert_playlist(date)
        else:
            logging.info(f"Playlist found in db, returning playlist on date: {date} with id {playlist.id}")
            print("Found playlist in db: ", date, playlist.id)
            playlist_id = playlist.id
        return playlist_id

    def create_if_not_exists_playlist_song(self, playlist_id, song_id, position, is_request):
        logging.info(f"Insering playlist song: {playlist_id}, {song_id}")
        self.db.insert_playlist_songs(playlist_id, song_id, position, is_request)
