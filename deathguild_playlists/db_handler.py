import sqlite3
import logging

from dtos import Song, Playlist

logging.basicConfig(filename='deathguild_playlist_generator.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# class DBHandler:
#     def __init__(self, db_path="deathguild_playlist_generator/db/deathguild.db"):
#         try:
#             # Enable concurrent access and increase timeout to avoid lock issues
#             self.connection = sqlite3.connect(
#                 db_path,
#                 check_same_thread=False,
#                 timeout=30,
#                 isolation_level=None  # Use explicit transactions
#             )
#             self.connection.execute("PRAGMA journal_mode=WAL;")
#             self.connection.row_factory = sqlite3.Row
#             self.cursor = self.connection.cursor()
#         except sqlite3.Error as e:
#             logging.error(f"Database connection error: {e}")
#             raise

#     def insert_playlist(self, date):
#         try:
#             self.cursor.execute("BEGIN")
#             self.cursor.execute("INSERT OR IGNORE INTO playlists (date) VALUES (?)", (date,))
#             self.cursor.execute("COMMIT")
#             return self.cursor.lastrowid
#         except Exception as e:
#             self.connection.rollback()
#             logging.error(f"Error inserting playlist: {e}")
#             return None

#     def insert_song(self, title, artist):
#         try:
#             self.cursor.execute("BEGIN")
#             self.cursor.execute("INSERT OR IGNORE INTO songs (title, artist) VALUES (?, ?)", (title, artist))
#             self.cursor.execute("COMMIT")
#             return self.cursor.lastrowid
#         except Exception as e:
#             self.connection.rollback()
#             logging.error(f"Error inserting song: {e}")
#             return None

#     def insert_playlist_songs_bulk(self, items):
#         """Insert a list of (playlist_id, song_id, position, is_request) tuples"""
#         try:
#             self.cursor.execute("BEGIN")
#             self.cursor.executemany(
#                 "INSERT OR IGNORE INTO playlist_songs (playlist_id, song_id, position, is_request) VALUES (?, ?, ?, ?)",
#                 items
#             )
#             self.cursor.execute("COMMIT")
#         except Exception as e:
#             self.connection.rollback()
#             logging.error(f"Error inserting playlist_songs bulk: {e}")
#             return None

#     def get_playlist(self, date):
#         try:
#             self.cursor.execute("SELECT id, date FROM playlists WHERE date = ?", (date,))
#             row = self.cursor.fetchone()
#             return Playlist(*row) if row else None
#         except Exception as e:
#             logging.error(f"Error fetching playlist: {e}")
#             return None

#     def get_song(self, title, artist):
#         try:
#             self.cursor.execute("SELECT id, artist, title FROM songs WHERE title = ? AND artist = ?", (title, artist))
#             row = self.cursor.fetchone()
#             return Song(*row) if row else None
#         except Exception as e:
#             logging.error(f"Error fetching song: {e}")
#             return None

#     def close(self):
#         try:
#             self.connection.close()
#         except Exception as e:
#             logging.error(f"Error closing database: {e}")

class DBHandler:
    def __init__(self, db_path="deathguild_playlist_generator/db/deathguild.db"):
        try:
            self.connection = sqlite3.connect(db_path)
            self.cursor = self.connection.cursor()
        except sqlite3.Error as e:
            logging.error(f"Database connection error: {e}")
            raise
    
    def insert_playlist(self, date):
        try:
            self.cursor.execute("INSERT INTO playlists (date) VALUES (?)", ((date),))
            self.connection.commit()
            return self.cursor.lastrowid
        except Exception as e:
            logging.error(f"Error inserting playlist into database {e}")
            return None

    def insert_song(self, title, artist):
        try:
            self.cursor.execute("INSERT INTO songs (title, artist) VALUES (?, ?)", (title, artist))
            self.connection.commit()
            return self.cursor.lastrowid
        except Exception as e:
            logging.error(f"Error inserting song into database: {e}")
            return None

    def insert_playlist_songs(self, playlist_id, song_id, position, is_request):
        try:
            self.cursor.execute("INSERT OR IGNORE INTO playlist_songs (playlist_id, song_id, position, is_request) VALUES (?, ?, ?, ?)", 
                                (playlist_id, song_id, position, is_request))
            self.connection.commit()
        except Exception as e:
            logging.error(f"Error inserting playlist_song to database: {e}")
            return None
        
    def get_playlist(self, date):
        try:
            self.cursor.execute("SELECT id, date FROM playlists WHERE date = ?", ((date),))
            row = self.cursor.fetchone()
            return Playlist(*row) if row else None
        except Exception as e:
            logging.error(f"Failed to check if playlist exists: {e}")
            return None

    def get_song(self, title, artist):
        try:
            self.cursor.execute("SELECT id, artist, title FROM songs WHERE title = ? AND artist = ?", (title, artist))
            row = self.cursor.fetchone()
            return Song(*row) if row else None
        except Exception as e:
            logging.error(f"Failed to check if song exists: {e}")
            return None

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            logging.error(f"Error closing database connection: {e}")