import re
import sqlite3 as sq3
from collections import namedtuple
from movies.db.sqlite_extensions import register_functions
from movies.conf import DB_FP, DATA_MAP

COLS_RE = re.compile("|".join(DATA_MAP.values()))

# sq3.enable_callback_tracebacks(True)

class DatabaseManager:
    def __init__(self, tests=False):
        self.db_fp = "tests/tmp.db" if tests else DB_FP
        self.con = self._connect()

    def _connect(self):
        con = sq3.connect(self.db_fp)
        register_functions(con)
        return con

    def get_titles(self):
        """List titles."""
        command = "SELECT title FROM movies;"
        try:
            cursor = self.con.cursor()
            command = "SELECT title FROM movies;"
            titles = cursor.execute(command).fetchall()
        except sq3.Error as err:
            raise ValueError(err)
        else:
            return [title[0] for title in titles]
        finally: 
            cursor.close()

    def insert_one(self, query, data, check=False):
        """Insert or update op."""
        try:        
            if check:
                self.has_title(data[0], has=True)
            self.con.execute(query, data)
            self.con.commit()
        except sq3.Error as err:
            raise ValueError(err)

    def insert_many(self, query, data, check=False):
        """Inserts or update ops."""
        try:
            if check:
                for movie in data:
                    self.has_title(movie[0], has=True)
            self.con.executemany(query, data)
            self.con.commit()
        except sq3.Error as err:
            raise ValueError(err)

    def select_one(self, query, data=None, check=False):
        try:    
            cursor = self.con.cursor()
            data = self.select_logic(cursor, query, data=data, check=check).fetchall()
            return data
        except sq3.Error as err:
            raise ValueError(err)
        finally: 
            cursor.close()

    def select_logic(self, cursor, query, data=None, check=False):
        if data:
            if check:
                for title in data:
                    self.has_title(title, has=False)
                return cursor.execute(query, data)
            return cursor.execute(query, data)
        return cursor.execute(query)

    def select_many(self, queries, data=None, check=False):
        data = [
            self.select_one(query, data=data, check=check)
            for query in queries
        ]
        return data

    def has_title(self, title, has):
        """Check whether movie title is in db."""
        query = "select * from movies where title=?"
        try:
            cur = self.con.cursor()
            movie = cur.execute(query, tuple([title])).fetchone()
            if has == bool(movie):
                raise ValueError(f'Error: Movie {"is" if has else "not"} in DB.')
        finally:
            cur.close()
