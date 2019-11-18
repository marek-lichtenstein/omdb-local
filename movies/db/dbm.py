import re
import sqlite3 as sq3
from collections import namedtuple
from movies.db.sqlite_extensions import register_functions
from movies.conf import DB_FP, DATA_MAP

COLS_RE = re.compile("|".join(DATA_MAP.values()))

# sq3.enable_callback_tracebacks(True)


def column_names():
    """List column names."""
    command = f"PRAGMA table_info(movies);"
    try:
        con = connect()
        cursor = con.cursor()
        table_info = cursor.execute(command).fetchall()
    except sq3.Error as err:
        print(err)
    else:
        return [column_info[1] for column_info in table_info][1:]
    finally:
        cursor.close()
        con.close()

def get_titles(tests=False):
    """List titles."""
    
    command = "SELECT title FROM movies;"
    con = connect(tests=tests)
    try:
       
        cursor = con.cursor()
        command = "SELECT title FROM movies;"
        titles = cursor.execute(command).fetchall()
        cursor.close()
    except sq3.Error as err:
        raise ValueError(err)
    else:
        return [title[0] for title in titles]
    finally: 
        con.close()

def insert_one(query, data, check=False, tests=False):
    """Insert or update op."""
    con = connect(tests=tests)
    try:        
        if check:
            has_title(data[0], has=True, tests=tests)
        with con:
            con.execute(query, data)
            con.commit()
    except sq3.Error as err:
        raise ValueError(err)

def insert_many(query, data, check=False, tests=False):
    """Inserts or update ops."""
    con = connect(tests=tests)   
    try:
        if check:
            for movie in data:
                has_title(movie[0], has=True)
        with con:
            con.executemany(query, data)
            con.commit()
    except sq3.Error as err:
        raise ValueError(err)

def select_one(query, data=None, check=False, tests=False):
    con = connect(tests=tests) 
    try:    
        cursor = con.cursor()
        data = conv_row(
            select_logic(cursor, query, data=data, check=check), tests=tests
        )
        return data
    except sq3.Error as err:
        raise ValueError(err)
    finally: 
        con.close()

def select_logic(cursor, query, data=None, check=False, tests=False):
    if data:
        if check:
            for title in data:
                has_title(title, has=False, tests=tests)
            return cursor.execute(query, data)
        return cursor.execute(query, data)
    return cursor.execute(query)

def select_many(queries, data=None, check=False, tests=False):
    data = [
        select_one(query, data=data, check=check, tests=tests)
        for query in queries
    ]
    return data

def has_title(title, has, tests=False):
    """Check whether movie title is in db."""
    query = "select * from movies where title=?"
    con = connect(tests=tests)
    try:
        cur = con.cursor()
        movie = cur.execute(query, tuple([title])).fetchone()
        if has == bool(movie):
            raise ValueError(f'Error: Movie {"is" if has else "not"} in DB.')
    finally:
        cur.close()

def clean_keys(keys):
    """Clean table columns names."""
    return {COLS_RE.search(col).group(): value for col, value in keys.items()}

def conv_row(row, tests=False): 
    data = list(map(clean_keys, iter(map(dict, row)))) 
    if tests:
        return list(map(lambda x: MovieInfo(**x).test_tuple(), data))
    data = list(map(lambda x: MovieInfo(**x), data))
    return data

def connect(tests=False):   
    if tests:
        con = sq3.connect("tests/test.db")
    con = sq3.connect(DB_FP)
    con.row_factory = sq3.Row
    register_functions(con)
    return con


class MovieInfo(namedtuple("Row", DATA_MAP.values(), defaults=[None] * len(DATA_MAP))):
    """An object to store data and to return formatted information about them."""

    def test_tuple(self):
        """Returning a standard tuple with values made from 'not-None' fields."""
        func = lambda att: getattr(self, att) if getattr(self, att) else None
        return tuple(filter(bool, map(func, self._fields,),))

    def display_row(self, width):
        """Return string of data in a table row style."""
        func = lambda att: getattr(self, att).center(width) if getattr(self, att) else None
        row_data = "|".join(filter(bool, map(func, self._fields,),))
        return "".join(["|", row_data, "|"])
    
    def widest_element(self):
        return max(max(map(lambda att: len(getattr(self, att) if getattr(self, att) else ""), self._fields)), 21)


class DatabaseManager:
    def __init__(self, tests=False):
        self.db_fp = "tests/tmp.db" if tests else DB_FP
        self.con = self._connect()
        self.tests = tests

    def _connect(self):
        con = sq3.connect(self.db_fp)
        con.row_factory = sq3.Row
        register_functions(con)
        return con

    def column_names(self):
        """List column names."""
        command = f"PRAGMA table_info(movies);"
        try:
            cursor = self.con.cursor()
            table_info = cursor.execute(command).fetchall()
        except sq3.Error as err:
            print(err)
        else:
            return [column_info[1] for column_info in table_info][1:]
        finally:
            cursor.close()

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
            data = self.conv_row(
                self.select_logic(cursor, query, data=data, check=check)
            )
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

    def clean_keys(self, keys):
        """Clean table columns names."""
        return {COLS_RE.search(col).group(): value for col, value in keys.items()}

    def conv_row(self, row): 
        data = list(map(self.clean_keys, iter(map(dict, row)))) 
        if self.tests:
            return list(map(lambda x: MovieInfo(**x).test_tuple(), data))
        data = list(map(lambda x: MovieInfo(**x), data))
        return data

class DatabaseManager_I:
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
