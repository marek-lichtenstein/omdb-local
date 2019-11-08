import hashlib
from operator import iadd, methodcaller
from itertools import chain
from movies.conf import INITIAL_DB_CHECKSUM, DB_FP, CREDENTIALS
import movies.db.query as query
import movies.db.dbm as dbm
from movies.requester import Downloader


class Commander:
    def __init__(self, ignore_checksum=False):
        self.ignore_checksum = ignore_checksum
        self.downloader = Downloader(credentials=CREDENTIALS)
        self.printer = DataPrinter()
        self.populate_db()

    def _verify_db_checksum(self, filepath=DB_FP):
        checksum = hashlib.sha1()
        with open(filepath, "rb") as file_:
            for chunk in iter(lambda: file_.read(4096), b""):
                checksum.update(chunk)
        return checksum.hexdigest() == INITIAL_DB_CHECKSUM

    def populate_db(self):
        if self.ignore_checksum or self._verify_db_checksum():
            self._dl_upload()

    def _dl_upload(self):
        titles = dbm.get_titles()
        movie_data = self.downloader.download_many(titles, process=True, rotated=True)
        dbm.insert_many(query.update(), movie_data)

    def sort_by(self, *args):
        """Sort movies by column(s)""" 
        try:
            args_ = set(args)
            if len(args_) != len(args):
                return "Please provide unique sorting parameters."
            data = dbm.select_one(query.sort(*args))
            return self.printer.display(data, args)
        except ValueError as err:
            return err

    def filter_by(self, category):
        """Filter data by a category."""
        try:
            data = dbm.select_one(query.filter_(category[0]), tuple([category[1]]))
            if not data:
                return "No movie match this restriction."
            return self.printer.display(data, columns=["title", category[0]])
        except ValueError as err:
            return err 
                   
    def compare(self, category, movie1, movie2):
        """Compare two movies by a category."""
        try:
            data = dbm.select_one(
                query.compare(category), data=(movie1, movie2), check=True
            )
            return self.printer.display(data, columns=["title", category])
        except ValueError as err:
            return err

    def add_movie(self, title):
        """Add movie to the database. Print msg if it's not found."""   
        try:
            movie_data = self.downloader.download_one(title, process=True)
            dbm.insert_one(query.insert(), data=movie_data, check=True)
            return "Movie added."
        except ValueError as err:
            return err

    def highscores(self):
        """Return highest value from columns:\n
        Runtime, Box office earnings, Most awards won,\n
        Most nominations, Most Oscars, Highest IMDB Rating.
        """
        try:
            data = dbm.select_many(query.highscores()) 
            return self.printer.display(data, columns=["Column", "Movie", "Value"], index_col=True)
        except ValueError as err:
            return err

class DataPrinter:
    def __init__(self, folding=True, max_rows=30, line_sym="-", fold_sym=":"):
        self.folding = folding
        self.max_rows = max_rows
        self.line_sym = line_sym
        self.fold_sym = fold_sym
        self.cats = {
            "language": "Language",
            "director": "Director",
            "actor": "Actor",
            "oscar_nom": "Oscar nominations",
            "imdb": "IMDb Rating",
            "boxoffice": "Box office earnings",
            "awards": "Number of awards won",
            "runtime": "Runtime",
            "title": "Title",
            "year": "Year",
            "genre": "Genre",
            "writer": "Writer",
            "country": "Country",
            "rating": "IMDb Rating",
            "votes": "Votes",
            "eighty": "Number of awards won",
        }

    def _flatten(self, data):
        return list(chain(*data)) if isinstance(data[0], list) else data

    def _get_width(self, data):
        get_width = methodcaller("widest_element")
        return max(map(get_width, data))

    def _table(self, width, columns):
        # header
        func = lambda col: self.cats.get(col, col).center(width)
        _header = "|".join(map(func, columns))
        header = "".join(["|", _header, "|"])
        # lining
        _lining = "|".join([self.line_sym * width] * len(columns))
        lining = "".join(["|", _lining, "|"])
        # folding
        _folding_ = "|".join([self.fold_sym * width] * len(columns))
        _folding = "".join(["|", _folding_, "|"])
        folding = "\n".join([_folding] * 3)
        return header, lining, folding

    def display(self, data, columns, index_col=False):
        columns = list(columns)
        if "title" in columns:
            columns.remove("title")
        if not index_col:
            columns.insert(0, "title")
        data = self._flatten(data)
        row_width = self._get_width(data)
        header, lining, folding = self._table(row_width, columns)
        func = methodcaller("display_row", row_width)
        if index_col:
            cats = ["Runtime", "Box Office", "Awards Won", "Nominations", "Oscars", "IMDB Rating",]
            cat_list = map(lambda cat: cat.center(row_width), cats)
            row_list = list(map(func, data))
            _rows_ = ["".join([cat, row_list[i]]) for i, cat in enumerate(cat_list)]
            _rows = [iadd("|", row) for row in _rows_]
            data = "\n".join(_rows)
            return "\n".join([lining, header, lining, data, lining])
            
        if self.folding and len(data) > self.max_rows:
            i = self.max_rows // 2
            data_1, data_2 = "\n".join(map(func, data[:i])), "\n".join(map(func, data[-i:]))
            return "\n".join([lining, header, lining, data_1, lining, folding, lining, data_2, lining])
        data = "\n".join(map(func, data))
        return "\n".join([lining, header, lining, data, lining])
