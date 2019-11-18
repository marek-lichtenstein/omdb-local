import pdb

import re
import os.path
import sys
import hashlib
from operator import iadd, methodcaller
from itertools import chain, zip_longest
from movies.tools import limsplit
from movies.conf import INITIAL_DB_CHECKSUM, DB_FP, CREDENTIALS, DATA_MAP
import movies.db.query as query
import movies.db.dbm as dbm
from movies.requester import Downloader


class Commander:
    def __init__(self, ignore_checksum=False):
        self.ignore_checksum = ignore_checksum
        self.db_api = dbm.DatabaseManager()
        self.printer = DataPrinter()
        self.downloader = None
        self.populate_db()

    def start_dl(self):
        try:
            return Downloader(credentials=CREDENTIALS)
        except ValueError as err:
            raise ValueError(
                f'An error occured while trying to download data: {", ".join(err.args)}'
            )

    def _verify_db_checksum(self, filepath=DB_FP):
        if not os.path.isfile(DB_FP):
            raise ValueError(f"error: {DB_FP} not found.")
        checksum = hashlib.sha1()
        with open(filepath, "rb") as file_:
            for chunk in iter(lambda: file_.read(4096), b""):
                checksum.update(chunk)
        return checksum.hexdigest() == INITIAL_DB_CHECKSUM

    def populate_db(self):
        try:
            if self.ignore_checksum or self._verify_db_checksum():
                self.downloader = self.start_dl()
                self._dl_upload()
        except ValueError as err:
            raise ValueError(f'An error during db operations: {", ".join(err.args)}')

    def _dl_upload(self):
        titles = dbm.get_titles()
        movie_data = self.downloader.download_many(titles, process=True, rotated=True)
        self.db_api.insert_many(query.update(), movie_data)

    def sort_by(self, *args):
        """Sort movies by column(s)"""
        try:
            args_ = set(args)
            if len(args_) != len(args):
                return "Please provide unique sorting parameters."
            data = self.db_api.select_one(query.sort(*args))
            return self.printer.display(data, args)
        except ValueError as err:
            return ", ".join(err.args)

    def filter_by(self, category):
        """Filter data by a category."""
        try:
            if len(category) == 1:
                data = self.db_api.select_one(query.filter_(category[0]))
            elif len(category) == 2:
                data = self.db_api.select_one(
                    query.filter_(category[0]), tuple([category[1]])
                )
            else:
                raise ValueError("Too many categories to filter by.")
            if not data:
                return "No movie match this restriction."
            return self.printer.display(data, columns=[category[0]])
        except ValueError as err:
            return ", ".join(err.args)

    def compare(self, category, movie1, movie2):
        """Compare two movies by a category."""
        try:
            data = self.db_api.select_one(
                query.compare(category), data=(movie1, movie2), check=True
            )
            if not data[0][1] or data[0][1] == "N/A":
                raise ValueError(
                    "Can't compare movies in that category, due to lack of data."
                )
            return self.printer.display(data, columns=[category])
        except ValueError as err:
            return ", ".join(err.args)

    def add_movie(self, title):
        """Add movie to the database. Print msg if it's not found."""
        try:
            if not self.downloader:
                self.downloader = self.start_dl()
            movie_data = self.downloader.download_one(title, process=True)
            self.db_api.insert_one(query.insert(), data=movie_data, check=True)
            return "Movie added."
        except ValueError as err:
            return ", ".join(err.args)

    def highscores(self):
        """Return highest value from columns:\n
        Runtime, Box office earnings, Most awards won,\n
        Most nominations, Most Oscars, Highest IMDB Rating.
        """
        try:
            data = self.db_api.select_many(query.highscores())
            return self.printer.print_highscores(data)
        except ValueError as err:
            return ", ".join(err.args)


COLS_RE = re.compile("|".join(DATA_MAP.values()))


class DataPrinter:
    def __init__(
        self,
        margin=2,
        default_width=25,
        max_rows=40,
        rows_pp=50,
        line_sym="-",
        hide_sym=":",
    ):
        self.rows_pp = rows_pp
        self.max_rows = max_rows
        self.line_sym = line_sym
        self.margin = margin
        self.default_col_width = default_width
        self.terminal_width = int(self.terminal_display()[1])
        self.cats = {
            "language": "Language",
            "director": "Director",
            "actor": "Actor",
            "eighty": "Movies that won more than 80% of their nominations. Awards information.",
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
            "actors": "Actors",
            "lanugages": "Languages",
        }

    def _flatten(self, data):
        return list(chain(*data)) if isinstance(data[0], list) else data

    def terminal_display(self):
        return os.popen("stty size", "r").read().split()

    def column_order(self, columns):
        columns = list(columns)
        if "title" in columns:
            columns.remove("title")
        columns.insert(0, "title")
        return columns

    def prepare_cols(self, cols):
        cols = self.column_order(cols)
        return [self.cats[col] for col in cols]

    def data_widths(self, data):
        return list(map(max, zip(*iter(map(len, row) for row in data))))

    def cols_widths(self, cols):
        return list(map(len, cols))

    def table_widths(self, cols_widths, data_widths):
        return list(map(max, zip(cols_widths, data_widths)))

    def check_display(self, table_width):
        return table_width <= self.terminal_width

    def column_width(self, n_cols):
        return self.terminal_width // n_cols - self.margin * 2 + 2

    def folding_bool_map(self, rows_widths, cols_widths):
        table_widths = self.table_widths(cols_widths, rows_widths)
        cols_map = list(map(lambda x: x[0] == x[1], zip(cols_widths, table_widths)))
        return cols_map

    def unfolded_rows(self, rows, table_widths):
        added_margins = self.add_margins(rows, table_widths)
        added_vertlines = self.add_vertlines(added_margins)
        horizontal = self.hzline(table_widths, margin=True)
        ready_rows = [iadd(vertline, horizontal) for vertline in added_vertlines]
        all_ = "\n".join(ready_rows)
        return all_

    def create_unfolded_table(self, rows, cols, table_widths):
        data = self.unfolded_rows(rows, table_widths)
        all_ = "\n".join([cols, data])
        return all_

    def create_folded_table(self, rows, cols, table_widths):
        data, table_widths = self.fold_rows(rows, table_widths)
        cols = self.fold_columns(cols, table_widths)[0]
        all_ = "\n".join([cols, data])
        return all_

    def mask(self, data_map, bool_map):
        return [data if bool_map[i] else 0 for i, data in enumerate(data_map)]

    def fold(self, rows, cols):
        cols_widths, rows_widths = self.cols_widths(cols), self.data_widths(rows)
        cols_str, table_widths = self.fold_columns(cols, rows_widths)
        table_width = sum(table_widths) + len(cols) + 1 + len(cols) * self.margin
        if self.check_display(table_width):
            return self.create_unfolded_table(rows, cols_str, table_widths)
        cols_map = self.folding_bool_map(rows_widths, cols_widths)
        table_widths = self.mask(table_widths, cols_map)
        return self.create_folded_table(rows, cols, table_widths)

    def fold_columns(self, cols, rows_widths):
        zipped = list(
            zip_longest(
                *iter(limsplit(col, rows_widths[i], " ") for i, col in enumerate(cols)),
                fillvalue="",
            )
        )
        cols_widths = self.data_widths(zipped)
        widths = self.table_widths(cols_widths, rows_widths)
        added_margins = self.add_margins(zipped, widths)
        added_vertlines = self.add_vertlines(added_margins)
        horizontal = self.hzline(widths, margin=True, sym="#")
        topline = self.topline(widths, sym="#")
        joined = "\n".join(added_vertlines)
        top_bottom = "".join([topline, joined, horizontal])
        return top_bottom, widths

    def folded_printer(self, rows, table_widths):
        added_margins = [
            self.add_margins(chain(row), table_widths) for row in chain(rows)
        ]
        added_vertlines = [self.add_vertlines(row) for row in chain(added_margins)]
        horizontal = self.hzline(table_widths, margin=True)
        ready_rows = [
            iadd("\n".join(row), horizontal) for row in chain(added_vertlines)
        ]
        return "\n".join(ready_rows)

    def display_interactive(self, data, columns):
        """Enter: next frame.
        """
        for i, frame in enumerate(self.rows_to_frames(data)):
            frame_str = self.fold(frame, columns)
            print(frame_str)
            ui = input(
                f"End of page {i+1}.\nPress any key to display next page...q for quit\n"
            )
            if ui in ["q", "quit", "exit", "end", "stop", "finish"]:
                return "Exit"
        return "End of pages"

    def rows_to_frames(self, rows):
        frame = []
        for i, row in enumerate(rows):
            frame.append(row)
            if not (i + 1) % self.rows_pp:
                yield frame
                frame.clear()

    def fold_rows(self, rows, cols_widths):
        split_ = self._splitter(rows, cols_widths, self.column_width(len(cols_widths)))
        row_widths_ = self._widests(split_)
        table_widths = self.table_widths(row_widths_, cols_widths)
        width_sum = (
            sum(table_widths) + len(row_widths_) * self.margin + len(row_widths_) + 1
        )
        if width_sum <= self.terminal_width:
            return self.folded_printer(split_, table_widths), table_widths
        raise ValueError(
            "Can't display all the columns without losing readability.\nPlease choose less columns to sort by, use wider terminal or consider adding export to CSV file feature."
        )

    def add_margins(self, rows, row_widths):
        return [
            [v.center(row_widths[i] + self.margin) for i, v in enumerate(row)]
            for row in rows
        ]

    def add_vertlines(self, rows):
        return ["".join(["|", "|".join(row), "|"]) for row in rows]

    def topline(self, row_widths, sym=None):
        if not sym:
            sym = self.line_sym
        row_widths = map(lambda r: r + self.margin, row_widths)
        topline = "".join(
            ["|", "|".join([sym * col_width for col_width in row_widths]), "|"]
        )
        return iadd(topline, "\n")

    def hzline(self, row_widths, margin=False, sym=None):
        if not sym:
            sym = self.line_sym
        if margin:
            row_widths = map(lambda r: r + self.margin, row_widths)
        horizontal = "".join(
            ["|", "|".join([sym * col_width for col_width in row_widths]), "|"]
        )
        return iadd("\n", horizontal)

    def _widests(self, data):
        widths = list(iter(list(map(len, row)) for row in chain(*data)))
        return list(map(max, zip(*widths)))

    def _splitter(self, data, cols_widths, col_width):
        split = [
            map(
                lambda val: limsplit(
                    val, lim=min(col_width, self.default_col_width), splitter=" "
                ),
                row,
            )
            for row in data
        ]

        return list(map(lambda row: list(zip_longest(*row, fillvalue="")), split))

    def display(self, data, columns=None, index_col=False):
        if index_col:
            return self.print_highscores(data)
        cols = self.prepare_cols(columns)
        if len(data) > max(self.rows_pp, 20):
            return self.display_interactive(data, cols)
        return self.fold(data, cols)

    def print_highscores(self, data):
        data = self._flatten(data)
        cols = ["Category", "Movie", "Value"]
        cats = [
            "Runtime",
            "Box Office",
            "Awards Won",
            "Nominations",
            "Oscars",
            "IMDB Rating",
        ]
        data = list(map(list, data))
        for i in range(len(data)):
            data[i].insert(0, cats[i])
        return self.fold(data, cols)
