import os
import json
import unittest
from operator import itemgetter
from itertools import zip_longest

from movies.db.sqlite_extensions import FUNCMAP, format_runtime
import movies.db.dbm as dbm
import movies.db.query as query
import movies.requester as req
from movies.conf import DATA_MAP


class TestSQLiteCustomFunctions(unittest.TestCase):
    def setUp(self):
        self.statements = [
            "Won 7 Oscars. Another 82 wins & 49 nominations.",
            "Won 6 Oscars. Another 40 wins & 67 nominations.",
            "Nominated for 1 Oscar. Another 10 wins & 34 nominations.",
            "Nominated for 7 Oscars. Another 19 wins & 32 nominations.",
            "1 win & 1 nomination.",
            "8 wins & 10 nominations.",
            "Nominated for 2 Golden Globes. Another 10 wins & 27 nominations.",
            "",
        ]

    def test_assert_oscars_nominations(self):
        zipped_results = zip([0, 0, 1, 7, 0, 0, 0, None], self.statements)
        for expected_result, statement in zipped_results:
            self.assertEqual(
                expected_result,
                FUNCMAP["osc_nom"][1](statement),
                msg=f"{statement}: {expected_result}",
            )

    def test_assert_oscars_won(self):
        zipped_results = zip([7, 6, 0, 0, 0, 0, 0, None], self.statements)
        for expected_result, statement in zipped_results:
            self.assertEqual(
                expected_result,
                FUNCMAP["osc_won"][1](statement),
                msg=f"{statement}: {expected_result}",
            )

    def test_assert_nominations(self):
        zipped_results = zip([49, 67, 34, 32, 1, 10, 27, None], self.statements)
        for expected_result, statement in zipped_results:
            self.assertEqual(
                expected_result,
                FUNCMAP["nominations"][1](statement),
                msg=f"{statement}: {expected_result}",
            )

    def test_assert_awards_won(self):
        zipped_results = zip([82, 40, 10, 19, 1, 8, 10, None], self.statements)
        for expected_result, statement in zipped_results:
            self.assertEqual(
                expected_result,
                FUNCMAP["awards_won"][1](statement),
                msg=f"{statement}: {expected_result}",
            )

    def test_assert_80_ratio(self):
        zipped_results = zip(
            [
                "Won 7 Oscars. Another 82 wins & 49 nominations.",
                False,
                False,
                False,
                "1 win & 1 nomination.",
                False,
                False,
                None,
            ],
            self.statements,
        )
        for expected_result, statement in zipped_results:
            self.assertEqual(
                expected_result,
                FUNCMAP["won_80_nom"][1](statement),
                msg=f"{statement}: {expected_result}",
            )

    def test_assert_actors_check(self):
        actors = "Colin Clive, Mae Clarke, John Boles, Boris Karloff"
        user_check = [
            ("colinclive", False),
            ("colin clive", "Colin Clive, Mae Clarke, John Boles, Boris Karloff"),
            ("ColinClive", False),
            ("Olin Clive", False),
            ("Mary Shelley", False),
            ("colin", False),
            ("Mae Clarek", False),
            ("Mae Clarke,", False),
            ("Mae Clarke", "Colin Clive, Mae Clarke, John Boles, Boris Karloff"),
            ("Colin Clive, Mae Clarke", False),
            (actors, False),
        ]
        for statement, expected_result in user_check:
            self.assertEqual(
                expected_result,
                FUNCMAP["has_person"][1](actors, statement),
                msg=f"{statement}: {expected_result}",
            )

    def test_assert_language_check(self):
        languages = "English, French, German, Italian"
        user_check = [
            ("French", "English, French, German, Italian"),
            ("german", "English, French, German, Italian"),
            ("Italia", "Lemerig"),
            ("Russian", "Lemerig"),
            (languages, "Lemerig"),
        ]

        for statement, expected_result in user_check:
            self.assertEqual(
                expected_result,
                FUNCMAP["has_language"][1](languages, statement),
                msg=f"{statement}: {expected_result}",
            )

    def test_assert_cln_ints(self):
        user_check = [("N/A", None), ("$333,..qwf1555,888", 3331555888), ("$444", 444)]
        for statement, expected_result in user_check:
            self.assertEqual(
                expected_result,
                FUNCMAP["clnstr"][1](statement),
                msg=f"{statement}: {expected_result}",
            )

    def test_assert_format_runtime(self):
        user_check = [("N/A", "N/A"), ("90 min", "1h30min"), ("44 min", "0h44min")]
        for statement, expected_result in user_check:
            self.assertEqual(
                expected_result,
                FUNCMAP["tform"][1](statement),
                msg=f"{statement}: {expected_result}",
            )


class TestCredentials(unittest.TestCase):
    def setUp(self):
        self.invalid_creds = {
            "invalid_key_creds.json": {"wrong_dict_key": "#"},
            "empty_value_key_creds.json": {"apikey": None},
            "not_dict_datatype.json": ["invalid datatype"],
            "invalid_creds.json": {"apikey": "invalid_api_key"},
            "empty_creds.json": {},
        }
        for fp in self.invalid_creds:
            with open(fp, "w") as f:
                json.dump(self.invalid_creds[fp], f)

    def tearDown(self):
        for fp in self.invalid_creds:
            os.remove(fp)

    def test_valid_key(self):
        self.assertEqual("9e9e0617", req.Credentials(key="9e9e0617").apikey())

    def test_invalid_key(self):
        self.assertRaises(ValueError, lambda: req.Credentials(key="invalid_key").apikey())

    def test_no_key_nor_file(self):
        self.assertRaises(ValueError, lambda: req.Credentials())

    def test_credentials_file_dont_exists(self):
        self.assertRaises(ValueError, lambda: req.Credentials(creds="file_dont_exists"))

    def test_credentials_file_invalid_apikey(self):
        self.assertRaises(ValueError, lambda: req.Credentials(creds="invalid_creds.json"))

    def test_credentials_file_invalid_key(self):
        self.assertRaises(
            ValueError, lambda: req.Credentials(creds="invalid_key_creds.json")
        )

    def test_empty_credentials_file(self):
        self.assertRaises(
            ValueError, lambda: req.Credentials(creds="empty_value_key_creds.json")
        )

    def test_credentials_file_with_invalid_datatype(self):
        self.assertRaises(
            ValueError, lambda: req.Credentials(creds="not_dict_datatype.json")
        )


class TestRequester(unittest.TestCase):
    def setUp(self):
        self.key = req.Credentials(creds="tests/credentials.json").apikey()

    def test_request_many(self):
        titles = [
            "Batman",
            "Superman",
            "Supergirl",
            "Spiderman",
            "Iron Man",
            "Aquaman",
            "Ant-Man",
        ]
        data_pack = req.Requester(self.key).request_many(titles)
        for data in data_pack:
            self.assertTrue(data.get("Title") in titles)

    def test_request_invalid_title(self):
        self.assertRaises(
            ValueError, lambda: req.Requester(self.key).request("Not a title!")
        )


class TestDataPrep(unittest.TestCase):
    def setUp(self):
        key = req.Credentials(creds="tests/credentials.json").apikey()
        self.row = req.Requester(key).request("Coffee and cigarettes")
        self.rows = req.Requester(key).request_many(["Snatch", "Kiler"])
        self.mapp = DATA_MAP

    def test_converting_single_row(self):
        iter_row = iter(req.row(self.row))
        for key in self.mapp:
            self.assertEqual(next(iter_row), self.row[key], msg=f"{key}")

    def test_converting_multiple_rows(self):
        iter_rows = iter(req.rows(self.rows))
        for row_ in self.rows:
            iter_row = iter(next(iter_rows))
            for key in self.mapp:
                self.assertEqual(next(iter_row), row_[key], msg=f"{key}")

    def test_rotated_row(self):
        keys = list(self.mapp)
        keys.append(keys.pop(0))
        iter_row = iter(req.rotated_row(self.row))
        for key in keys:
            self.assertEqual(next(iter_row), self.row[key], msg=f"{key}")

    def test_rotated_rows(self):
        keys = list(self.mapp)
        keys.append(keys.pop(0))
        iter_rows = iter(req.rotated_rows(self.rows))
        for row_ in self.rows:
            iter_row = iter(next(iter_rows))
            for key in keys:
                self.assertEqual(next(iter_row), row_[key], msg=f"{key}")


class TestDatabaseManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestDatabaseManager, cls).setUpClass()
        cls.titles = dbm.DatabaseManager().get_titles()
        cls.movies_data = req.Downloader("tests/credentials.json").download_many(cls.titles[:5])

    def setUp(self):
        self.test_db = os.system("cp tests/test.db tests/tmp.db")
        self.db_api = dbm.DatabaseManager(tests=True)
        self.update_data = req.rotated_rows(TestDatabaseManager.movies_data)
        self.insert_data = req.rows(TestDatabaseManager.movies_data)
        self.test_data = self.insert_data

    def tearDown(self):
        os.system("rm tests/tmp.db")

    def test_updating_one_row(self):
        self.db_api.insert_one(query.update(), self.update_data[0])
        db_data = self.db_api.select_one(
            query.select(), tuple([self.test_data[0][0]]))
        self.assertEqual(
            db_data,
            [
                tuple(
                    format_runtime(v) if i == 2 else v
                    for i, v in enumerate(self.test_data[0])
                )
            ],
        )

    def test_updating_multiple_rows(self):
        self.db_api.insert_many(query.update(), self.update_data)
        binding = [tuple([row[0]]) for row in self.test_data]
        params = list(zip_longest([query.select()], binding, fillvalue=query.select()))
        db_data = [
            self.db_api.select_one(selq, val) for selq, val in params
        ]
        test_data = [
            [tuple(format_runtime(v) if i == 2 else v for i, v in enumerate(data))]
            for data in self.test_data
        ]

        self.assertEqual(test_data, db_data)

    def test_inserting_one_row(self):
        title = "Deadline Auto Theft"
        movie_data = req.row(req.Downloader("tests/credentials.json").download_one(title))
        self.db_api.insert_one(query.insert(), movie_data)
        test_data = [tuple(format_runtime(v) if i == 2 else v for i, v in enumerate(movie_data))]
        
        db_data = self.db_api.select_one(
            query.select(), tuple([movie_data[0]]))
        self.assertEqual(test_data, db_data)


    def test_inserting_multiple_rows(self):
        titles = ["Deadline Auto Theft", "Braindead", "Planet of the apes"]
        movies_data = req.rows(req.Downloader("tests/credentials.json").download_many(titles))
        self.db_api.insert_many(query.insert(), movies_data)
        binding = [tuple([row[0]]) for row in movies_data]
        params = list(zip_longest([query.select()], binding, fillvalue=query.select()))
        db_data = [
            self.db_api.select_one(selq, val) for selq, val in params
        ]
        movies_data = [
            [tuple(format_runtime(v) if i == 2 else v for i, v in enumerate(data))]
            for data in movies_data
        ]
        self.assertEqual(movies_data, db_data)

    def test_inserting_movie_that_is_in_db(self):
        title = "Fight Club"
        movie_data = req.row(req.Downloader("tests/credentials.json").download_one(title))
        self.assertRaises(
            ValueError,
            lambda: self.db_api.insert_one(query.insert(), movie_data, check=True),
        )

    def test_sorting_by_single_columns(self):
        data = self.db_api.select_one(query.sort("title"))
        titles = [v[0] for v in data]
        self.assertEqual(tuple(titles), tuple(sorted(titles, reverse=True)))

    def test_sorting_by_multiple_columns(self):
        self.db_api.insert_many(query.update(), self.update_data)
        data = self.db_api.select_one(query.sort("year", "title"))
        titles_year = [v[:2] for v in data[:5]]
        years = list(
            zip(
                map(itemgetter(0), titles_year),
                map(
                    lambda x: int(x) if x.isdigit() else None,
                    map(itemgetter(1), titles_year),
                ),
            )
        )
        years = sorted(years, key=itemgetter(1, 0), reverse=True)
        years = list(
            zip(
                map(itemgetter(0), titles_year),
                map(
                    lambda x: "N/A" if x is None else str(x),
                    map(itemgetter(1), titles_year),
                ),
            )
        )
        self.assertEqual(years, titles_year)

    def test_sorting_by_awards(self):
        self.db_api.insert_many(query.update(), self.update_data)
        data = self.db_api.select_one(query.sort("awards"))
        data_int = data[:5]
        test_data_int = sorted(
            list(
                zip(
                    map(itemgetter(0), data_int),
                    map(
                        lambda x: int(x) if x.isdigit() else None,
                        map(itemgetter(1), data_int),
                    ),
                )
            ),
            key=itemgetter(1),
            reverse=True,
        )
        test_data = list(
            zip(
                map(itemgetter(0), test_data_int),
                map(
                    lambda x: "N/A" if x is None else str(x),
                    map(itemgetter(1), test_data_int),
                ),
            )
        )
        self.assertEqual(test_data, data[:5])

    def test_filter_by_director(self):
        self.db_api.insert_many(query.update(), self.update_data)
        data = self.db_api.select_one(
            query.filter_("director"), data=("Lukasz Palkowski",)
        )
        self.assertEqual(data[0][1], "Lukasz Palkowski")

    def test_filter_by_actor(self):
        self.db_api.insert_many(query.update(), self.update_data)
        data = self.db_api.select_one(
            query.filter_("actor"), data=("Szymon Piotr Warszawski",)
        )
        self.assertEqual(
            data,
            [
                (
                    "Gods",
                    "Tomasz Kot, Piotr Glowacki, Szymon Piotr Warszawski, Magdalena Czerwinska",
                )
            ],
        ),

    def test_filter_by_movies_with_oscar_nomination(self):
        self.db_api.insert_many(query.update(), self.update_data)
        data = self.db_api.select_one(query.filter_("oscar_nom"))
        self.assertEqual(
            list(map(itemgetter(0), data)),
            ["The Shawshank Redemption", "Memento", "In Bruges"],
        )

    def test_filter_by_movies_over_80_ratio(self):
        self.db_api.insert_many(query.update(), self.update_data)
        data = self.db_api.select_one(query.filter_("eighty"))
        self.assertEqual(list(map(itemgetter(0), data)), ["Memento", "The Godfather"])

    def test_filter_by_language(self):
        self.db_api.insert_many(query.update(), self.update_data)
        data = self.db_api.select_one(
            query.filter_("language"), data=("polish",)
        )
        self.assertEqual(list(map(itemgetter(0), data)), ["Gods"])

    def test_compare_movie_not_in_db(self):
        self.db_api.insert_many(query.update(), self.update_data)
        self.assertRaises(
            ValueError,
            lambda: self.db_api.select_one(
                query.compare("imdb"),
                data=("The Dogfather", "The Godfather,"),
                check=True,
            ),
        )

    def test_compare_by_imdb_rating(self):
        self.db_api.insert_many(query.update(), self.update_data)
        self.assertEqual(
            [("The Godfather", "9.2")],
            self.db_api.select_one(
                query.compare("imdb"), data=("The Godfather", "Gods")
            ),
        )

    def test_compare_by_boxoffice_earnings(self):
        self.db_api.insert_many(query.update(), self.update_data)
        self.assertEqual(
            [("Memento", "$23,844,220")],
            self.db_api.select_one(
                query.compare("boxoffice"), data=("Memento", "In Bruges")
            ),
        )

    def test_compare_by_awards_won(self):
        self.db_api.insert_many(query.update(), self.update_data)
        self.assertEqual(
            [("The Godfather", "24")],
            self.db_api.select_one(
                query.compare("awards"), data=("The Godfather", "Gods")
            ),
        )

    def test_compare_by_runtime(self):
        self.db_api.insert_many(query.update(), self.update_data)
        self.assertEqual(
            [("The Godfather", "2h55min")],
            self.db_api.select_one(
                query.compare("runtime"), data=("The Godfather", "Gods")
            ),
        )

    def test_compare_when_one_na(self):
        self.db_api.insert_many(query.update(), self.update_data)
        self.assertEqual(
            [("In Bruges", "$7,550,836")],
            self.db_api.select_one(
                query.compare("boxoffice"), data=("In Bruges", "Gods"),
            ),
        )

    def test_highscore_in_runtime(self):
        self.db_api.insert_many(query.update(), self.update_data)
        self.assertEqual(
            [("The Godfather", "2h55min")],
            self.db_api.select_one(query.highscores()[0],),
        )

    def test_highscore_in_earnings(self):
        self.db_api.insert_many(query.update(), self.update_data,)
        self.assertEqual(
            [("Memento", "$23,844,220")],
            self.db_api.select_one(query.highscores()[1],),
        )

    def test_highscore_in_awards(self):
        self.db_api.insert_many(query.update(), self.update_data,)
        self.assertEqual(
            [("Memento", "56")],
            self.db_api.select_one(query.highscores()[2],),
        )

    def test_highscore_in_nominations(self):
        self.db_api.insert_many(query.update(), self.update_data)
        self.assertEqual(
            [("Memento", "55")],
            self.db_api.select_one(query.highscores()[3]),
        )

    def test_highscore_in_oscars(self):
        self.db_api.insert_many(query.update(), self.update_data)
        self.assertEqual(
            [("The Godfather", "3")],
            self.db_api.select_one(query.highscores()[4]),
        )

    def test_highscore_in_imdb_rating(self):
        self.db_api.insert_many(query.update(), self.update_data)
        self.assertEqual(
            [("The Shawshank Redemption", "9.3")],
            self.db_api.select_one(query.highscores()[5]),
        )

    def test_highscores_in_all_categories(self):
        self.db_api.insert_many(query.update(), self.update_data)
        self.assertEqual(
            [
                [("The Godfather", "2h55min")],
                [("Memento", "$23,844,220")],
                [("Memento", "56")],
                [("Memento", "55")],
                [("The Godfather", "3")],
                [("The Shawshank Redemption", "9.3")],
            ],
            self.db_api.select_many(query.highscores()),
        )
