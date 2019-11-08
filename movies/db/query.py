from operator import iadd
from movies.conf import DATA_MAP

FILTER = {
    "director": ("DIRECTOR", "DIRECTOR=has_person(DIRECTOR, ?)"),
    "actor": ("CAST", '"CAST"=has_person("CAST", ?)'),
    "eighty": ("AWARDS", "AWARDS=won_80_nom(AWARDS)"),
    "oscar_nom": ("AWARDS", "AWARDS=has_osc_nom(AWARDS)"),
    "boxoffice": ("BOX_OFFICE", "BOX_OFFICE>100000000"),
    "language": ("LANGUAGE", "LANGUAGE=has_language(LANGUAGE, ?)"),
}

COMPARE = {
    "imdb": "str(MAX(IMDb_Rating))",
    "boxoffice": "int_to_account(MAX(BOX_OFFICE))",
    "awards": "str(MAX(awards_won(AWARDS)))",
    "runtime": "tform(MAX(clnstr(RUNTIME)))",
}

SORT = {
    "title": ('"TITLE"', '"TITLE"'),
    "year": ('_str("YEAR")', '_int("YEAR")'),
    "runtime": ('tform("RUNTIME")', 'clnstr("RUNTIME")'),
    "genre": ('"GENRE"', '"GENRE"'),
    "director": ('"DIRECTOR"', '"DIRECTOR"'),
    "actors": ('"CAST"', '"CAST"'),
    "writer": ('"WRITER"', '"WRITER"'),
    "language": ('"LANGUAGE"', '"LANGUAGE"'),
    "country": ("COUNTRY", "COUNTRY"),
    "awards": ('_str(awards_won("AWARDS"))', "awards_won(AWARDS)"),
    "rating": ('_str("IMDb_Rating"', "IMDb_Rating"),
    "votes": ('int_to_comas("IMDb_votes")', "_int(IMDb_votes)"),
    "boxoffice": ('int_to_account("BOX_OFFICE")', "_int(BOX_OFFICE)"),
}


SELECT_CONV = {
    "year": "str",
    "runtime": "tform",
    "imdb_rating": "str",
    "imdb_votes": "int_to_comas",
    "box_office": "int_to_account",
}

INSERT_CONV = {
    "year": "clnstr",
    "imdb_rating": "float",
    "imdb_votes": "clnstr",
    "box_office": "clnstr",
}

HIGHSCORES = [
    ("tform(RUNTIME)", "clnstr(RUNTIME)"),
    ("int_to_account(BOX_OFFICE)", "_int(BOX_OFFICE)"),
    ("_str(awards_won(AWARDS))", "awards_won(AWARDS)"),
    ("_str(nominations(AWARDS))", "nominations(AWARDS)"),
    ("_str(osc_won(AWARDS))", "osc_won(AWARDS)"),
    ("_str(IMDb_Rating)", "IMDB_RATING"),
]

QUERY = {
    "filter": """SELECT TITLE, {} FROM MOVIES WHERE {};""",
    "sort": """SELECT TITLE{} FROM MOVIES ORDER BY {};""",
    "highscores": """SELECT TITLE, {} FROM MOVIES ORDER BY {} DESC LIMIT 1;""",
    "insert": """INSERT INTO MOVIES ({}) VALUES ({});""",
    "update": """UPDATE MOVIES SET {} WHERE TITLE=?;""",
    "select": """SELECT {} FROM MOVIES WHERE TITLE=?;""",
    "compare": """ SELECT TITLE, {} FROM MOVIES WHERE TITLE IN (?,?)""",
}

DATA_MAP_VALUES = list(DATA_MAP.values())


def select():
    names = ", ".join(map(_select_coat, DATA_MAP_VALUES))
    return QUERY["select"].format(names)


def update():
    names = ", ".join(map(_update_coat, DATA_MAP_VALUES[1:]))
    return QUERY["update"].format(names)


def insert():
    return QUERY["insert"].format(
        ", ".join(DATA_MAP_VALUES), ", ".join(map(_insert_coat, DATA_MAP_VALUES))
    )


def sort(*args):
    try: 
        return QUERY["sort"].format(
            _sort_cols(*args), ", ".join([f"{SORT[arg][1]} DESC" for arg in args])
        )
    except KeyError as err:
        raise ValueError(f"You can't sort by that column: {err.args[0]}.")
        


def filter_(col):
    try:
        return QUERY["filter"].format(_select_coat(FILTER[col][0]), FILTER[col][1])
    except KeyError as err:
        raise ValueError(f"You can't filter with that column: {err.args[0]}.")


def compare(col):
    try:
        return QUERY["compare"].format(COMPARE[col])
    except KeyError as err:
        raise ValueError(f"You can't compare with that: {err.args[0]}.")

def highscores():
    return [QUERY["highscores"].format(sel_col, fcol) for sel_col, fcol in HIGHSCORES]


def _insert_coat(col):
    return f"{INSERT_CONV[col.lower()]}(?)" if col.lower() in INSERT_CONV else "?"


def _select_coat(col):
    return (
        f'ifnull({SELECT_CONV[col.lower()]}("{col}"), "N/A")'
        if col.lower() in SELECT_CONV
        else f'ifnull("{col}", "N/A")'
    )


def _sort_coat(col):
    return f'ifnull({SORT[col][0]}, "N/A")'


def _update_coat(col):
    return (
        f'"{col}"={INSERT_CONV[col.lower()]}(?)'
        if col.lower() in INSERT_CONV
        else f'"{col}"=?'
    )


def _sel_cols(*args):
    args = list(args)
    if "title" in args:
        args.remove("title")
    if args:
        return iadd(", ", ", ".join([_select_coat(arg) for arg in args]))
    return ""


def _sort_cols(*args):
    args = list(args)
    if "title" in args:
        args.remove("title")
    if args:
        return iadd(", ", ", ".join([f'ifnull({SORT[arg][0]}, "N/A")' for arg in args]))
    return ""
