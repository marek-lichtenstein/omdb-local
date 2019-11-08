import re


def register_functions(con):
    """Register functions in SQLite."""
    for func in FUNCMAP:
        con.create_function(func, FUNCMAP[func][0], FUNCMAP[func][1])

def nominations(sentence):
    """Return the number of nominations, excluding Oscars."""
    if not sentence:
        return None
    if sentence.find("nomination") < 0:
        return 0
    match = re.search("(?P<non_osc_nom>\d+) nomination", sentence)
    return int(match.groupdict()["non_osc_nom"]) if match else 0


def awards_won(sentence):
    """Return all won awards, excluding oscars."""
    if not sentence:
        return None
    if sentence and sentence.find("win") < 0:
        return 0
    match = re.search("(?P<non_osc_win>\d+) win", sentence)
    return int(match.groupdict()["non_osc_win"]) if match else None


def won80nom(sentence):
    if not sentence:
        return None
    """Checks if ratio of awards to nominations is more or equal to 0.8."""
    if (sentence.find("nomination") or sentence.find("win")) < 0:
        return False
    regex = "(?P<non_osc_win>\d+) win|(?P<non_osc_nom>\d+) nomination"
    matches = list(re.finditer(regex, sentence))
    if len(matches) == 2:
        non_osc_win = matches[0].groupdict()["non_osc_win"]
        non_osc_nom = matches[1].groupdict()["non_osc_nom"]
        ratio = int(non_osc_win) / int(non_osc_nom)
        return sentence if ratio > 0.8 else False
    return False


def has_osc_nom(sentence):
    if not sentence:
        return False
    if sentence.find("Oscar") < 0:
        return False
    match = re.search("Nominated for (?P<osc_nom>\d+) Oscar", sentence)
    return sentence if match else False


def oscars_nom(sentence):
    """Return the number of Oscar nominations."""
    if not sentence:
        return None
    if sentence.find("Oscar") < 0:
        return 0
    match = re.search("Nominated for (?P<osc_nom>\d+) Oscar", sentence)
    return int(match.groupdict()["osc_nom"]) if match else 0


def oscars_won(sentence):
    """Return the number of won Oscar awards."""
    if not sentence:
        return None
    if sentence.find("Oscar") < 0:
        return 0
    match = re.search("Won (?P<osc_won>\d+) Oscar", sentence)
    return int(match.groupdict()["osc_won"]) if match else 0


def person_check(persons, person):
    if not persons:
        return False
    return (
        persons
        if any(
            (
                map(
                    lambda p: p if person.lower() == p.lstrip(" ").lower() else False,
                    persons.split(","),
                )
            )
        )
        else False
    )


def language_check(languages, language):
    """Checks if language in languages list."""
    if not languages:
        return None
    return (
        languages
        if any(
            map(
                lambda lang: language.lower() == lang.lstrip(" ").lower(),
                languages.split(","),
            )
        )
        else "Lemerig"
    )


def format_runtime(runtime):
    """Format runtime."""
    if not runtime:
        return "N/A"
    if isinstance(runtime, str):
        clean = clean_dirty_digit_s(runtime)
        if not clean:
            return "N/A"

        return f"{clean//60}h{clean%60}min"
    return f"{runtime//60}h{runtime%60}min"


def clean_dirty_digit_s(dirty_digit_s):
    """Convert string containing non-digit chars to integral."""
    if not dirty_digit_s:
        return None
    clean_digit_s = "".join(filter(str.isdigit, dirty_digit_s))
    return int(clean_digit_s) if clean_digit_s else None


def int_to_account(value):
    return "$" + format(value, ",") if value else value


def int_to_comas(value):
    return format(value, ",") if value else value

def _int(value):
    return int(value) if value else value

def _str(value):
    return str(value) if value else "N/A"


FUNCMAP = {
    "str": (1, str),
    "float": (1, float),
    "_int": (1, _int),
    "_str": (1, _str),
    "int": (1, int),
    "nominations": (1, nominations),
    "awards_won": (1, awards_won),
    "won_80_nom": (1, won80nom),
    "osc_won": (1, oscars_won),
    "osc_nom": (1, oscars_nom),
    "has_person": (2, person_check),
    "has_language": (2, language_check),
    "clnstr": (1, clean_dirty_digit_s),
    "tform": (1, format_runtime),
    "int_to_account": (1, int_to_account),
    "int_to_comas": (1, int_to_comas),
    "has_osc_nom": (1, has_osc_nom),
}
