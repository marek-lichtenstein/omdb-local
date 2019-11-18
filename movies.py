import sys
import argparse
from movies.utils import Commander as cmd

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A db api statistics script.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--sort_by",
        metavar="str",
        help= "Sorts data by selected columns. Choose any combination from: [ Title ] / sort_by --title / [ Year ] / sort_by --year / [ Runtime ] / --sort_by runtime / [ Genre ] / --sort_by genre / [ Director ] / --sort_by director / [ Actors ] / --sort_by actors / [ Writer ] / --sort_by writer / [ Language ] / --sort_by language / [ Country ] / --sort_by country / [ Awards ] / --sort_by awards / [ IMDb Rating ] / --sort_by rating / [ IMDb Votes ] / --sort_by votes / [ Box office ] / --sort_by boxoffice / [ To sort by a few categories i.e. ] / sort_by runtime boxoffice rating /",
        nargs="+",
    )
    mode.add_argument(
        "--filter_by",
        metavar="str",
        help= "Filters data by a category. [ Director ] / --filter_by director <full name> / [ Actor ] / --filter_by actor <full name> / [ Movies that were nominated for Oscar but did not win any ] / --filter_by oscar_nom / [ Movies that won more than 80%% of nominations / --filter_by eighty / [ Only movies in certain language ] / --filter_by language <language> / [ Movies that earned more than $100,000,000 ] / --filter_by boxoffice /",
        nargs="+",
    )
    mode.add_argument(
        "--compare",
        metavar="str",
        help= "Compare two movies in selected category. [ IMDb Rating ] / --compare imdb movie_1 movie_2 / [ Box Office ] / --compare boxoffice movie_1 movie_2 / [ Number of awards won ] / --compare awards movie_1 movie_2 / [ Runtime ] / --compare runtime movie_1 movie_2 /",
        nargs=3,
    )
    mode.add_argument(
        "--add",
        metavar="str",
        help="[ Add a movie to a database. ] / --add movie_title /",
        nargs="?",
    )
    mode.add_argument(
        "--highscores",
        help="[ Provide a list of highscores in runtime, box office, awards won, nominations, oscars won, IMDB rating ] / --highscores /",
        action="store_true",
    )
    args = parser.parse_args()

    if args.sort_by:
        print(cmd().sort_by(*args.sort_by))
        sys.exit(1)
    elif args.filter_by:
        print(cmd().filter_by(args.filter_by))
        sys.exit(1)

    elif args.compare:
        print(cmd().compare(*args.compare))
        sys.exit(1)
    elif args.add:
        print(cmd().add_movie(args.add))
        sys.exit(1)
    elif args.highscores:
        print(cmd().highscores())
        sys.exit(1)
    else:
        print("Please choose mode to run in.")
        sys.exit(1)
