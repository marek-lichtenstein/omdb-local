import sys
import argparse
from movies.utils import Commander as cmd

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A db api statistics script.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--sort_by",
        metavar="str",
        help="Provide name of columns to sort by.",
        nargs="+"
    )
    mode.add_argument(
        "--filter_by",
        metavar="str",
        help="Provide a category to filter by.",
        nargs="+",
    )
    mode.add_argument(
        "--compare",
        metavar="str",
        help="Provide a compare category and movies titles",
        nargs=3,
    )
    mode.add_argument(
        "--add",
        metavar="str",
        help="Provide a movie title to add to database.",
        nargs="?",
    )
    mode.add_argument(
        "--highscores",
        help="Provide a list of highscores category",
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
