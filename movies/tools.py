from itertools import chain

def limsplit(strg, lim, splitter):
    """It takes a string and it splits it from left to right, with kept order, and with parts not exceeding length limit but as close to it as possible.
    limsplit("John, Mary, Tom, Susan", 10, ", ") => ["John, Mary", "Tom, Susan"]
    """
    splits = [[]]
    for split in strg.split(splitter):
        if sum(map(len, splits[-1])) + len(split) <= lim:
            splits[-1].append(split)
        else:
            splits.append([split]) 
    return list(map(splitter.join, chain(splits if splits[0] else splits[1:])))

def wrapper(func, statement):
    return f"{func}({statement})"

def multiwrapper(*args, statement):
    return mutliwrapper(*args[1:], statement=wrapper(args[0], statement)) if args else statement
