from os import path, makedirs, walk

def file_paths(folder):
    if path.isfile(folder):
        return [folder]

    files = []

    for dirpath,_,filenames in walk(folder):
       for f in filenames:
           files.append(path.abspath(path.join(dirpath, f)))

    files.sort()
    return files