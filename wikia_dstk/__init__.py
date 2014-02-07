import os

def chrono_sort(directory):
    """Return a list of files in a directory, sorted chronologically"""
    files = [(os.path.join(directory, filename), os.path.getmtime(os.path.join(directory, filename))) for filename in os.listdir(directory)]
    files.sort(key=lambda x: x[1])
    return files

def ensure_dir_exists(directory):
    """
    Makes sure the directory given as an argument exists, and returns the same
    directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory
