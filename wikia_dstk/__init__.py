import os

def chrono_sort(directory):
    """Return a list of files in a directory, sorted chronologically"""
    files = [(os.path.join(directory, filename), os.path.getmtime(os.path.join(directory, filename))) for filename in os.listdir(directory)]
    files.sort(key=lambda x: x[1])
    return files
