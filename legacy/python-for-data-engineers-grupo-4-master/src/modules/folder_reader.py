from os import listdir
from os.path import isfile

class FolderReader:

    @staticmethod
    def get_filepaths_from_folder(folder_path: str) -> list:
        filenames = [path for path in listdir(folder_path) if isfile(f"{folder_path}/{path}")]
        filepaths = [f"{folder_path}/{path}" for path in filenames]
        return filepaths

    def __init__(self) -> None:
        raise NotImplementedError()
