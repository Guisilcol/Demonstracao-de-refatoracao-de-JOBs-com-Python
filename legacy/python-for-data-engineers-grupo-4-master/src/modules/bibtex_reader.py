from bibtexparser import load as load_bibtex
import pandas as pd

class BibtexReader:

    @staticmethod
    def read_files_to_dataframe(filepaths: list):
        files = (open(path, encoding="utf-8") for path in filepaths)  # type: ignore
        bibtexts = (load_bibtex(file) for file in files)
        dataframe_list = (pd.DataFrame(bib.entries) for bib in bibtexts)
        return pd.concat(dataframe_list)
        
    def __init__(self):
        raise NotImplementedError()
    