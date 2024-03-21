#%% Imports 
from os import getenv as env
from typing import Literal

from dotenv import load_dotenv
import pandas as pd 
from pandas import DataFrame, Series

from internal_libs.glue_helper import get_args

load_dotenv()

ARGS = get_args({
    "AWS_S3_SOURCE_PATH": env("AWS_S3_SOURCE_PATH"),
    "AWS_S3_WORK_PATH": env("AWS_S3_WORK_PATH")
})


#%% Computes functions 

class Tranformations:
    @staticmethod
    def generate_key_with_journal_name(series: Series):
        """
        Given a string column, it creates a key identifying the same textual content
        
        Args:
            series (str): The string column to be transformed.
            
        Returns:
            Series: A series with the key generated.
        """
        
        return (
            series
                .str.replace("&", "AND") 
                .str.replace(r"([^A-Za-z0-9]+)", "") 
                .str.upper() 
                .str.strip()
        )
    
    @staticmethod
    def parse_bibtex_to_dataframe(filepath: str):
        """
        Read a bibtex file and returns a dataframe with the content of the file.
        This function is used to read the bibtex files in parallel in "compute_raw_bibtex" function.
        
        Args:
            filepath (str): The path of the bibtex file to be read.
            
        Returns:
            DataFrame: A dataframe with the content of the bibtex file.
        """
        
        import bibtexparser
        
        file = open(filepath, 'r', encoding="utf-8")
        bib = bibtexparser.load(file)
        df = pd.DataFrame(bib.entries)
        file.close()
        return df
    
def compute_raw_bibtex(source_path: str, type: Literal['acm', 'ieee', 'science_direct']):
    """ 
    Reads the bibtex files and returns a dataframe with the content of the files.
    
    Args:
        source_path (str): The path where the bibtex files are located.
        type (Literal['acm', 'ieee', 'science_direct']): The type of the bibtex files to read.
    
    Returns:
        DataFrame: A dataframe with the content of the bibtex files.
    """
    
    from glob import glob
    from multiprocessing import Pool
    from os import cpu_count
    
    filepaths = glob(f'{source_path}/{type}/*.bib')
    processes_quantity = cpu_count()
    
    with Pool(processes_quantity) as pool:
        dataframes = pool.map(Tranformations.parse_bibtex_to_dataframe, filepaths)
        return pd.concat(dataframes)
    
def compute_acm_dataframe(df_raw_bibtex: DataFrame):
    """
    Transforms the raw ACM bibtex dataframe into a dataframe with the Bibtex final format.
    
    Args:
        df_raw_bibtex (DataFrame): The raw bibtex dataframe.
        
    Returns:
        DataFrame: A dataframe with the Bibtex final format.
    """
    
    df_acm = df_raw_bibtex.convert_dtypes()
    df_acm = df_acm[["author", "title", "keywords", "abstract", "year", "ENTRYTYPE", "doi", "issn", "isbn", "journal", "url"]]
    df_acm = df_acm.rename(columns={"ENTRYTYPE": "type_publication"})
    df_acm["issn"] = df_acm["issn"].mask(~df_acm["issn"].isnull(), df_acm["issn"].str.replace("-", ""))
    df_acm["source"] = "acm"
    
    return df_acm

def compute_ieee_dataframe(df_raw_bibtex: DataFrame):
    """
    Transforms the raw IEEE bibtex dataframe into a dataframe with the Bibtex final format.
    
    Args:
        df_raw_bibtex (DataFrame): The raw bibtex dataframe.
        
    Returns:
        DataFrame: A dataframe with the Bibtex final format.
    """
    
    df_ieee = df_raw_bibtex.convert_dtypes()
    df_ieee = df_ieee[["author", "title", "keywords", "abstract", "year", "ENTRYTYPE", "doi", "issn", "journal"]]
    df_ieee = df_ieee.rename(columns={"ENTRYTYPE": "type_publication"})
    df_ieee["url"] = pd.NA
    df_ieee["isbn"] = pd.NA
    df_ieee["issn"] = df_ieee["issn"].mask(~df_ieee["issn"].isnull(), df_ieee["issn"].str.replace("-", ""))
    df_ieee["source"] = "ieee"
    
    return df_ieee

def compute_sd_dataframe(df_raw_bibtex: DataFrame):
    """
    Transforms the raw Science Direct bibtex dataframe into a dataframe with the Bibtex final format.
    
    Args:
        df_raw_bibtex (DataFrame): The raw bibtex dataframe.
        
    Returns:
        DataFrame: A dataframe with the Bibtex final format.
    """
    
    df_sd = df_raw_bibtex.convert_dtypes()
    df_sd = df_sd[["author", "title", "keywords", "abstract", "year", "ENTRYTYPE", "doi", "issn", "isbn", "journal", "url"]]
    df_sd = df_sd.rename(columns={"ENTRYTYPE": "type_publication"})
    df_sd["issn"] = df_sd["issn"].mask(~df_sd["issn"].isnull(), df_sd["issn"].str.replace("-", ""))
    df_sd["source"] = "science direct"
    
    return df_sd

def compute_bibtex_dataframe(df_acm: DataFrame, df_ieee: DataFrame, df_sd: DataFrame):
    """
    Concatenates the ACM, IEEE and Science Direct dataframes into a single dataframe and make some transformations.
    
    Args:
        df_acm (DataFrame): The ACM dataframe.
        df_ieee (DataFrame): The IEEE dataframe.
        df_sd (DataFrame): The Science Direct dataframe.
        
    Returns:
        DataFrame: A dataframe with the Bibtex final format.
    """
    
    df_bibtex = pd.concat([df_acm, df_ieee, df_sd])
    df_bibtex = df_bibtex.convert_dtypes()
    df_bibtex = df_bibtex.drop_duplicates()
    df_bibtex["journal_title_key"] = df_bibtex["journal"].pipe(Tranformations.generate_key_with_journal_name)
    
    return df_bibtex

def compute_jcs_dataframe(source_path: str):
    """
    Computes the Journal Citation Score (JCS) dataframe.
    
    Args:
        source_path (str): The path where the JCS file is located.
        
    Returns:
        DataFrame: A dataframe with the JCS values.
    """
    
    FILEPATH = f'{source_path}/jcs_2020.csv'
    
    df = pd.read_csv(FILEPATH, header=0, encoding="utf-8", sep=';', low_memory=False)
    df = df[["Full Journal Title", "Journal Impact Factor"]]
    df = df.rename(columns={"Full Journal Title": "title", "Journal Impact Factor": "jcs_value"})
    df["journal_title_key"] = df["title"].pipe(Tranformations.generate_key_with_journal_name) 
    return df
    
def compute_scimago_dataframe(source_path: str):
    """
    Computes the Scimago Journal & Country Rank (SJR) dataframe.
    
    Args:
        source_path (str): The path where the SJR file is located.
        
    Returns:
        DataFrame: A dataframe with the SJR values.
    """
    
    FILEPATH = f'{source_path}/scimagojr 2020.csv'
    
    df = pd.read_csv(FILEPATH, header=0, encoding="utf-8", sep=';', low_memory=False)
    df = df.convert_dtypes()
    df = df[['Issn', 'Title', 'SJR']]
    df = df.rename(columns={'SJR': 'scimago_value', 'Issn': 'issn', 'Title': 'title'})
    df["journal_title_key"] = df["title"].pipe(Tranformations.generate_key_with_journal_name)
    return df

def compute_journal_dataframe(df_jcs: DataFrame, df_scimago: DataFrame):
    """
    Merge the JCS and SJR dataframes into a single dataframe and make some transformations.
    
    Args:
        df_jcs (DataFrame): The JCS dataframe.
        df_scimago (DataFrame): The SJR dataframe.
        
    Returns:
        DataFrame: A dataframe with the JCS and SJR values.
    """
    
    df_journal = pd.merge(left=df_scimago, right=df_jcs, left_on=["journal_title_key"], right_on=["journal_title_key"], how="outer") 
    
    df_journal["title"] = df_journal["title_x"].mask(pd.isnull(df_journal["title_x"]), df_journal["title_y"])

    df_journal = df_journal[['title', "issn", "journal_title_key", "scimago_value", "jcs_value"]]
    df_journal = df_journal.rename(columns={"issn": "issn_journal"})

    df_journal["issn_journal"] = df_journal["issn_journal"].mask(pd.isnull(df_journal["issn_journal"]), '-')

    df_journal["upper_title"] = df_journal["title"].str.upper().str.strip()

    df_journal = df_journal.drop_duplicates()

    df_regex_groups = df_journal['issn_journal'].str.split(pat=",", n=3, expand=True)
    df_journal['issn_1'] = df_regex_groups.loc[:, 0]
    df_journal['issn_2'] = df_regex_groups.loc[:, 1]
    df_journal['issn_3'] = df_regex_groups.loc[:, 2]

    df_journal['issn_1'] = df_journal['issn_1'].mask(df_journal["issn_1"] == '-') #Caso issn seja = '-', coloca nulo no campo
    df_journal['issn_2'] = df_journal['issn_2'].mask(df_journal["issn_2"] == '-') #Caso issn seja = '-', coloca nulo no campo
    df_journal['issn_3'] = df_journal['issn_3'].mask(df_journal["issn_3"] == '-') #Caso issn seja = '-', coloca nulo no campo

    df_journal = df_journal.drop_duplicates()
    
    return df_journal

def compute_final_dataframe(df_bibtex: DataFrame, df_journal: DataFrame):
    """
    Joins the Bibtex and Journal dataframes into a single dataframe and make some transformations.
    
    Args:
        df_bibtex (DataFrame): The Bibtex dataframe.
        df_journal (DataFrame): The Journal dataframe.
        
    Returns:
        DataFrame: A dataframe with the final dataset.
    """
    
    import duckdb

    sql = """
      SELECT DISTINCT
            bib.author
            ,bib.title
            ,bib.keywords
            ,bib.abstract
            ,bib.year
            ,bib.type_publication
            ,bib.doi
            ,bib.issn
            ,COALESCE(bib.journal, jor.title) journal
            ,bib.source
            ,jor.scimago_value
            ,jor.jcs_value
            ,bib.url
      FROM 
            df_bibtex bib
            LEFT JOIN df_journal jor
                  ON (bib.issn = jor.issn_1
                  OR bib.issn = jor.issn_2
                  OR bib.issn = jor.issn_3
                  OR bib.journal_title_key = jor.journal_title_key)
                  AND NOT (bib.issn is null and bib.journal is null);
                  
    """
    
    df = duckdb.sql(sql).df()
    df = df.drop_duplicates(subset=["title", "year"])
    
    return df

def load_final_dataframe(df: DataFrame, output_path: str):
    """
    Loads the final dataframe into a parquet file.
    
    Args:
        df (DataFrame): The final dataframe.
        output_path (str): The path where the parquet file will be saved.
        
    Returns:
        None
    """
    
    FILEPATH = f'{output_path}/final_dataset.parquet'
    df.to_parquet(FILEPATH)


#%% Job execution 
if __name__ == "__main__":
    """Main execution of the job"""
    
    df_raw_acm = compute_raw_bibtex(ARGS['AWS_S3_SOURCE_PATH'], 'acm')
    df_raw_ieee = compute_raw_bibtex(ARGS['AWS_S3_SOURCE_PATH'], 'ieee')
    df_raw_sd = compute_raw_bibtex(ARGS['AWS_S3_SOURCE_PATH'], 'science_direct')

    df_acm = compute_acm_dataframe(df_raw_acm)
    df_ieee = compute_ieee_dataframe(df_raw_ieee)
    df_sd = compute_sd_dataframe(df_raw_sd)

    df_bibtex = compute_bibtex_dataframe(df_acm, df_ieee, df_sd)
    
    df_jcs = compute_jcs_dataframe(ARGS['AWS_S3_SOURCE_PATH'])
    df_scimago = compute_scimago_dataframe(ARGS['AWS_S3_SOURCE_PATH'])
    
    df_journal = compute_journal_dataframe(df_jcs, df_scimago)
    
    df = compute_final_dataframe(df_bibtex, df_journal)
    
    load_final_dataframe(df, ARGS['AWS_S3_WORK_PATH'])

