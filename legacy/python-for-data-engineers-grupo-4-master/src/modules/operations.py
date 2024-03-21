from typing import Literal
import pandas as pd 

class Operations: 
    @staticmethod
    def map(df: pd.DataFrame, cols: list):
        """Retorna um dataframe com as colunas informadas"""
        return df[cols]

    @staticmethod
    def apply_to_every_row(df: pd.DataFrame, output_col: str, function):
        """Para cada linha aplica a função informada, jogando o output na informada como parâmetro"""
        df = df.copy()
        df[output_col] = df.apply(function, axis = 1)
        return df

    @staticmethod
    def create_column(df: pd.DataFrame, col_name: str, col_type: str, col_default_value):
        """Cria uma nova coluna no dataframe"""
        df = df.copy()
        df[col_name] = col_default_value
        df[col_name] = df[col_name].astype(col_type)
        return df

    @staticmethod
    def rename_cols(df: pd.DataFrame, cols: dict):
        """Renomeia as colunas do Dataframe"""
        return df.rename(columns=cols)

    @staticmethod
    def join(df_left: pd.DataFrame, df_right: pd.DataFrame, left_on: list, right_on: list, how: Literal["left", "right","cross", "inner", "outer"]):
        "Aplica um join entre os dataframes informados"
        return pd.merge(left=df_left, right=df_right, left_on=left_on, right_on=right_on, how=how)

    @staticmethod
    def distinct(df: pd.DataFrame):
        """Aplica um distinct no dataframes informado"""
        return df.drop_duplicates()

    @staticmethod
    def query(df: pd.DataFrame, condition: str):
        """Aplica uma query (DataFrame.query()) no dataframe informado"""
        return df.query(condition)

    @staticmethod
    def convert_type(df: pd.DataFrame, col: str, type):
        """Converte o tipo da coluna informada"""
        df = df.copy()
        df[col] = df[col].astype(type)
        return df

    @staticmethod
    def display(df: pd.DataFrame):
        """Imprime o dataframe"""
        import IPython.display

        IPython.display.clear_output()
        IPython.display.display(df)
        return df

    @staticmethod
    def dtypes(df: pd.DataFrame):
        print(df.dtypes)
        return df    
    

