import pandas as pd
import yaml

class SpecificOperations:

    @staticmethod
    def __parse_list_to_regex(list: list):
        return "|".join(list)

    @staticmethod
    def filter_column_by_regex(df: pd.DataFrame, config: dict):
        df = df.copy()

        if not config['filters']["title"] == None:
            regex = config['filters']["title"]
            df = df[df.title.str.contains(regex)]  # type: ignore

        if not config['filters']["keywords"] == None:
            array = config['filters']["keywords"] 
            df = df[df.keywords.str.contains(SpecificOperations.__parse_list_to_regex(array))]

        if not config['filters']["abstract"]  == None:
            regex = config['filters']["abstract"] 
            df = df[df.abstract.str.contains(regex)]

        if not config['filters']["year"]  == None:
            array = config['filters']["year"]  
            df = df[df.year.isin(array)]

        if not config['filters']["type_publication"]  == None:
            regex = config['filters']["type_publication"]
            df = df[df.type_publication.str.contains(regex)]

        if not config['filters']["doi"] == None:
            regex = config['filters']["doi"]
            df = df[df.doi.str.contains(regex)]

        if not config['filters']["jcs_value"] == None:
            data: float = config['filters']["jcs_value"]
            df = df[df.jcs_value == data]

        if not config['filters']["scimago_value"] == None:
            data: float = config['filters']["scimago_value"]
            df = df[df.scimago_value == data]

        return df

    @staticmethod
    def export_dataframe(dataframe: pd.DataFrame, config: dict, output_folder: str):
        EXPORT_TYPE = config["export_format"]

        if EXPORT_TYPE == "json":
            dataframe.to_json(f"{output_folder}/data.json", orient="records")

        elif EXPORT_TYPE == "csv":
            dataframe.to_csv(f"{output_folder}/data.csv", index=False, sep="|")

        elif EXPORT_TYPE == "yaml":
            data = yaml.dump(dataframe.to_dict(orient="records"))
            open(f"{output_folder}/data.yaml", 'w').write(data)

        elif EXPORT_TYPE == "xml":
            dataframe.to_xml(f"{output_folder}/data.xml", index=False)

        return dataframe

    @staticmethod
    def generate_key_with_journal_name(series: pd.Series):
        return series.str.replace("&", "AND")\
                            .str.replace(r"([^A-Za-z0-9]+)", "")\
                            .str.upper()\
                            .str.strip()\
      