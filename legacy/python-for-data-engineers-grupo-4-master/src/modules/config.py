import yaml 
from schema import Schema, And, Or 
from dataclasses import dataclass

class Config:
    _ACCEPTED_EXPORT_TYPES = ["json", "xml", "csv", "yaml"] 

    _CONFIG_FILE_SCHEMA_VALIDATOR = Schema({
        'export_format': And(str, lambda data: data in Config._ACCEPTED_EXPORT_TYPES),
        'filters': {
            'title' : Or(str, None),      
            'keywords': Or([str], None),       
            'abstract': Or(str, None),       
            'year': Or([int], None),            
            'type_publication': Or(str, None),       
            'doi': Or(str, None),            
            'jcs_value': Or(int, None),       
            'scimago_value': Or(int, None), 
        },
        'iee_api_config': {
            'query_terms': Or([str], None),
            'pages_quantity': Or(int, None)
        },
        'science_direct_api_config': {
            'query_terms': Or([str], None),
            'pages_quantity': Or(int, None)
        }
    })

    @staticmethod
    def get_config(config_filepath: str) -> dict:
        with open(config_filepath) as file:
            config = yaml.load(file, yaml.loader.SafeLoader)
            return Config._CONFIG_FILE_SCHEMA_VALIDATOR.validate(config)
    
    def __init__(self) -> None:
         raise NotImplementedError()
