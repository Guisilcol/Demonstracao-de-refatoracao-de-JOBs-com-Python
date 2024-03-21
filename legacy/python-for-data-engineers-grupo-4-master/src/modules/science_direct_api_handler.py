import requests 
import json
from typing import List
import pandas as pd

class ScienceDirectApiHandler:

    API_KEY: str = '7f59af901d2d86f78a1fd60c1bf9426a'
    BASE_URL: str = f'https://api.elsevier.com/content/search/sciencedirect'

    __REQUEST_HEADER = {
        "x-els-apikey": API_KEY,
        "Content-Type": "application/json"
    }

    @staticmethod
    def __generate_request_body(query_terms: str, ordinal_page_position: int):
        return json.dumps({
            "qs": query_terms,
            "display": {
                "offset": 100 * ordinal_page_position,
                "show": 100,
                "sortBy": "date"
            }
        })
        

    @staticmethod
    def get_data(config: dict):
        query_terms = config.get("query_terms", ["big data", "data quality"])
        query_terms = ", ".join(query_terms)

        pages_quantity = config.get("pages_quantity", 1)
        url = f"{ScienceDirectApiHandler.BASE_URL}"

        responses = []
        for ordinal_position in range(0, pages_quantity):
            request_body = ScienceDirectApiHandler.__generate_request_body( query_terms, 
                                                                            ordinal_page_position=ordinal_position)  
            response = requests.put(url, 
                                    data=request_body, 
                                    headers=ScienceDirectApiHandler.__REQUEST_HEADER)

            if response.status_code != 200:
                raise Exception("Não foi possivel recuperar os dados da api Science Direct")
            
            responses.append(response.json())

        return responses


    @staticmethod
    def parse_response_to_dataframe(responses: List[dict]):
        dataframe_content = []
        for response in responses:
            results: List[dict] = response.get('results', [])

            for result in results:
                nested_authors = result.get("authors", [])
                authors = ", ".join([author["name"] for author in nested_authors])
                data = {
                    "authors": authors,
                    "title": result.get("title"),
                    "keywords": None,
                    "abstract": None,
                    "year": result.get("publicationDate", "")[0:4],
                    "type_publication": None,
                    "doi": result.get("doi"),
                    "issn": None,
                    "journal": result.get("sourceTitle"),
                    "source": "science direct API"
                }

                dataframe_content.append(data)
        
        return pd.DataFrame(dataframe_content)
            
