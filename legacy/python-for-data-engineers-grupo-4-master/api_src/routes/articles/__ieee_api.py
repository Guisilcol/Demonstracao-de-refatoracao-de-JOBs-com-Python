import requests
import typing as tp

class IeeeApiHandler():

    API_KEY: str = 'm4ch9nnmvqma6dycwwj3zppt'
    BASE_URL: str = f"http://ieeexploreapi.ieee.org/api/v1/search/articles?apikey={API_KEY}&format=json"

    @staticmethod
    def __generate_query_parameters(query_terms: str, ordinal_page_position: int):
        max_records = 200 
        start_record = 200 * ordinal_page_position
        query = f"max_records={max_records}&start_record={start_record}&index_terms={query_terms}".replace(" ", "+")
        return query


    @staticmethod
    def get_data(iee_api_page_quantity: int, iee_api_query_terms: str | None) -> tp.List[dict]:
        if iee_api_query_terms == None: 
            return []
        
        query_terms = ", ".join(iee_api_query_terms.split(","))

        responses: tp.List[dict] = []
        for ordinal_position in range(0, iee_api_page_quantity):
            request_query = IeeeApiHandler.__generate_query_parameters(query_terms, ordinal_page_position=ordinal_position)
            url = f"{IeeeApiHandler.BASE_URL}&{request_query}"
            response = requests.get(url)

            if (response.status_code != 200):
                raise Exception("Não foi possivel recuperar os dados da api IEEE")

            responses.append(response.json())
        
        content = []
        for response in responses:
            articles: tp.List[dict] = response.get('articles')  # type: ignore

            for article in articles:
                nested_authors = article.get("authors", {}).get("authors", [])
                authors = ", ".join([author.get("full_name") for author in nested_authors])

                keywords_list = article.get("index_terms", {}).get("ieee_terms", {}).get("terms", [])
                keywords = ", ".join(keywords_list)
                data = {
                    "authors": authors,
                    "title": article.get("title"),
                    "keywords": keywords,
                    "abstract": article.get("abstract"),
                    "year": article.get("publication_year"),    
                    "type_publication": article.get("content_type"),
                    "doi": article.get("doi"),
                    "issn": article.get("issn"),
                    "journal": article.get("publisher"),
                    "source": "ieee API",
                    "url": article.get("html_url", None)
                }
                content.append(data)

        return content