import fastapi as fapi
import sqlite3
import typing as tp
from routes.articles.utils import ValidateQueryParams, DynamicWhereConditionGenerator
from routes.articles.request_model import GetMethodRequestParamsModel
from routes.articles.response_model import GetMethodResponseBodyModel, PostMethodRequestBodyModel
from api_resources.response import Response
from routes.articles.__ieee_api import IeeeApiHandler
from routes.articles.__science_direct_api import ScienceDirectApiHandler
from database import Database
from bibtexparser import loads as parse_bibtex
import asyncio
import traceback
from uuid import uuid4 as generate_uuid


class Service():

    @staticmethod
    async def get(request: fapi.Request, response: fapi.Response):

        async def __get_data_from_database(connection: sqlite3.Connection, query: str):
            return Database.execute(connection, query, [])

        async def __get_data_from_ieee_api(pages_quantity: tp.Union[int, None], query_terms: tp.Union[str, None]):
            if pages_quantity == None:
                pages_quantity = 1
            
            return IeeeApiHandler.get_data(iee_api_page_quantity=pages_quantity, iee_api_query_terms=query_terms)

        async def __get_data_from_science_direct_api(pages_quantity: tp.Union[int, None], query_terms: tp.Union[str, None]):
            if pages_quantity == None:
                pages_quantity = 1

            return ScienceDirectApiHandler.get_data(science_direct_page_quantity=pages_quantity, science_direct_query_terms=query_terms)

        connection = Database.get_connection()

        try:
            allowed_operators = ["=", ">", "<", ">=", "<=", "<>", "like", "in", "is null", "is not null"]
            req_params = GetMethodRequestParamsModel(**dict(request.query_params)) #type: ignore
            
            if not ValidateQueryParams.validade("author", req_params.author, allowed_operators):
                return Response.make_error_response(response, 400, "Invalid operator or syntax in 'author'")

            if not ValidateQueryParams.validade("title", req_params.title, allowed_operators):
                return Response.make_error_response(response, 400, "Invalid operator or syntax in 'title'")

            if not ValidateQueryParams.validade("keywords", req_params.keywords, allowed_operators):
                return Response.make_error_response(response, 400, "Invalid operator or syntax in 'keywords'")

            if not ValidateQueryParams.validade("abstract", req_params.abstract, allowed_operators):
                return Response.make_error_response(response, 400, "Invalid operator or syntax in 'abstract'")

            if not ValidateQueryParams.validade("year", req_params.year, allowed_operators):
                return Response.make_error_response(response, 400, "Invalid operator or syntax in 'year'")

            if not ValidateQueryParams.validade("type_publication", req_params.type_publication, allowed_operators):
                return Response.make_error_response(response, 400, "Invalid operator or syntax in 'type_publication'")

            if not ValidateQueryParams.validade("doi", req_params.doi, allowed_operators):
                return Response.make_error_response(response, 400, "Invalid operator or syntax in 'doi'")

            if not ValidateQueryParams.validade("jcs_value", req_params.jcs_value, allowed_operators):
                return Response.make_error_response(response, 400, "Invalid operator or syntax in 'jcs_value'")

            if not ValidateQueryParams.validade("scimago_value", req_params.scimago_value, allowed_operators):
                return Response.make_error_response(response, 400, "Invalid operator or syntax in 'scimago_value'")

            if Database.verify_if_row_exists(connection, "SELECT * FROM tb_consulta WHERE metadata_id = ? and metadata_id is not null", [req_params.metadata_id]):
                return Response.make_error_response(response, 400, "Metadata id already exists")

            generated_where_condition_tb_bibtex_extraidos_manualmente = DynamicWhereConditionGenerator.generate_for_tb_bibtex_extraidos_manualmente(req_params)
            generated_where_condition_tb_bibtex_inseridos_via_api = DynamicWhereConditionGenerator.generate_for_tb_bibtex_inseridos_via_api(req_params)

            query = f"""SELECT distinct author, title, keywords, abstract, year, type_publication, doi, issn, journal, url, jcs_value, scimago_value, source 
                        FROM tb_bibtex_extraidos_manualmente
                        WHERE {generated_where_condition_tb_bibtex_extraidos_manualmente}
                        UNION
                        SELECT distinct author, title, keywords, abstract, year, type_publication, doi, issn, journal, url, null as jcs_value, null scimago_value, 'via post request in /articles' as source
                        FROM tb_bibtex_inseridos_via_api
                        WHERE {generated_where_condition_tb_bibtex_inseridos_via_api};
                        """
            
            future_database_data = __get_data_from_database(connection, query)
            future_ieee_api_data = __get_data_from_ieee_api(req_params.iee_api_page_quantity, req_params.iee_api_query_terms)
            future_science_direct_api_data = __get_data_from_science_direct_api(req_params.science_direct_page_quantity, req_params.science_direct_query_terms)

            data = await asyncio.gather(future_database_data, future_ieee_api_data, future_science_direct_api_data)

            metadata_id = None
            if req_params.metadata_id != None and req_params.metadata_id.strip() != "":
                metadata_id = req_params.metadata_id
                Database.execute(connection, "INSERT INTO tb_consulta (metadata_id, author, title, keywords, abstract, year, type_publication, doi, jcs_value, scimago_value, iee_api_page_quantity, iee_api_query_terms, science_direct_page_quantity, science_direct_query_terms, database_total, ieee_api_total, science_direct_api) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", 
                    [req_params.metadata_id, req_params.author, req_params.title, req_params.keywords, req_params.abstract, req_params.year, 
                        req_params.type_publication, req_params.doi, req_params.jcs_value, req_params.scimago_value, req_params.iee_api_page_quantity, 
                        req_params.iee_api_query_terms, req_params.science_direct_page_quantity, req_params.science_direct_query_terms, len(data[0]), len(data[1]), len(data[2])])
            else: 
                metadata_id = str(generate_uuid())
                Database.execute(connection, "INSERT INTO tb_consulta (metadata_id, author, title, keywords, abstract, year, type_publication, doi, jcs_value, scimago_value, iee_api_page_quantity, iee_api_query_terms, science_direct_page_quantity, science_direct_query_terms, database_total, ieee_api_total, science_direct_api) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", 
                    [metadata_id, req_params.author, req_params.title, req_params.keywords, req_params.abstract, req_params.year, 
                        req_params.type_publication, req_params.doi, req_params.jcs_value, req_params.scimago_value, req_params.iee_api_page_quantity, 
                        req_params.iee_api_query_terms, req_params.science_direct_page_quantity, req_params.science_direct_query_terms, len(data[0]), len(data[1]), len(data[2])])

            response_body = {
                "database": data[0], 
                "ieee_api": data[1],
                "science_direct_api": data[2],
                "metadata_id": metadata_id
            }

            connection.commit()
            return Response.make_successful_response(response, 200, GetMethodResponseBodyModel(**response_body))
        except Exception as e:
            traceback.print_exc()
            return Response.make_error_response(response, 500, "Erro interno ao consultar as bases de dados")
        
        finally:
            connection.close()

        

    @staticmethod
    async def post(request: fapi.Request, response: fapi.Response):
        connection = Database.get_connection()

        try:
            def __standardize_bibtex(bibtex: dict):
                return {
                    "author": bibtex.get("author", None),
                    "title": bibtex.get("title", None),
                    "keywords": bibtex.get("keywords", None),
                    "abstract": bibtex.get("abstract", None),
                    "year": bibtex.get("year", None),
                    "type_publication": bibtex.get("ENTRYTYPE", None),
                    "doi": None if bibtex.get("doi") == None else str(bibtex.get("doi")).replace("https://doi.org/", ""),
                    "issn": None if bibtex.get("issn") == None else str(bibtex.get("issn")).replace("-", ""),
                    "journal": bibtex.get("journal", None),
                    "url": bibtex.get("url", None)
                }

            request_form_data = await request.form()
            file = request_form_data.get("file", None)

            if file is None or isinstance(file, str):
                return Response.make_error_response(response, 400, "O arquivo .bib não foi enviado na requisição")

            if not file.filename.endswith(".bib"):
                return Response.make_error_response(response, 400, "O arquivo enviado não é um arquivo .bib")

            file_content = await file.read()
            bibtex_db = parse_bibtex(file_content.decode("utf-8"))

            standardized_bibtexs = [__standardize_bibtex(bib) for bib in bibtex_db.entries]
            standardized_bibtexs_values = [tuple(bib.values()) for bib in standardized_bibtexs]

            
            query = """ INSERT INTO tb_bibtex_inseridos_via_api (author, title, keywords, abstract, year, type_publication, doi, issn, journal, url) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""

            Database.execute_many(connection, query, standardized_bibtexs_values)

            connection.commit()
            return Response.make_successful_response(response, 201, PostMethodRequestBodyModel(**{}))
            
        except Exception as e:
            print(traceback.format_exc())
            return Response.make_error_response(response, 500, "Erro interno no servidor ao tentar processar o arquivo .bib")

        finally: 
            connection.close()

        