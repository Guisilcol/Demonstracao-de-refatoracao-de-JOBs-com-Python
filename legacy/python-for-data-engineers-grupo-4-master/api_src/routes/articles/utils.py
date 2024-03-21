from .request_model import GetMethodRequestParamsModel


class ValidateQueryParams:

    @staticmethod
    def validade(param_name, value, valid_operators):
        return True

class DynamicWhereConditionGenerator:
    
    @staticmethod
    def generate_for_tb_bibtex_extraidos_manualmente(query_params: GetMethodRequestParamsModel):
        conditions = '(1 = 1'

        if query_params.author is not None:
            conditions += f" AND author {query_params.author}"
        if query_params.title is not None:
            conditions += f" AND title {query_params.title}"
        if query_params.keywords is not None:
            conditions += f" AND keywords {query_params.keywords}"
        if query_params.abstract is not None:
            conditions += f" AND abstract {query_params.abstract}"
        if query_params.year is not None:
            conditions += f" AND year {query_params.year}"
        if query_params.type_publication is not None:
            conditions += f" AND type_publication {query_params.type_publication}"
        if query_params.doi is not None:
            conditions += f" AND doi {query_params.doi}"
        if query_params.jcs_value is not None:
            conditions += f" AND jcs_value {query_params.jcs_value}"
        if query_params.scimago_value is not None:
            conditions += f" AND scimago_value {query_params.scimago_value}"

        conditions += ')'
            
        return conditions

    @staticmethod
    def generate_for_tb_bibtex_inseridos_via_api(query_params: GetMethodRequestParamsModel):
        conditions = '(1 = 1'

        if query_params.author is not None:
            conditions += f" AND author {query_params.author}"
        if query_params.title is not None:
            conditions += f" AND title {query_params.title}"
        if query_params.keywords is not None:
            conditions += f" AND keywords {query_params.keywords}"
        if query_params.abstract is not None:
            conditions += f" AND abstract {query_params.abstract}"
        if query_params.year is not None:
            conditions += f" AND year {query_params.year}"
        if query_params.type_publication is not None:
            conditions += f" AND type_publication {query_params.type_publication}"
        if query_params.doi is not None:
            conditions += f" AND doi {query_params.doi}"

        conditions += ')'
            
        return conditions