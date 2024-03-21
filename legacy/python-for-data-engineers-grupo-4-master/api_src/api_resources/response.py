import typing as tp
import fastapi as fapi
from pydantic import BaseModel

class Response():
    
    def __init__(self, response: fapi.Response, status_code: int, data: tp.Union[BaseModel, None] = None, headers: tp.Union[dict, None] = None, error_message: tp.Union[str, None] = None):        
        self.fast_api_response = response
        
    

    @staticmethod
    def __prepare_response(response: fapi.Response, status_code: int, data: BaseModel | None, headers: dict | None = None, error_message: str | None = None):
        """_summary_ : Método responsável por preparar a "response" do API. Ela modifica o "status code", "headers" do objeto "response" e retorna o body da response. 
        """
        if headers != None:
            for key, value in headers.items():
                response.headers[key] = value
        
        content = None
        if error_message != None:
            content = {
                "status_code": status_code,
                "message": error_message
            }
        else:
            content = data.dict() if data != None else {}

        response.status_code = status_code
        return content

    @staticmethod
    def make_successful_response(response, status_code: int, data: BaseModel, headers: dict | None = None):
        return Response.__prepare_response(response, status_code, data, headers)

    @staticmethod
    def make_error_response(response, status_code: int, error_message: str, headers: dict | None = None):
        return Response.__prepare_response(response, status_code, None, headers, error_message)