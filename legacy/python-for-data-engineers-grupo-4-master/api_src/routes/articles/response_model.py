import pydantic

class GetMethodResponseBodyModel(pydantic.BaseModel):
    database: list 
    ieee_api: list
    science_direct_api: list
    metadata_id: str

class PostMethodRequestBodyModel(pydantic.BaseModel):
    pass