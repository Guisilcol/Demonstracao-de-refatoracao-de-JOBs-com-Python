import pydantic

class GetMethodRequestParamsModel(pydantic.BaseModel):
  author: str | None
  title: str | None
  keywords: str | None
  abstract: str | None       
  year: str | None           
  type_publication: str | None      
  doi: str | None            
  jcs_value: str | None     
  scimago_value: str | None  

  iee_api_page_quantity: int | None
  iee_api_query_terms: str | None

  science_direct_page_quantity: int | None
  science_direct_query_terms: str | None
  
  metadata_id: str | None

class PostRequestBodyModel(pydantic.BaseModel):
  pass