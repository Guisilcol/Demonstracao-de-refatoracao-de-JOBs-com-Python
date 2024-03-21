import fastapi 
import routes.articles as articles_route
import typing as tp
import uvicorn

app = fastapi.FastAPI()

@app.get("/articles")#, response_model=articles_route.GetMethodResponseBodyModel, response_model_exclude_none=False)
async def get_articles(request: fastapi.Request, response: fastapi.Response):
    return await articles_route.Service.get(request, response)


@app.post("/articles")
async def create_article_from_bibtex(request: fastapi.Request, response: fastapi.Response):
    return await articles_route.Service.post(request, response)


if __name__ == "__main__":
    uvicorn.run("main:app", port=8000, reload=True)