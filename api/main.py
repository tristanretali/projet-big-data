from fastapi import FastAPI
from routes import router

app = FastAPI(title="Ecotrack API")


app.include_router(router)
