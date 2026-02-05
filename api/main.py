from fastapi import FastAPI
from routes import router
from spark_connexion import create_spark_session

app = FastAPI(title="Ecotrack API")


app.include_router(router)


@app.on_event("startup")
async def startup_event():
    app.state.spark = create_spark_session()
    print("Session Spark initialisée")
