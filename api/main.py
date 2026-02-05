from fastapi import FastAPI
from routes import router
from spark_connexion import create_spark_session

app = FastAPI(title="Ecotrack API")

spark = None


@app.on_event("startup")
async def startup_event():
    global spark
    spark = create_spark_session()
    app.state.spark = spark
    print("Session Spark initialisée")


@app.on_event("shutdown")
async def shutdown_event():
    if spark:
        spark.stop()
        print("Session Spark arrêtée")


app.include_router(router)
