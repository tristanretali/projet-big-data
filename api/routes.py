from fastapi import APIRouter, Request
from pyspark.sql import SparkSession

router = APIRouter()


def get_spark_session(request: Request):
    return request.app.state.spark


@router.get("/sales_per_country")
def sales_per_country():
    return {"message": "vente par pays"}


@router.get("/top_products")
def top_products():
    return {"message": "top produits"}


@router.get("/return_products")
def return_products():
    return {"message": "produits retournés"}


@router.get("/periodic")
def periodic():
    return {"message": "période"}
