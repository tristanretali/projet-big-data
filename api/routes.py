from fastapi import APIRouter


router = APIRouter()


@router.get("/sales_per_country")
def read_root():
    return {"message": "vente par pays"}


@router.get("/top_products")
def read_root():
    return {"message": "top produits"}


@router.get("/return_products")
def read_root():
    return {"message": "produits retournés"}


@router.get("/periodic")
def read_root():
    return {"message": "période"}
