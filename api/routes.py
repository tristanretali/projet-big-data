from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional
from .auth import verify_token, create_access_token
from .database import get_db_connection
from pydantic import BaseModel

router = APIRouter()

class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/login")
def login(credentials: LoginRequest):
    if credentials.username == "admin" and credentials.password == "admin":
        token = create_access_token({"sub": credentials.username})
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Invalid credentials")


@router.get("/sales_per_country")
def sales_per_country(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    token: dict = Depends(verify_token)
):
    # MODIFICATION: Utilisation du context manager avec 'with'
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        offset = (page - 1) * page_size
        
        query = '''
            SELECT "Country", total_sales, number_of_orders, total_quantity
            FROM sales_per_country
            ORDER BY total_sales DESC
            LIMIT %s OFFSET %s
        '''

        cursor.execute("SELECT COUNT(*) as total FROM sales_per_country")
        total = cursor.fetchone()['total']


        cursor.execute(query, (page_size, offset))
        data = cursor.fetchall()
        cursor.close()
        
        
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "data": data
        }
        


@router.get("/top_products")
def top_products(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    mode: str = Query("sales", regex="^(sales|returns)$"),
    token: dict = Depends(verify_token)
):
    # MODIFICATION: Utilisation du context manager avec 'with'
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        offset = (page - 1) * page_size
        
        query = '''
            SELECT "StockCode", "Description", total_revenue, total_quantity_sold, number_of_sales
            FROM top_products
            ORDER BY total_revenue DESC
            LIMIT %s OFFSET %s
        '''
        
        cursor.execute(query, (page_size, offset))
        data = cursor.fetchall()
        
        cursor.execute("SELECT COUNT(*) as total FROM return_products")
        total = cursor.fetchone()['total']
        
        cursor.close()
        
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": data
        }
        


@router.get("/return_products")
def return_products(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    token: dict = Depends(verify_token)
):
    # MODIFICATION: Utilisation du context manager avec 'with'
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        offset = (page - 1) * page_size
        
        query = '''
            SELECT "StockCode", "Description", total_returned, number_of_returns, total_loss
            FROM return_products
            ORDER BY total_returned DESC
            LIMIT %s OFFSET %s
        '''
        
        cursor.execute(query, (page_size, offset))
        data = cursor.fetchall()
        
        cursor.execute("SELECT COUNT(*) as total FROM return_products")
        total = cursor.fetchone()['total']
        
        cursor.close()
        
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": data
        }


@router.get("/periodic")
def periodic_sales(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    period: str = Query("month", regex="^(day|month|year)$"),
    token: dict = Depends(verify_token)
):
        
    # MODIFICATION: Utilisation du context manager avec 'with'
    with get_db_connection() as conn:

        cursor = conn.cursor()
        
        offset = (page - 1) * page_size

        period_column = {
            "day": "day",
            "month": "month", 
            "year": "year"
        }.get(period, "month")

        query = f'''
            SELECT "{period_column}", total_sales, number_of_orders
            FROM sales_by_period
            WHERE "{period_column}" IS NOT NULL
            ORDER BY "{period_column}" DESC
            LIMIT %s OFFSET %s
        '''
        
        cursor.execute(query, (page_size, offset))
        data = cursor.fetchall()
        
        cursor.execute(
            f'SELECT COUNT(*) as total FROM sales_by_period WHERE "{period_column}" IS NOT NULL')
        total = cursor.fetchone()['total']
        
        cursor.close()
        
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": data
        }