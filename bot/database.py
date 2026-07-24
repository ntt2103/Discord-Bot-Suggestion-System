import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "admin"),
        password=os.getenv("DB_PASS", "password123"),
        dbname=os.getenv("DB_NAME", "tiki_db")
    )

def fetch_valid_categories():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT category_key FROM cleaned_products WHERE category_key IS NOT NULL")
        categories = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return categories
    except Exception as e:
        print(f"Lỗi lấy category từ DB: {e}")
        return []

def get_candidate_products(category_key, max_price=None, limit=50):
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = "SELECT * FROM cleaned_products WHERE category_key ILIKE %s"
        params = [f"%{category_key}%"]
        
        if max_price:
            query += " AND CASE WHEN price ~ '^[0-9]+$' THEN CAST(price AS numeric) ELSE NULL END <= %s"
            params.append(max_price)
            
        query += " ORDER BY base_score DESC LIMIT %s"
        params.append(limit)
        
        cur.execute(query, tuple(params))
        products = cur.fetchall()
        
        cur.close()
        conn.close()
        return products
    except Exception as e:
        print(f"Lỗi truy vấn DB: {e}")
        return []
