import os
import json
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_API_KEY and GEMINI_API_KEY != "your_gemini_key_here":
    genai.configure(api_key=GEMINI_API_KEY)

def analyze_intent(user_input, valid_categories=None):
    """
    Sử dụng Gemini để phân tích câu lệnh của User.
    Trả về định dạng JSON:
    {
        "category_key": "tai-nghe-co-day",
        "max_price": 500000, 
        "keywords": ["pin trâu", "chống ồn"]
    }
    """
    if not GEMINI_API_KEY or GEMINI_API_KEY == "your_gemini_key_here":
        print("LỖI: Chưa cấu hình GEMINI_API_KEY trong file .env")
        return None

    model = genai.GenerativeModel('gemini-3.5-flash')
    
    categories_prompt = ""
    if valid_categories:
        categories_prompt = f"Hãy cố gắng map sản phẩm user muốn tìm với 1 trong các category_key sau đây: {', '.join(valid_categories)}."
        
    prompt = f"""
    Bạn là một trợ lý bán hàng E-commerce. Hãy phân tích yêu cầu mua sắm của người dùng.
    Yêu cầu: "{user_input}"
    {categories_prompt}
    
    Trả về KẾT QUẢ ĐẦU RA là một chuỗi JSON chuẩn với 3 trường:
    - "category_key": (string) Tên danh mục sản phẩm (URL key). Cố gắng chọn từ danh sách trên nếu có thể.
    - "max_price": (integer) Mức giá tối đa người dùng yêu cầu (quy ra VNĐ). Nếu không đề cập giá, để null.
    - "keywords": (array of strings) Các tính năng, từ khóa quan trọng mà người dùng muốn (vd: "chơi game", "cơ", "giá rẻ", "chống ồn").

    CHỈ trả về đúng chuỗi JSON, không giải thích, không có markdown formatting (như ```json).
    """
    
    try:
        response = model.generate_content(prompt)
        text_response = response.text.strip()
        
        # Clean up markdown if Gemini still returns it despite the prompt
        if text_response.startswith("```json"):
            text_response = text_response[7:-3].strip()
        elif text_response.startswith("```"):
            text_response = text_response[3:-3].strip()
            
        data = json.loads(text_response)
        return data
    except Exception as e:
        print(f"Lỗi khi gọi Gemini API: {e}")
        return None
