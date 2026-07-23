import requests
import time
import random

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*'
}

# 1. Danh sách các parent_category_id bạn muốn crawl
# (Ví dụ: 1815 là Thiết bị số, 1882 là Điện gia dụng, 8322 là Nhà cửa đời sống...)
parent_category_ids = ['8215', '2667', '8039', ' 28432', '8214', '28670'] 

print("Danh sách kết quả để bạn copy:")
print("[")

# 2. Vòng lặp cha duyệt qua từng parent_id
for parent_id in parent_category_ids:
    api_url = f'https://tiki.vn/api/v2/categories?parent_id={parent_id}'
    
    try:
        response = requests.get(api_url, headers=headers)
        
        if response.status_code == 200:
            categories_data = response.json().get('data', [])
            
            # Lặp qua các danh mục con trả về và in ra theo format yêu cầu
            for cat in categories_data:
                cat_id = str(cat.get('id'))
                url_key = cat.get('url_key')
                
                # Dùng {{ và }} để in ra dấu ngoặc nhọn trong f-string
                print(f"    {{'urlKey': '{url_key}', 'category': '{cat_id}'}},")
                
        else:
            print(f"    # Lỗi gọi API ở parent_id {parent_id}: {response.status_code}")
            
    except Exception as e:
        print(f"    # Xảy ra lỗi ở parent_id {parent_id}: {e}")
        
    # Nghỉ 1-3 giây giữa các lần gọi API danh mục để tránh bị Tiki chặn
    time.sleep(random.uniform(1, 3))

print("]")