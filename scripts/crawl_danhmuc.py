import requests
from bs4 import BeautifulSoup
import re

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36',
}

url = 'https://tiki.vn/thiet-bi-kts-phu-kien-so/c1815' 
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')

categories_list = []

# BƯỚC QUAN TRỌNG: Thu hẹp phạm vi tìm kiếm vào đúng cái Div chứa menu
# Thay 'TÊN_CLASS_CỦA_KHUNG_MENU' bằng class bạn tìm được ở Bước 1
# (Ví dụ: 'style__CategoryList-sc-12345' hoặc dùng thuộc tính khác)
menu_container = soup.find('div', class_='sc-36d678cb-0 eSTCTE')

# Nếu tìm thấy khung menu, ta mới bắt đầu lặp các thẻ <a> bên trong nó
if menu_container:
    print("Đã khoanh vùng được menu!")
    for a_tag in menu_container.find_all('a', href=True):
        href = a_tag['href']
        
        # Lọc ra các URL có cấu trúc của danh mục con
        match = re.search(r'/([^/]+)/c(\d+)', href)
        if match:
            url_key = match.group(1)
            category_id = match.group(2)
            
            cat_dict = {'urlKey': url_key, 'category': category_id}
            if cat_dict not in categories_list:
                categories_list.append(cat_dict)
else:
    print("Không tìm thấy khung chứa menu, hãy kiểm tra lại class!")

print("Danh sách danh mục con cần crawl:")
for cat in categories_list:
    print(cat)