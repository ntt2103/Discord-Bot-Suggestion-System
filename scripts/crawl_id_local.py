import requests
import time
import random
import pandas as pd
from datetime import datetime
import boto3
import io
from botocore.exceptions import ClientError
import concurrent.futures
from pathlib import Path
import os

s3_client = boto3.client(
    's3',
    endpoint_url=os.environ.get('MINIO_URL', 'http://localhost:9000'),     
    aws_access_key_id='admin',    
    aws_secret_access_key='password123', 
    region_name='us-east-1'                   
)
BUCKET_NAME = 'tiki-data-raw'

def init_minio():
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=BUCKET_NAME)

categories_crawl = [
    {'urlKey': 'tai-nghe-co-day', 'category': '1804'},
    {'urlKey': 'tai-nghe-bluetooth', 'category': '1811'},
    {'urlKey': 'loa-nghe-nhac', 'category': '1805'},
    {'urlKey': 'micro', 'category': '28642'},
    {'urlKey': 'dac-va-amplifier-di-dong', 'category': '20170'},
    {'urlKey': 'may-thu-va-may-phat-am-thanh', 'category': '28656'},
    {'urlKey': 'thiet-bi-am-thanh', 'category': '30374'},
    {'urlKey': 'thiet-bi-phat-streaming', 'category': '28858'},
    {'urlKey': 'receivers-amplifier-mixer', 'category': '7079'},
    {'urlKey': 'micro-karaoke-san-khau', 'category': '26652'},
    {'urlKey': 'phu-kien-am-thanh', 'category': '8068'},
    {'urlKey': 'dan-am-thanh', 'category': '4966'},
    {'urlKey': 'loa', 'category': '7075'},
    {'urlKey': 'dia-game', 'category': '12676'},
    {'urlKey': 'ghe-gaming', 'category': '7273'},
    {'urlKey': 'may-choi-game-console', 'category': '8251'},
    {'urlKey': 'phu-kien-gaming', 'category': '6742'},
    {'urlKey': 'tay-bam-game-thiet-bi-dieu-khien', 'category': '9013'},
    {'urlKey': 'mat-kinh-thong-minh', 'category': '8040'},
    {'urlKey': 'thiet-bi-thuc-te-ao-vr', 'category': '8042'},
    {'urlKey': 'dong-ho-thong-minh', 'category': '2657'},
    {'urlKey': 'vong-deo-thong-minh-vong-theo-doi-van-dong', 'category': '8043'},
    {'urlKey': 'phu-kien-thiet-bi-deo-thong-minh', 'category': '28420'},
    {'urlKey': 'thiet-bi-so-thiet-bi-thong-minh', 'category': '28436'},
    {'urlKey': 'mach-dien-lap-trinh-va-linh-kien-dien-tu', 'category': '20246'},
    {'urlKey': 'the-nho-va-bo-nho-mo-rong', 'category': '28448'},
    {'urlKey': 'sim-dien-thoai', 'category': '10691'},
    {'urlKey': 'pin-sac-du-phong', 'category': '1821'},
    {'urlKey': 'day-sac-day-cap', 'category': '1823'},
    {'urlKey': 'adapter-cu-sac', 'category': '3803'},
    {'urlKey': 'linh-kien-thay-the', 'category': '20234'},
    {'urlKey': 'dung-cu-sua-chua-va-ve-sinh', 'category': '20242'},
    {'urlKey': 'gia-do-chan-de-moc-dan', 'category': '20220'},
    {'urlKey': 'tui-bao-ve-tui-chong-nuoc', 'category': '20236'},
    {'urlKey': 'hop-dung-tui-dung-phu-kien', 'category': '20244'},
    {'urlKey': 'vong-deo-dai-deo', 'category': '20240'},
    {'urlKey': 'day-deo-va-phu-kien-trang-tri', 'category': '20222'},
    {'urlKey': 'mieng-dan-man-hinh', 'category': '1824'},
    {'urlKey': 'bao-da-op-lung', 'category': '1822'},
    {'urlKey': 'ban-phim-roi-but-cam-ung', 'category': '20238'},
    {'urlKey': 'phu-kien-chup-hinh', 'category': '28620'},
    {'urlKey': 'phu-kien-choi-game', 'category': '28624'},
    {'urlKey': 'phu-kien-di-dong-khac', 'category': '1819'},
    {'urlKey': 'mieng-dan-man-hinh-dien-thoai-va-may-tinh-bang', 'category': '69243'},
    {'urlKey': 'dung-cu-ve-sinh-va-sua-chua', 'category': '28672'},
    {'urlKey': 'phu-kien-laptop', 'category': '7795'},
    {'urlKey': 'webcam-may-tinh', 'category': '4880'},
    {'urlKey': 'but-trinh-chieu', 'category': '28688'},
    {'urlKey': 'ban-di-chuot-mieng-lot-chuot', 'category': '1831'},
    {'urlKey': 'phu-kien-phim-chuot-van-phong', 'category': '28690'},
    {'urlKey': 'phu-kien-phim-chuot-choi-game', 'category': '28692'},
    {'urlKey': 'tai-nghe-may-tinh', 'category': '28702'},
    {'urlKey': 'cong-cu-do-hoa-thiet-bi-cam-ung', 'category': '28706'},
    {'urlKey': 'cac-loai-cap-chuyen-doi', 'category': '12296'}
]

cookies = {
    'TIKI_GUEST_TOKEN': 'Cn1sZNr2UaBc3QWxuVI8XSGYb4EADdqw',
    'TOKENS': '{%22access_token%22:%22Cn1sZNr2UaBc3QWxuVI8XSGYb4EADdqw%22%2C%22expires_in%22:157680000%2C%22expires_at%22:1763654224277%2C%22guest_token%22:%22Cn1sZNr2UaBc3QWxuVI8XSGYb4EADdqw%22}',
    'amp_99d374': 'eSc-_0HT1um7cb57E7dwA0...1enloc6a2.1enlpc1q6.b.i.t',
    'amp_99d374_tiki.vn': 'eSc-_0HT1um7cb57E7dwA0...1enloc6a2.1enlp8817.3.1.1',
    '_gcl_au': '1.1.559117409.1605974236',
    '_ants_utm_v2': '',
    '_pk_id.638735871.2fc5': 'b92ae025fbbdb31f.1605974236.1.1605975278.1605974236.',
    '_pk_ses.638735871.2fc5': '*',
    '_trackity': '1a736b8b-f086-e756-92b9-b0f815daaa7e',
    '_ga_NKX31X43RV': 'GS1.1.1605974235.1.1.1605975326.0',
    '_ga': 'GA1.1.657946765.1605974236',
    'ai_client_id': '11935756853.1605974227',
    'an_session': 'zizkzrzjzlzizqzlzqzjzdzizizqzgzmzkzmzlzrzmzgzdzizlzjzmzqzkznzhzhzkzdzizhzdzizlzjzmzqzkznzhzhzkzdzizlzjzmzqzkznzhzhzkzdzjzdzhzqzdzizd2f27zdzjzdzlzmzmznzq',
    'au_aid': '11935756853',
    'dgs': '1605975268%3A3%3A0',
    'au_gt': '1605974227146',
    '_ants_services': '%5B%22cuid%22%5D',
    '__admUTMtime': '1605974236',
    '__iid': '749',
    '__su': '0',
    '_bs': 'bb9a32f6-ab13-ce80-92d6-57fd3fd6e4c8',
    '_gid': 'GA1.2.867846791.1605974237',
    '_fbp': 'fb.1.1605974237134.1297408816',
    '_hjid': 'f152cf33-7323-4410-b9ae-79f6622ebc48',
    '_hjFirstSeen': '1',
    '_hjIncludedInPageviewSample': '1',
    '_hjAbsoluteSessionInProgress': '0',
    '_hjIncludedInSessionSample': '1',
    'tiki_client_id': '657946765.1605974236',
    '__gads': 'ID=ae56424189ecccbe-227eb8e1d6c400a8:T=1605974229:RT=1605974229:S=ALNI_MZFWYf2BAjzCSiRNLC3bKI-W_7YHA',
    'proxy_s_sv': '1605976041662',
    'TKSESSID': '8bcd49b02e1e16aa1cdb795c54d7b460',
    'TIKI_RECOMMENDATION': '21dd50e7f7c194df673ea3b717459249',
    'cto_bundle': 'V9Dkml9NVXNkQmJ6aEVLcXNqbHdjcVZoQ0l5RXpOMlRybjdDT0JrUnNxVXc2bHd0N2J3Y2FCdmZVQXdYY1QlMkJYUmxXSHZ2UEFwd3IzRHhzRWJYMlQlMkJsQjhjQlA1JTJCcExyRzlUQk5CYUdMdjl2STNQanVsa3cycHd3SHElMkJabnI3dzNhREpZcFVyandyM1d1QWpJbWYxT1UyWDdHZyUzRCUzRA',
    'TIKI_RECENTLYVIEWED': '58259141',
    '_ants_event_his': '%7B%22action%22%3A%22view%22%2C%22time%22%3A1605974691247%7D',
    '_gat': '1',
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
    'Referer': 'https://tiki.vn/dien-thoai-may-tinh-bang/c1789',
    'x-guest-token': 'Cn1sZNr2UaBc3QWxuVI8XSGYb4EADdqw',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}
def crawl_single_category(cat):
    """
    Hàm xử lý cào dữ liệu cho 1 danh mục cụ thể
    """
    current_urlKey = cat['urlKey']
    current_category = cat['category']
    
    print(f"\n[BẮT ĐẦU] --- Đang crawl: {current_urlKey} ---")
    
    params = {
        'limit': '40',
        'include': 'advertisement',
        'aggregations': '2',
        'trackity_id': '1a736b8b-f086-e756-92b9-b0f815daaa7e',
        'category': current_category,
        'page': '1',
        'urlKey': current_urlKey,
    }
    product_id = []
    
    # Vòng lặp con quét qua các trang
    for i in range(1, 20):
        params['page'] = str(i)
        
        try:
            response = requests.get('https://tiki.vn/api/personalish/v1/blocks/listings', headers=headers, params=params, cookies=cookies)
            
            if response.status_code == 200:
                try:
                    data = response.json().get('data')
                except Exception as e:
                    print(f"[{current_urlKey}] Lỗi parse JSON ở trang {i}: {e}. Nội dung trả về: {response.text[:200]}")
                    break
                
                # Hết data thì thoát vòng lặp trang
                if not data:
                    print(f'[{current_urlKey}] Trang {i} rỗng, kết thúc danh mục.')
                    break
                    
                for record in data:
                    product_id.append({'id': record.get('id')})
                print(f'[{current_urlKey}] Crawl thành công - Trang {i}')
            else:
                print(f'[{current_urlKey}] LỖI {response.status_code} - Trang {i}')
                
        except Exception as e:
            print(f"[{current_urlKey}] Lỗi kết nối tại trang {i}: {e}")
            
        # Nghỉ ngơi giữa các trang
        time.sleep(random.randrange(3, 8))
        
    # Lưu file khi duyệt xong danh mục
    if product_id: 
        df = pd.DataFrame(product_id)
        current_date = datetime.now().strftime("%d%m%y")
        
        csv_buffer = df.to_csv(index=False).encode('utf-8')
        object_key = f'crawled_ids/product_id_{current_urlKey}_{current_date}.csv'
        
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=object_key,
                Body=csv_buffer
            )
            print(f"[{current_urlKey}] -> Đã lưu xong file lên MinIO: {object_key}")
        except Exception as e:
            print(f"[{current_urlKey}] -> Lỗi lưu file lên MinIO: {e}")
        
    # Nghỉ ngơi một chút trước khi thread này nhận nhiệm vụ (danh mục) mới
    time.sleep(random.randrange(5, 10))


# ==========================================
# KHU VỰC ĐIỀU PHỐI ĐA LUỒNG
# ==========================================
if __name__ == '__main__':
    # max_workers: Số lượng danh mục chạy song song cùng 1 lúc. 
    # KHUYẾN NGHỊ: Đặt từ 3 đến 5 để không bị Tiki chặn IP. Đừng tham đặt quá cao!
    MAX_THREADS = 4 
    
    print(f"Bắt đầu khởi chạy {MAX_THREADS} luồng song song...")
    
    init_minio()
    
    # Bắt đầu giao việc cho các luồng
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # Lệnh map sẽ tự động lấy từng item trong categories_crawl ném vào hàm crawl_single_category
        executor.map(crawl_single_category, categories_crawl)
        
    print("\n🎉 HOÀN THÀNH TẤT CẢ DANH MỤC!")