import pandas as pd
import requests
import time
import random
import sys
import boto3
import io
import os
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from requests import RequestException
from tqdm import tqdm

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
            print(f"Tạo bucket {BUCKET_NAME}...")
            s3_client.create_bucket(Bucket=BUCKET_NAME)

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

params = (
    ('platform', 'web'),
    ('spid', 187266106)
    #('include', 'tag,images,gallery,promotions,badges,stock_item,variants,product_links,discount_tag,ranks,breadcrumbs,top_features,cta_desktop'),
)



MAX_WORKERS = 3
MAX_WORKERS = 3
SAVE_EVERY = 20
REQUEST_DELAY_RANGE = (0.8, 2.0)
MAX_RETRIES = 2
REQUIRED_COLUMNS = {'id', 'quantity_sold_text', 'quantity_sold_value'}


if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')


def parser_product(json):
    stock_item = json.get('stock_item') or {}
    brand = json.get('brand') or {}
    quantity_sold = json.get('quantity_sold') or {}

    d = dict()
    d['id'] = json.get('id')
    d['sku'] = json.get('sku')
    d['short_description'] = json.get('short_description')
    d['price'] = json.get('price')
    d['name'] = json.get('name')
    d['list_price'] = json.get('list_price')
    d['price_usd'] = json.get('price_usd')
    d['discount'] = json.get('discount')
    d['discount_rate'] = json.get('discount_rate')
    d['review_count'] = json.get('review_count')
    d['order_count'] = json.get('order_count')
    d['quantity_sold_text'] = quantity_sold.get('text')
    d['quantity_sold_value'] = quantity_sold.get('value')
    d['inventory_status'] = json.get('inventory_status')
    d['is_visible'] = json.get('is_visible')
    d['stock_item_qty'] = stock_item.get('qty')
    d['stock_item_max_sale_qty'] = stock_item.get('max_sale_qty')
    d['product_name'] = json.get('meta_title')
    d['brand_id'] = brand.get('id')
    d['brand_name'] = brand.get('name')
    return d


def fetch_product(session, pid):
    url = 'https://tiki.vn/api/v2/products/{}'.format(pid)
    response = session.get(url, headers=headers, params=params, cookies=cookies, timeout=30)
    content_type = response.headers.get('content-type', '')

    if response.status_code != 200:
        return None, 'status {}'.format(response.status_code)

    if 'application/json' not in content_type:
        preview = response.text[:120].replace('\n', ' ')
        return None, 'not JSON: {} {}'.format(content_type or 'unknown content-type', preview)

    try:
        return response.json(), None
    except ValueError as exc:
        return None, 'invalid JSON: {}'.format(exc)


def crawl_one(pid):
    session = requests.Session()

    for attempt in range(MAX_RETRIES + 1):
        time.sleep(random.uniform(*REQUEST_DELAY_RANGE) + attempt * 3)

        try:
            data, error = fetch_product(session, pid)
        except RequestException as exc:
            data, error = None, 'request error: {}'.format(exc)

        if not error:
            return parser_product(data), None

    return None, {'id': pid, 'error': error}


def save_rows(rows, output_key):
    if not rows:
        return None

    df_product = pd.DataFrame(rows).drop_duplicates(subset=['id'])
    csv_buffer = df_product.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=output_key,
        Body=csv_buffer
    )
    return output_key

# Định nghĩa lại số lượng luồng để tránh bị khóa IP
MAX_CONCURRENT_FILES = 2  # Số lượng file chạy song song cùng lúc
MAX_WORKERS_PER_FILE = 3  # Số lượng sản phẩm chạy song song trong 1 file

def process_single_file(input_key):
    """
    Hàm xử lý độc lập cho 1 file duy nhất. 
    Sẽ được gọi đồng thời bởi ThreadPoolExecutor trong main()
    """
    file_name = input_key.split('/')[-1]
    base_name = file_name.replace('product_id_', '').replace('.csv', '')
    
    output_key = f'crawled_products/crawled_{base_name}.csv'
    failed_key = f'crawled_products/failed_{base_name}.csv'
    
    # Đọc file ID hiện tại từ MinIO
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=input_key)
        df_id = pd.read_csv(response['Body'])
    except Exception as e:
        print(f'[{file_name}] Lỗi đọc file từ MinIO: {e}')
        return

    if 'id' not in df_id.columns:
        print(f'[{file_name}] Không có cột "id", bỏ qua...')
        return
        
    p_ids = df_id['id'].dropna().drop_duplicates().tolist()
    result = []
    failed = []
    
    # Kiểm tra tính liên tục (Resume) trên MinIO
    try:
        old_response = s3_client.get_object(Bucket=BUCKET_NAME, Key=output_key)
        old_df = pd.read_csv(old_response['Body'])
        if REQUIRED_COLUMNS.issubset(old_df.columns):
            result = old_df.to_dict('records')
            crawled_ids = set(old_df['id'].dropna().tolist())
            p_ids = [pid for pid in p_ids if pid not in crawled_ids]
            print(f'[{file_name}] Resume: Đã có {len(result)} sp, còn lại {len(p_ids)}.')
        elif 'id' in old_df.columns:
            print(f'[{file_name}] CSV cũ thiếu cột. Crawl lại {len(p_ids)} IDs.')
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            pass
        else:
            print(f"[{file_name}] Lỗi khi check output cũ: {e}")

    if not p_ids:
        print(f'[{file_name}] Đã crawl hoàn tất từ trước. Bỏ qua!')
        return

    print(f'>>> BẮT ĐẦU CRAWL FILE: {file_name} ({len(p_ids)} sản phẩm)')

    # Chạy đa luồng cho các sản phẩm trong file này
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_FILE) as executor:
        futures = {executor.submit(crawl_one, pid): pid for pid in p_ids}

        # Bỏ tqdm đi để tránh bị nhiễu log console khi nhiều file in ra cùng lúc
        for index, future in enumerate(as_completed(futures), start=1):
            pid = futures[future]
            try:
                row, error = future.result()
            except Exception as exc:
                row, error = None, {'id': pid, 'error': 'unexpected error: {}'.format(exc)}

            if error:
                failed.append(error)
                print(f'[{base_name}] Lỗi ID {pid}: {error["error"]}')
            else:
                result.append(row)
                # Tạm ẩn dòng in thành công để đỡ rác console, chỉ in định kỳ lúc save
                # print(f'[{base_name}] Crawl success {pid}')

            # Lưu định kỳ
            if index % SAVE_EVERY == 0:
                save_rows(result, output_key)
                print(f'[{base_name}] Đã quét {index}/{len(p_ids)} sản phẩm...')

    # Lưu chốt lại khi hoàn thành 1 file
    saved_path = save_rows(result, output_key)
    
    if failed:
        df_failed = pd.DataFrame(failed)
        csv_buffer = df_failed.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        s3_client.put_object(Bucket=BUCKET_NAME, Key=failed_key, Body=csv_buffer)
        print(f'[{base_name}] Đã lưu {len(failed)} IDs lỗi vào {failed_key}')
        
    if saved_path:
        final_count = len(pd.DataFrame(result).drop_duplicates(subset=['id']))
        print(f'🎉 [HOÀN THÀNH {base_name}] Đã lưu tổng cộng {final_count} sản phẩm!')


def main():
    init_minio()
    
    # Liệt kê tất cả các file từ thư mục crawled_ids trên MinIO
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix='crawled_ids/')
    if 'Contents' not in response:
        print("Không tìm thấy file danh sách ID nào trên MinIO!")
        return
        
    input_keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
    
    if not input_keys:
        print("Không tìm thấy file danh sách ID định dạng .csv nào trên MinIO!")
        return
        
    print(f'Tìm thấy {len(input_keys)} file danh mục cần xử lý.')
    print(f'Đang cấu hình chạy song song {MAX_CONCURRENT_FILES} file cùng lúc...')
    print('======================================================\n')

    # Dùng ThreadPoolExecutor để chạy song song các file
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_FILES) as executor:
        # Submit tất cả các file vào pool
        futures = [executor.submit(process_single_file, input_key) for input_key in input_keys]
        
        # Đợi các file hoàn thành (Không bắt buộc phải có, nhưng giúp catch error tổng)
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Lỗi nghiêm trọng khi xử lý file: {e}")

    print('\n======================================================')
    print('TẤT CẢ CÁC FILE ĐÃ ĐƯỢC XỬ LÝ XONG!')

if __name__ == '__main__':
    main()