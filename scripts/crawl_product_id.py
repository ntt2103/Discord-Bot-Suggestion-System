import requests
import time
import random
import pandas as pd
from datetime import datetime
import boto3
import io
from botocore.exceptions import ClientError

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
params = {
    'limit': '40',
    'include': 'advertisement',
    'aggregations': '2',
    'trackity_id': '1a736b8b-f086-e756-92b9-b0f815daaa7e',
    'category': '1815',
    'page': '1',
    'urlKey':  'thiet-bi-kts-phu-kien-so',
}
name = params.get("urlKey")

product_id = []
for i in range(1, 20):
    params['page'] = i
    response = requests.get('https://tiki.vn/api/personalish/v1/blocks/listings', headers=headers, params=params, cookies=cookies)
    if response.status_code == 200:
        print('request success!!!')
        for record in response.json().get('data'):
            product_id.append({'id': record.get('id')})
    time.sleep(random.randrange(3, 10))

df = pd.DataFrame(product_id)
current_date = datetime.now().strftime("%d%m%y")
csv_buffer = df.to_csv(index=False).encode('utf-8')
file_name = f'product_id_{name}_{current_date}.csv'

##client boto3
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',     
    aws_access_key_id='admin',    
    aws_secret_access_key='password123', 
    region_name='us-east-1'                   
)

bucket_name = 'tiki-data-product-id'

# 3. Kiểm tra và tạo bucket nếu chưa có
try:
    s3_client.head_bucket(Bucket=bucket_name)
except ClientError as e:
    error_code = e.response['Error']['Code']
    if error_code == '404':
        print(f"Bucket '{bucket_name}' chưa tồn tại, đang tiến hành tạo mới...")
        s3_client.create_bucket(Bucket=bucket_name)
    else:
        print(f"Lỗi khi kiểm tra bucket: {e}")

# 4. Đẩy thẳng dữ liệu từ bộ nhớ lên MinIO
try:
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=csv_buffer
    )
    print(f"Tải thành công '{file_name}' thẳng lên bucket '{bucket_name}' (không dùng file local)!")
except Exception as err:
    print(f"Xảy ra lỗi khi upload: {err}")
