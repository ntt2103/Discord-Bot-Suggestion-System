import requests
import pandas as pd
import time
import random
import sys
from math import ceil
from datetime import datetime
from pathlib import Path
from requests import RequestException
from tqdm import tqdm

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
    'Accept-Language': 'vi-VN,vi;q=0.8,en-US;q=0.5,en;q=0.3',
    'Referer': 'https://tiki.vn/?src=header_tiki',
    'x-guest-token': 'Cn1sZNr2UaBc3QWxuVI8XSGYb4EADdqw',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}
params = {
    'product_id': '58259141',
    'sort': 'score|desc,id|desc,stars|all',
    'page': '1',
    'limit': '10',
    'include': 'comments'
}

REVIEWS_URL = 'https://tiki.vn/api/v2/reviews'
PRODUCT_IDS_PATH = Path('product_id_ncds.csv')
PRODUCT_DATA_PATH = Path('crawled_data_ncds.csv')
OUTPUT_PATH = Path('comments_data_ncds.csv')
FAILED_PATH = Path('failed_comment_product_ids.csv')
MAX_PAGES_PER_PRODUCT = 20
REQUEST_DELAY_RANGE = (1, 3)
REQUEST_TIMEOUT = (5, 10)


if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')


def comment_parser(json):
    created_by = json.get('created_by') or {}

    d = dict()
    d['id'] = json.get('id')
    d['product_id'] = json.get('product_id')
    d['title'] = json.get('title')
    d['content'] = json.get('content')
    d['thank_count'] = json.get('thank_count')
    d['customer_id']  = json.get('customer_id')
    d['rating'] = json.get('rating')
    d['created_at'] = json.get('created_at')
    d['customer_name'] = created_by.get('name')
    d['purchased_at'] = created_by.get('purchased_at')
    return d


def fetch_review_page(session, pid, page):
    params['product_id'] = pid
    params['page'] = page
    response = session.get(REVIEWS_URL, headers=headers, params=params, cookies=cookies, timeout=REQUEST_TIMEOUT)
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


def load_product_ids():
    df_id = pd.read_csv(PRODUCT_IDS_PATH)
    p_ids = df_id['id'].dropna().drop_duplicates().tolist()
    review_counts = {}

    if PRODUCT_DATA_PATH.exists():
        df_product = pd.read_csv(PRODUCT_DATA_PATH)
        if {'id', 'review_count'}.issubset(df_product.columns):
            review_counts = dict(zip(df_product['id'], df_product['review_count'].fillna(0)))
            df_has_reviews = df_product[df_product['review_count'].fillna(0) > 0]
            ids_with_reviews = set(df_has_reviews['id'].dropna().tolist())
            p_ids = [pid for pid in p_ids if pid in ids_with_reviews]
            print('Skip products with 0 reviews. {} products remaining.'.format(len(p_ids)))

    return p_ids, review_counts


def save_rows(rows, output_path):
    if not rows:
        return None

    df_comment = pd.DataFrame(rows).drop_duplicates(subset=['id'])
    try:
        df_comment.to_csv(output_path, index=False, encoding='utf-8-sig')
        return output_path
    except PermissionError:
        fallback_path = Path('comments_data_ncds_{}.csv'.format(datetime.now().strftime('%Y%m%d_%H%M%S')))
        df_comment.to_csv(fallback_path, index=False, encoding='utf-8-sig')
        print('File {} is being used. Saved to {} instead.'.format(output_path, fallback_path))
        return fallback_path


def main():
    p_ids, review_counts = load_product_ids()
    result = []
    failed = []
    session = requests.Session()

    for pid in tqdm(p_ids, total=len(p_ids)):
        print('Crawl comment for product {}'.format(pid))
        review_count = int(review_counts.get(pid, 0) or 0)
        max_pages = MAX_PAGES_PER_PRODUCT
        if review_count > 0:
            max_pages = min(MAX_PAGES_PER_PRODUCT, ceil(review_count / int(params['limit'])))

        for page in range(1, max_pages + 1):
            try:
                data, error = fetch_review_page(session, pid, page)
            except RequestException as exc:
                data, error = None, 'request error: {}'.format(exc)

            if error:
                print('Product {}, page {} failed: {}'.format(pid, page, error))
                failed.append({'id': pid, 'page': page, 'error': error})
                break

            comments = data.get('data') or []
            if not comments:
                print('Product {}, page {} has no comments. Skip to next product.'.format(pid, page))
                break

            print('Product {}, page {}: got {} comments'.format(pid, page, len(comments)))
            for comment in comments:
                result.append(comment_parser(comment))

            paging = data.get('paging') or {}
            current_page = paging.get('current_page') or page
            last_page = paging.get('last_page')
            if last_page and current_page >= last_page:
                break

            time.sleep(random.uniform(*REQUEST_DELAY_RANGE))

        if result:
            save_rows(result, OUTPUT_PATH)

    saved_path = save_rows(result, OUTPUT_PATH)
    if failed:
        pd.DataFrame(failed).to_csv(FAILED_PATH, index=False, encoding='utf-8-sig')
        print('Saved {} failed pages to {}'.format(len(failed), FAILED_PATH))
    print('Saved {} comments to {}'.format(len(pd.DataFrame(result).drop_duplicates(subset=['id'])) if result else 0, saved_path))


if __name__ == '__main__':
    main()
