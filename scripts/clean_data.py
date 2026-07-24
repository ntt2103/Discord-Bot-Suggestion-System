import pandas as pd
import argparse
import boto3
import io
from botocore.exceptions import ClientError

def clean_data(input_bucket, input_key, output_bucket, output_key):
    # Khởi tạo client boto3 kết nối đến MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',     
        aws_access_key_id='admin',    
        aws_secret_access_key='password123', 
        region_name='us-east-1'                   
    )

    print(f"Đang đọc dữ liệu từ {input_bucket}/{input_key} trên MinIO...")
    # Tải file từ MinIO vào bộ nhớ
    response = s3_client.get_object(Bucket=input_bucket, Key=input_key)
    df = pd.read_csv(response['Body'])

    # Thực hiện clean data
    cols_to_drop = ["sku", "short_description", "price_usd", "order_count", "is_visible", "inventory_status", "stock_item_qty", "stock_item_max_sale_qty", "product_name"]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])
    
    if "quantity_sold_text" in df.columns:
        df = df.drop(columns="quantity_sold_text")
        
    if "quantity_sold_value" in df.columns:
        df = df.fillna({"quantity_sold_value" : 0})
        df["quantity_sold_value"] = df["quantity_sold_value"].astype(int)
        
    df = df.drop_duplicates()

    # Kiểm tra và tạo output bucket nếu chưa có
    try:
        s3_client.head_bucket(Bucket=output_bucket)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Bucket '{output_bucket}' chưa tồn tại, đang tiến hành tạo mới...")
            s3_client.create_bucket(Bucket=output_bucket)
        else:
            print(f"Lỗi khi kiểm tra bucket: {e}")

    # Chuyển dataframe thành bytes để đẩy lên MinIO
    print(f"Đang lưu dữ liệu đã clean lên {output_bucket}/{output_key}...")
    csv_buffer = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
    
    s3_client.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=csv_buffer
    )
    print("Hoàn tất clean và upload!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean data from MinIO and save to MinIO")
    parser.add_argument("--input_bucket", type=str, required=True, help="Tên bucket chứa file raw (vd: tiki-data-raw)")
    parser.add_argument("--input_key", type=str, required=True, help="Đường dẫn file raw trong bucket (vd: crawled_product/file.csv)")
    parser.add_argument("--output_bucket", type=str, required=True, help="Tên bucket chứa file cleaned (vd: tiki-data-cleaned)")
    parser.add_argument("--output_key", type=str, required=True, help="Đường dẫn file cleaned trong bucket (vd: cleaned_product/file.csv)")
    
    args = parser.parse_args()
    
    clean_data(args.input_bucket, args.input_key, args.output_bucket, args.output_key)