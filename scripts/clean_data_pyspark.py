import os
import glob
import boto3
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, input_file_name, regexp_extract
from botocore.exceptions import ClientError

import sys

# Tự động tìm và cấu hình JAVA_HOME nếu terminal chưa nhận diện được (chỉ áp dụng cho Windows)
if "JAVA_HOME" not in os.environ and sys.platform == "win32":
    java_paths = glob.glob(r"C:\Program Files\Eclipse Adoptium\jdk-17*") + \
                 glob.glob(r"C:\Program Files\Java\jdk-17*") + \
                 glob.glob(r"C:\Program Files\Java\jdk-11*") + \
                 glob.glob(r"C:\Program Files\Java\jdk1.8*")
    
    if java_paths:
        os.environ["JAVA_HOME"] = java_paths[0]
        os.environ["PATH"] = os.path.join(java_paths[0], "bin") + os.pathsep + os.environ["PATH"]
        print(f"Đã tự động cấu hình JAVA_HOME = {java_paths[0]}")
    else:
        print("CẢNH BÁO: Không tìm thấy bản Java 8/11/17 phù hợp cho PySpark. Hãy chắc chắn bạn đã cài đặt!")

# Tự động tải winutils.exe (yêu cầu của PySpark trên Windows)
if sys.platform == "win32":
    import urllib.request
    hadoop_home = os.path.join(os.path.expanduser("~"), ".hadoop")
    bin_dir = os.path.join(hadoop_home, "bin")
    os.makedirs(bin_dir, exist_ok=True)
    winutils_path = os.path.join(bin_dir, "winutils.exe")
    hadoop_dll_path = os.path.join(bin_dir, "hadoop.dll")

    try:
        if not os.path.exists(winutils_path):
            urllib.request.urlretrieve("https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/winutils.exe", winutils_path)
        if not os.path.exists(hadoop_dll_path):
            urllib.request.urlretrieve("https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/hadoop.dll", hadoop_dll_path)
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ["PATH"]
    except Exception as e:
        pass

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=os.environ.get("MINIO_URL", 'http://localhost:9000'),     
        aws_access_key_id='admin',    
        aws_secret_access_key='password123', 
        region_name='us-east-1'                   
    )

def init_minio_bucket(bucket_name):
    s3_client = get_s3_client()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Tạo bucket {bucket_name}...")
            s3_client.create_bucket(Bucket=bucket_name)

def download_raw_data(bucket, prefix, local_dir):
    s3 = get_s3_client()
    os.makedirs(local_dir, exist_ok=True)
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', [])
    for obj in objects:
        key = obj['Key']
        if key.endswith('.csv'):
            local_path = os.path.join(local_dir, os.path.basename(key))
            s3.download_file(bucket, key, local_path)

def upload_clean_data(bucket, prefix, local_dir):
    s3 = get_s3_client()
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    for file_name in os.listdir(local_dir):
        if file_name.endswith('.csv'):
            local_path = os.path.join(local_dir, file_name)
            s3_key = f"{prefix}/cleaned_products_{timestamp}.csv"
            s3.upload_file(local_path, bucket, s3_key)
            break # Chỉ upload 1 file do đã dùng coalesce(1)

def main():
    # 1. Tải dữ liệu thô từ MinIO về máy local qua Boto3 để tránh lỗi S3A của PySpark trên Windows
    print("Đang tải dữ liệu thô từ MinIO về máy local...")
    raw_dir = "tmp_data/raw"
    clean_dir = "tmp_data/cleaned"
    shutil.rmtree(raw_dir, ignore_errors=True)
    shutil.rmtree(clean_dir, ignore_errors=True)
    
    download_raw_data("tiki-data-raw", "crawled_products", raw_dir)

    output_bucket = "tiki-data"
    init_minio_bucket(output_bucket)

    print("Đang khởi tạo Spark Session...")
    # Chỉ cần JDBC Postgres, bỏ qua các thư viện S3A để tránh xung đột
    spark = SparkSession.builder \
        .appName("TikiDataCleaner") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC") \
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC") \
        .getOrCreate()
    print("Spark Session đã được khởi tạo thành công.")

    try:
        # Đọc dữ liệu local với chuẩn UTF-8
        df = spark.read.option("header", "true") \
                  .option("inferSchema", "true") \
                  .option("encoding", "utf-8") \
                  .csv(f"{raw_dir}/*.csv")
    except Exception as e:
        print(f"Lỗi khi đọc file csv: {e}")
        return

    # Lấy category_key từ tên file (vd: crawled_adapter-cu-sac_230726.csv)
    df = df.withColumn("file_path", input_file_name())
    df = df.withColumn("category_key", regexp_extract(col("file_path"), r"crawled_(.*?)_\d+\.csv", 1))
    df = df.drop("file_path")

    cols_to_drop = [
        "sku", "short_description", "price_usd", "order_count", 
        "is_visible", "inventory_status", "stock_item_qty", 
        "stock_item_max_sale_qty", "product_name", "quantity_sold_text"
    ]
    
    existing_cols = df.columns
    cols_to_drop = [c for c in cols_to_drop if c in existing_cols]
    
    df = df.drop(*cols_to_drop)

    if "quantity_sold_value" in df.columns:
        df = df.withColumn("quantity_sold_value", coalesce(col("quantity_sold_value"), lit(0)).cast("float").cast("int"))
    else:
        df = df.withColumn("quantity_sold_value", lit(0))

    df = df.dropDuplicates(["id"])

    if "review_count" in df.columns:
        df = df.withColumn("review_count", coalesce(col("review_count"), lit(0)).cast("float").cast("int"))
    else:
        df = df.withColumn("review_count", lit(0))

    if "discount_rate" in df.columns:
        df = df.withColumn("discount_rate", coalesce(col("discount_rate"), lit(0)).cast("float"))
    else:
        df = df.withColumn("discount_rate", lit(0.0))

    df = df.withColumn(
        "base_score", 
        (col("quantity_sold_value") * 0.5) + (col("review_count") * 0.3) + (col("discount_rate") * 0.2)
    )

    print("Đang ghi dữ liệu vào PostgreSQL...")
    db_url = os.environ.get("DB_URL", "jdbc:postgresql://localhost:5432/tiki_db")
    db_properties = {
        "user": "admin",
        "password": "password123",
        "driver": "org.postgresql.Driver"
    }
    
    try:
        # 1. Ghi dữ liệu vào bảng tạm
        df.write.jdbc(url=db_url, table="cleaned_products_temp", mode="overwrite", properties=db_properties)
        
        # 2. Sử dụng JDBC của PySpark để chạy câu lệnh UPSERT
        driver = spark._jvm.java.sql.DriverManager
        conn = driver.getConnection(db_url, db_properties["user"], db_properties["password"])
        stmt = conn.createStatement()
        
        # Đảm bảo bảng chính tồn tại (với cấu trúc giống bảng tạm)
        stmt.execute("CREATE TABLE IF NOT EXISTS cleaned_products AS SELECT * FROM cleaned_products_temp WHERE 1=0")
        
        # Thiết lập Khóa Chính (Primary Key) nếu chưa có
        try:
            stmt.execute("ALTER TABLE cleaned_products ADD PRIMARY KEY (id)")
        except Exception:
            pass # Lỗi nghĩa là khóa chính đã tồn tại
            
        # Build câu lệnh UPSERT động dựa theo schema hiện hành của DataFrame
        columns = df.columns
        col_names = ", ".join([f'"{c}"' for c in columns])
        excluded_cols = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in columns if c != "id"])
        
        upsert_query = f"""
            INSERT INTO cleaned_products ({col_names})
            SELECT {col_names} FROM cleaned_products_temp
            ON CONFLICT (id) DO UPDATE SET
            {excluded_cols}
        """
        stmt.execute(upsert_query)
        
        # Xoá bảng tạm
        stmt.execute("DROP TABLE cleaned_products_temp")
        
        stmt.close()
        conn.close()
        print("Ghi và Cập nhật (UPSERT) PostgreSQL thành công!")
    except Exception as e:
        print(f"Lỗi ghi PostgreSQL: {e}")

    print("Đang xuất dữ liệu ra CSV...")
    try:
        # Gộp tất cả partition thành 1 file duy nhất bằng coalesce(1)
        # Sử dụng encoding UTF-8 để không bị lỗi font tiếng Việt
        df.coalesce(1).write.option("header", "true") \
                            .option("encoding", "utf-8") \
                            .mode("overwrite").csv(clean_dir)
        # Upload lên MinIO bằng Boto3
        print("Đang upload kết quả lên MinIO (bucket tiki-data)...")
        upload_clean_data(output_bucket, "cleaned_products", clean_dir)
        print("Ghi MinIO thành công!")
    except Exception as e:
        print(f"Lỗi xuất file hoặc upload: {e}")
    
    spark.stop()
    
    # Dọn dẹp
    shutil.rmtree("tmp_data", ignore_errors=True)
    print("XONG!")

if __name__ == "__main__":
    main()
