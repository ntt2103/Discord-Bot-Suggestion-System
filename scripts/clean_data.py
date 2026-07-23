import pandas as pd

df = pd.read_csv("./data/raw/crawled_product/crawled_adapter-cu-sac_230726.csv")
df = df.drop(columns=["sku", "short_description", "price_usd", "order_count", "is_visible", "inventory_status", "stock_item_qty", "stock_item_max_sale_qty", "product_name"])
df = df.drop(columns= "quantity_sold_text")
df = df.fillna({"quantity_sold_value" : 0})
df["quantity_sold_value"] = df["quantity_sold_value"].astype(int)
df = df.drop_duplicates()

df.to_csv("./data/cleaned/cleaned_adapter_230726.csv", index=False, encoding='utf-8-sig')