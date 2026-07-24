def calculate_dynamic_score(product, keywords, max_price=None):
    score = 0
    name = (product.get("name") or "").lower()
    
    # 1. Khớp từ khóa trong tên sản phẩm
    if keywords:
        for kw in keywords:
            if kw.lower() in name:
                # Tăng 500 điểm cho mỗi từ khóa khớp để tạo ảnh hưởng đủ lớn
                score += 500
                
    # 2. Phân tích giá cả
    try:
        price = float(product.get("price", 0))
    except (ValueError, TypeError):
        price = 0
        
    if max_price and price > 0:
        # Nếu sản phẩm nằm trong khoảng 70% - 100% ngân sách (Sát với ngân sách user nhất)
        if max_price * 0.7 <= price <= max_price:
            score += 300
        # Nếu sản phẩm rẻ hơn nhiều so với ngân sách (< 70%)
        elif price < max_price * 0.7:
            score += 150
            
    # 3. Thêm điểm nếu đang có giảm giá mạnh (trên 20%)
    discount = product.get("discount_rate", 0)
    if discount and float(discount) >= 20:
        score += 200
            
    return score

def get_top_recommendations(products, keywords, max_price=None, top_n=3):
    for p in products:
        # Đảm bảo base_score là kiểu số
        base_score = float(p.get("base_score") or 0)
        
        dynamic_score = calculate_dynamic_score(p, keywords, max_price)
        
        # CÔNG THỨC: Total Score = Base_score * 1 + Dynamic_score * 2
        total_score = base_score * 1.0 + dynamic_score * 2.0
        
        p["dynamic_score"] = dynamic_score
        p["total_score"] = total_score
        
    # Sắp xếp giảm dần theo total_score
    sorted_products = sorted(products, key=lambda x: x["total_score"], reverse=True)
    
    return sorted_products[:top_n]
