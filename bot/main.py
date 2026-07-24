import sys
import io
import os

# Fix UnicodeEncodeError in Windows console for Vietnamese characters
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import discord
from discord.ext import commands
from dotenv import load_dotenv

from nlp_engine import analyze_intent
from database import fetch_valid_categories, get_candidate_products
from recommender import get_top_recommendations

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

# Cấp quyền cho Bot đọc tin nhắn
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

VALID_CATEGORIES = []

@bot.event
async def on_ready():
    global VALID_CATEGORIES
    print(f'Bot {bot.user.name} đã sẵn sàng hoạt động!')
    VALID_CATEGORIES = fetch_valid_categories()
    print(f'Đã tải {len(VALID_CATEGORIES)} danh mục từ cơ sở dữ liệu.')

@bot.command(name='goi-y')
async def recommend(ctx, *, user_query: str):
    await ctx.send(f"Đang dùng AI phân tích yêu cầu: *'{user_query}'*...")
    
    # 1. NLP Parse
    intent = analyze_intent(user_query, VALID_CATEGORIES)
    if not intent:
        await ctx.send("Xin lỗi, AI đang gặp sự cố khi phân tích câu lệnh. Vui lòng thử lại!")
        return
        
    category_key = intent.get("category_key")
    max_price = intent.get("max_price")
    keywords = intent.get("keywords", [])
    
    # 2. Kéo dữ liệu từ DB
    products = get_candidate_products(category_key, max_price)
    if not products:
        await ctx.send(f"Rất tiếc, mình không tìm thấy sản phẩm nào thuộc `{category_key}` với giá <= {max_price or 'bất kỳ'} đ.")
        return
        
    # 3. Tính điểm Dynamic Score và lọc Top 3
    top_3 = get_top_recommendations(products, keywords, max_price, top_n=3)
    
    if not top_3:
        await ctx.send("Không có sản phẩm nào phù hợp để gợi ý.")
        return
    
    # 4. Hiển thị lên Discord bằng Embed
    embed = discord.Embed(
        title="🎯 TOP 3 Gợi ý dành riêng cho bạn", 
        description=f"**Ý định hiểu được:**\n- Danh mục: `{category_key}`\n- Giá tối đa: `{max_price or 'Không giới hạn'} đ`\n- Từ khóa: `{', '.join(keywords)}`",
        color=discord.Color.green()
    )
    
    for i, p in enumerate(top_3):
        name = p.get('name', 'Sản phẩm chưa có tên')
        
        try:
            price = float(p.get('price', 0))
        except (ValueError, TypeError):
            price = 0
            
        discount = p.get('discount_rate', 0)
        p_id = p.get('id', '')
        link = f"https://tiki.vn/product-p{p_id}.html"
        
        # Rút gọn tên nếu quá dài
        short_name = (name[:75] + '...') if len(name) > 75 else name
        
        info = (
            f"💰 **Giá:** {price:,.0f} đ\n"
            f"🔥 **Giảm giá:** {discount}%\n"
            f"⭐ **Điểm hệ thống:** {p.get('total_score'):,.0f} (Base: {p.get('base_score'):.0f} | Dynamic: {p.get('dynamic_score'):.0f})\n"
            f"[👉 Bấm vào đây để xem chi tiết]({link})"
        )
        
        embed.add_field(name=f"#{i+1} - {short_name}", value=info, inline=False)
        
    await ctx.send(embed=embed)

if __name__ == "__main__":
    if not TOKEN or TOKEN == "your_discord_token_here":
        print("LỖI: Vui lòng cấu hình DISCORD_TOKEN trong file .env!")
    else:
        print("Đang khởi động bot...")
        bot.run(TOKEN)
