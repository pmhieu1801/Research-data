import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

# ... (phần code còn lại đến hàm process_single_term giữ nguyên) ...

def process_single_term(driver, term):
    # ... (phần code gọi API giữ nguyên) ...

    # --- Chụp ảnh màn hình VÀ LỌC NGÔN NGỮ ---
    ads_data = []
    print(f"Found {len(data)} ads for '{term}'. Filtering for Vietnamese language and capturing screenshots...")
    
    vietnamese_ads_count = 0
    for ad in data:
        ad_text = ad.get('page_name', '') # Lấy tên Trang để kiểm tra

        # Bắt đầu kiểm tra ngôn ngữ
        try:
            # Nếu văn bản không rỗng và được phát hiện là tiếng Việt ('vi')
            if ad_text and detect(ad_text) == 'vi':
                vietnamese_ads_count += 1
                screenshot_path = capture_ad_snapshot(driver, ad.get('ad_snapshot_url'), screenshots_dir, ad.get('id'))
                ads_data.append({
                    'ad_id': ad.get('id'), 'page_id': ad.get('page_id'), 'page_name': ad.get('page_name'),
                    'snapshot_url': ad.get('ad_snapshot_url'), 'local_screenshot_path': screenshot_path,
                    'spend_lower_bound': ad.get('spend', {}).get('lower_bound'), 'spend_upper_bound': ad.get('spend', {}).get('upper_bound'),
                    'impressions_lower_bound': ad.get('impressions', {}).get('lower_bound'), 'impressions_upper_bound': ad.get('impressions', {}).get('upper_bound'),
                    'currency': ad.get('currency')
                })
        except LangDetectException:
            # Bỏ qua nếu không thể phát hiện ngôn ngữ (văn bản quá ngắn, v.v.)
            continue

    # --- Lưu kết quả ---
    if ads_data:
        df = pd.DataFrame(ads_data)
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"✅ Filtered and saved data for {len(ads_data)} Vietnamese ads to {output_csv}")
    else:
        print(f"No Vietnamese language ads found for the term '{term}'.")
# Tải các biến môi trường từ file .env
load_dotenv()

# Lấy thông tin đăng nhập và access token
ACCESS_TOKEN = os.getenv('FB_ACCESS_TOKEN')
FB_EMAIL = os.getenv('FB_EMAIL')
FB_PASSWORD = os.getenv('FB_PASSWORD')

# API URL
AD_LIBRARY_API_URL = 'https://graph.facebook.com/v19.0/ads_archive'

# ✅ SỬA ĐỔI 1: TẠO DANH SÁCH CÁC TỪ KHÓA BẠN MUỐN TÌM
SEARCH_TERMS_LIST = [
# === 1. Giảm Giá / Khuyến Mãi (Sales / Promotions) ===
    'giảm giá', 'khuyến mãi', 'ưu đãi', 'siêu sale', 'flash sale', 
    'sale sốc', 'sale khủng', 'sale sập sàn', 'xả kho', 'thanh lý', 
    'giảm 50%', 'giảm 30%', 'mua 1 tặng 1', 'mua 2 tặng 1', 'quà tặng', 
    'tặng kèm', 'deal hời', 'hot deal', 'big sale', 'mega sale', 
    'săn sale', 'săn deal', 'voucher', 'mã giảm giá', 'giá tốt', 
    'giá rẻ', 'rẻ vô địch', 'giá hủy diệt', 'tri ân', 'sinh nhật',

    # === 2. Giá Cả / Định Lượng (Pricing / Scarcity) ===
    'chỉ từ', '99k', '199k', '299k', '49k', '1k', '9k',
    'đồng giá', '100k', '50k', 'combo', 'trọn bộ', 'giá chỉ', 
    'miễn phí', '0 đồng', 'giá sốc', 'số lượng có hạn', 'có hạn',

    # === 3. Vận Chuyển (Shipping) ===
    'freeship', 'free ship', 'miễn phí vận chuyển', 'miễn ship', 
    'giao hàng', 'giao hàng nhanh', 'hỏa tốc', 'ship hỏa tốc', 
    'ship COD', 'COD', 'toàn quốc', 'kiểm tra hàng',

    # === 4. Kêu Gọi Hành Động (Call to Action - CTA) ===
    'mua ngay', 'đặt ngay', 'chốt đơn', 'xem ngay', 'xem thêm',
    'đăng ký', 'đăng ký ngay', 'bình luận', 'chấm', '.', # Dấu chấm "." là một CTA rất phổ biến
    'ib', 'inbox', 'nhắn tin', 'gửi tin nhắn', 'tư vấn', 
    'liên hệ', 'gọi ngay', 'nhanh tay', 'đặt hàng', 'chốt liền',

    # === 5. Từ Khóa "Mồi" / Gấp Gáp (Urgency / Clickbait) ===
    'hôm nay', 'chỉ hôm nay', 'cơ hội', 'cuối cùng', 'sắp hết', 
    'đừng bỏ lỡ', 'gấp', 'hot', 'new', 'mới', 'mới về', 
    'hàng mới', 'tin vui', 'bất ngờ', 'đặc biệt', 'duy nhất',

    # === 6. Ngành Hàng Phổ Biến (Common Industries) ===
    'thời trang', 'mỹ phẩm', 'làm đẹp', 'chăm sóc da', 'son', 
    'váy', 'đầm', 'áo', 'quần', 'giày', 'túi xách', 
    'phụ kiện', 'công nghệ', 'điện thoại', 'laptop', 'gia dụng', 
    'nhà bếp', 'nội thất', 'sức khỏe', 'thực phẩm', 'ăn vặt',
    'khóa học', 'bất động sản', 'BĐS', 'chung cư', 'nhà đất',

    # === 7. Chất Lượng / Cam Kết (Quality / Assurance) ===
    'chính hãng', 'cao cấp', 'uy tín', 'chất lượng', 'bảo hành', 
    'xách tay', 'hàng hiệu', 'nhập khẩu', 'độc quyền', '100%',
    'cam kết', 'hoàn tiền', 'hiệu quả',

    # === 8. Tương Tác (Social Interaction) ===
    'livestream', 'like', 'share', 'chia sẻ', 'tag', 
    'bạn bè', 'trúng thưởng', 'give away', 'minigame',

    # === 9. Từ khóa chung (General) ===
    'và', 'của', 'là', 'cho', 'tại', 'shop', 'cửa hàng'
]

# Single screenshots directory for all search terms
SCREENSHOTS_DIR = 'data/ad_screenshots/'
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)


def _sanitize_for_filename(s: str) -> str:
    """Make a simple filename-safe version of the search term."""
    if not s:
        return 'term'
    return ''.join(c if (c.isalnum() or c in ('-', '_')) else '_' for c in s)

def capture_ad_snapshot(driver, url, dest_folder, ad_id):
    if not url: return None
    try:
        driver.get(url)
        time.sleep(4)
        filepath = os.path.join(dest_folder, f"{ad_id}.png")
        driver.save_screenshot(filepath)
        print(f"  -> Captured screenshot for ad_id: {ad_id}")
        return filepath
    except Exception as e:
        print(f"  -> Failed to capture screenshot for ad_id {ad_id}: {e}")
        return None

def process_single_term(driver, term):
    """Hàm này xử lý toàn bộ logic cho một từ khóa duy nhất."""
    
    # Output CSV (per-term) but screenshots will be saved to a single folder
    output_csv = f'facebook_ads_{_sanitize_for_filename(term)}.csv'
    screenshots_dir = SCREENSHOTS_DIR

    search_params = {
        'access_token': ACCESS_TOKEN,
        'ad_type': 'ALL',
        'ad_reached_countries': '["VN"]',
        'search_terms': term, # Sử dụng từ khóa từ vòng lặp
        'fields': 'id,page_id,page_name,ad_snapshot_url,spend,impressions,currency',
        'limit': 50
    }

    # --- Gọi API ---
    print(f"Querying API for term: '{term}'...")
    try:
        resp = requests.get(AD_LIBRARY_API_URL, params=search_params, timeout=30)
        resp.raise_for_status()
        data = resp.json().get('data', [])
        if not data:
            print(f"No ads returned for '{term}'. Skipping.")
            return # Bỏ qua và chuyển sang từ khóa tiếp theo
    except requests.exceptions.RequestException as e:
        print(f"API request failed for term '{term}': {e}")
        return # Bỏ qua

    # --- Chụp ảnh màn hình ---
    ads_data = []
    print(f"Found {len(data)} ads for '{term}'. Starting screenshot capture...")
    term_safe = _sanitize_for_filename(term)
    for ad in data:
        # Prefix filename with the search term to avoid collisions
        filename_ad_id = f"{term_safe}_{ad.get('id')}"
        screenshot_path = capture_ad_snapshot(driver, ad.get('ad_snapshot_url'), screenshots_dir, filename_ad_id)
        ads_data.append({
            'ad_id': ad.get('id'), 'page_id': ad.get('page_id'), 'page_name': ad.get('page_name'),
            'snapshot_url': ad.get('ad_snapshot_url'), 'local_screenshot_path': screenshot_path,
            'spend_lower_bound': ad.get('spend', {}).get('lower_bound'), 'spend_upper_bound': ad.get('spend', {}).get('upper_bound'),
            'impressions_lower_bound': ad.get('impressions', {}).get('lower_bound'), 'impressions_upper_bound': ad.get('impressions', {}).get('upper_bound'),
            'currency': ad.get('currency')
        })

    # --- Lưu kết quả ---
    if ads_data:
        df = pd.DataFrame(ads_data)
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"✅ Saved data for {len(ads_data)} ads to {output_csv}")

def main():
    print('\nStarting toolv3.py...')
    if not all([ACCESS_TOKEN, FB_EMAIL, FB_PASSWORD]):
        print("Error: Make sure FB_ACCESS_TOKEN, FB_EMAIL, and FB_PASSWORD are set.")
        return

    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--window-size=800,1200")
    chrome_options.add_argument("--disable-notifications")

    driver = None
    try:
        # === Đăng nhập một lần duy nhất ===
        print("Initializing browser and logging into Facebook...")
        service = Service()
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.get("https://www.facebook.com/")
        
        wait = WebDriverWait(driver, 10)
        email_input = wait.until(EC.presence_of_element_located((By.ID, "email")))
        pass_input = driver.find_element(By.ID, "pass")
        email_input.send_keys(FB_EMAIL)
        pass_input.send_keys(FB_PASSWORD)
        driver.find_element(By.NAME, "login").click()
        
        wait.until(EC.url_contains("facebook.com"))
        print("Login successful!")
        
        # ✅ SỬA ĐỔI 3: TẠO VÒNG LẶP CHÍNH
        for term in SEARCH_TERMS_LIST:
            print(f"\n{'='*15} Processing Term: {term.upper()} {'='*15}")
            process_single_term(driver, term) # Gọi hàm xử lý cho mỗi từ khóa

    finally:
        if driver:
            driver.quit()
        print("\nProcess finished.")

if __name__ == "__main__":
    main()
