#!/usr/bin/env python3
"""
toolv3.py - Optimized for long-running crawl (stable for many hours / thousands of items).

Main improvements for long runs:
 - requests.Session with Retry/backoff for API & image downloads
 - RotatingFileHandler logging
 - Checkpointing (state.json) for resume capability
 - Append-mode merged.csv (no full-file concat each term)
 - Periodic webdriver restart to avoid memory leak
 - Disk free check and retention policy hook
 - Signal handling for graceful shutdown
 - Garbage collection & metrics hooks
"""

import os
import time
import json
import signal
import logging
import shutil
import gc
from pathlib import Path
from typing import Optional, List, Dict

import requests
import pandas as pd
from dotenv import load_dotenv
from urllib.parse import urlparse

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

# ---- Config ----
load_dotenv()
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
FB_EMAIL = os.getenv("FB_EMAIL")
FB_PASSWORD = os.getenv("FB_PASSWORD")

AD_LIBRARY_API_URL = "https://graph.facebook.com/v19.0/ads_archive"

# Operational parameters (tune these for your infra)
SEARCH_TERMS_LIST: List[str] = [
    'giảm giá','khuyến mãi','ưu đãi','siêu sale','flash sale','mua ngay','miễn phí vận chuyển'
]

DATA_DIR = Path("data")
SCREENSHOTS_DIR = DATA_DIR / "ad_screenshots"
OUTPUT_DIR = DATA_DIR / "outputs"
STATE_FILE = OUTPUT_DIR / "state.json"
MERGED_CSV = OUTPUT_DIR / "merged.csv"
LOG_FILE = OUTPUT_DIR / "toolv3.log"

SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Long-run tuning
MAX_ADS_PER_DRIVER_RESTART = int(os.getenv("MAX_ADS_PER_DRIVER_RESTART", "200"))
MAX_TERMS_PER_RUN = int(os.getenv("MAX_TERMS_PER_RUN", "10000"))  # large number, control externally
MAX_RUNTIME_SECONDS = int(os.getenv("MAX_RUNTIME_SECONDS", str(60*60*10)))  # default ~10 hours
MIN_FREE_DISK_GB = float(os.getenv("MIN_FREE_DISK_GB", "5.0"))
BATCH_WRITE_EVERY_N_ROWS = int(os.getenv("BATCH_WRITE_EVERY_N_ROWS", "50"))
IMAGE_RETENTION_DAYS = int(os.getenv("IMAGE_RETENTION_DAYS", "90"))

# Logging with rotation
logger = logging.getLogger("toolv3")
logger.setLevel(logging.INFO)
from logging.handlers import RotatingFileHandler
rh = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
rh.setFormatter(formatter)
logger.addHandler(rh)
console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)

# ---- HTTP session with retries ----
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; toolv3/1.0)"})
    return s

session = make_session()

# ---- State handling (checkpoint/resume) ----
state: Dict = {
    "processed_terms": [],   # list of terms fully processed
    "processed_ad_ids": []   # global list to avoid duplicates (keeps memory small; can rotate/persist)
}

def load_state():
    global state
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            logger.info(f"Loaded state: {len(state.get('processed_terms',[]))} terms processed, "
                        f"{len(state.get('processed_ad_ids',[]))} ad_ids recorded")
        except Exception as e:
            logger.warning(f"Failed to load state file: {e}")


def save_state():
    try:
        STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("State saved")
    except Exception as e:
        logger.error(f"Failed to save state: {e}")

# ---- Disk utilities ----
def has_enough_disk(min_free_gb: float = MIN_FREE_DISK_GB) -> bool:
    total, used, free = shutil.disk_usage(str(DATA_DIR))
    free_gb = free / (1024**3)
    logger.debug(f"Disk free: {free_gb:.2f} GB")
    return free_gb >= min_free_gb

def cleanup_old_images(retention_days: int = IMAGE_RETENTION_DAYS):
    """Optional quick retention: remove images older than retention_days."""
    try:
        cutoff = time.time() - retention_days*24*3600
        removed = 0
        for p in SCREENSHOTS_DIR.glob("*"):
            try:
                if p.is_file() and p.stat().st_mtime < cutoff:
                    p.unlink()
                    removed += 1
            except Exception:
                continue
        if removed:
            logger.info(f"Cleanup removed {removed} old images")
    except Exception as e:
        logger.debug(f"cleanup_old_images error: {e}")

# ---- Selenium driver management ----
def init_driver(headless: bool = True):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1200,1000")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-notifications")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def safe_quit_driver(driver):
    try:
        driver.quit()
    except Exception:
        pass

# ---- Image download & screenshot (uses session) ----
def _download_image(url: str, dest_path: Path, timeout: int = 15) -> Optional[str]:
    try:
        r = session.get(url, stream=True, timeout=timeout)
        r.raise_for_status()
        ct = r.headers.get("Content-Type", "")
        if "image" not in ct:
            return None
        ext = ct.split("/")[-1].split(";")[0] if "/" in ct else "jpg"
        path = dest_path.with_suffix("." + ext)
        with open(path, "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
        return str(path)
    except Exception as e:
        logger.debug(f"download_image failed {url}: {e}")
        return None

def _screenshot_page(driver, url: str, dest: Path) -> Optional[str]:
    try:
        driver.get(url)
        try:
            WebDriverWait(driver, 6).until(EC.presence_of_element_located((By.TAG_NAME, "img")))
        except Exception:
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(1)
        driver.save_screenshot(str(dest))
        return str(dest)
    except Exception as e:
        logger.debug(f"screenshot_page failed for {url}: {e}")
        return None

def capture_ad_snapshot(driver, url: Optional[str], dest_folder: Path, ad_id: str) -> Optional[str]:
    if not url:
        return None
    if not has_enough_disk():
        logger.error("Insufficient disk space, aborting image capture")
        return None
    dest_folder.mkdir(parents=True, exist_ok=True)
    parsed = urlparse(url)
    base = dest_folder / ad_id
    if parsed.path.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
        p = base.with_suffix(Path(parsed.path).suffix)
        img = _download_image(url, p)
        if img:
            return img
    try:
        head = session.head(url, timeout=8)
        ct = head.headers.get("Content-Type", "")
        if "image" in ct:
            ext = ct.split("/")[-1].split(";")[0]
            p = base.with_suffix("." + ext)
            img = _download_image(url, p)
            if img:
                return img
    except Exception:
        pass
    p = base.with_suffix(".png")
    ss = _screenshot_page(driver, url, p)
    return ss

# ---- API fetching with session & pagination ----
def fetch_ads_for_term(term: str, limit: int = 50, max_pages: int = 5):
    if not ACCESS_TOKEN:
        raise RuntimeError("FB_ACCESS_TOKEN not configured")
    fields = ",(".join([
        "id", "page_id", "page_name", "ad_snapshot_url",
        "ad_creative_body", "ad_creative_link_title", "ad_creative_link_description", "ad_creative_link_caption",
        "spend", "impressions", "currency"
    ]))
    params = {
        "access_token": ACCESS_TOKEN,
        "ad_type": "ALL",
        "ad_reached_countries": '[]',
        "search_terms": term,
        "fields": fields,
        "limit": limit
    }
    url = AD_LIBRARY_API_URL
    results = []
    pages = 0
    while url and pages < max_pages:
        try:
            resp = session.get(url, params=params if pages == 0 else None, timeout=30)
            resp.raise_for_status()
            j = resp.json()
            data = j.get("data", [])
            results.extend(data)
            paging = j.get("paging", {})
            url = paging.get("next")
            pages += 1
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"fetch_ads_for_term: failed page {pages+1} for '{term}': {e}")
            break
    return results

# ---- CSV helpers (append safe) ----
def append_to_csv(df: pd.DataFrame, path: Path):
    header = not path.exists()
    df.to_csv(path, mode="a", header=header, index=False, encoding="utf-8-sig")

# ---- Graceful shutdown ----
shutting_down = False
def _signal_handler(sig, frame):
    global shutting_down
    logger.info(f"Received signal {sig}, saving state and exiting...")
    shutting_down = True
    save_state()

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ---- Main processing loop with driver restart and batching ----
def process_single_term(driver, term: str, only_vietnamese: bool = True, max_pages: int = 5):
    term_safe = "".join(c if (c.isalnum() or c in ("-", "_")) else "_" for c in term).strip("_")
    out_csv = OUTPUT_DIR / f"facebook_ads_{term_safe}.csv"

    ads = fetch_ads_for_term(term, limit=50, max_pages=max_pages)
    if not ads:
        logger.info(f"No ads for '{term}'")
        state["processed_terms"].append(term)
        save_state()
        return []

    rows = []
    for ad in ads:
        if shutting_down:
            break
        ad_id = ad.get("id")
        if ad_id in state.get("processed_ad_ids", []):
            continue
        page_name = ad.get("page_name") or ""
        creative_body = ad.get("ad_creative_body") or ""
        link_title = ad.get("ad_creative_link_title") or ""
        link_desc = ad.get("ad_creative_link_description") or ""
        link_caption = ad.get("ad_creative_link_caption") or ""
        ad_text = creative_body or " ".join([link_title, link_desc, link_caption, page_name]).strip()
        if only_vietnamese:
            if not ad_text:
                continue
            try:
                if detect(ad_text) != "vi":
                    continue
            except LangDetectException:
                continue

        snapshot_url = ad.get("ad_snapshot_url")
        filename_prefix = f"{term_safe}_{ad_id}"
        screenshot_path = capture_ad_snapshot(driver, snapshot_url, SCREENSHOTS_DIR, filename_prefix)

        row = {
            "term": term,
            "ad_id": ad_id,
            "page_id": ad.get("page_id"),
            "page_name": page_name,
            "ad_text": creative_body,
            "link_title": link_title,
            "link_description": link_desc,
            "link_caption": link_caption,
            "snapshot_url": snapshot_url,
            "local_screenshot_path": screenshot_path,
            "spend_lower_bound": (ad.get("spend") or {}).get("lower_bound"),
            "spend_upper_bound": (ad.get("spend") or {}).get("upper_bound"),
            "impressions_lower_bound": (ad.get("impressions") or {}).get("lower_bound"),
            "impressions_upper_bound": (ad.get("impressions") or {}).get("upper_bound"),
            "currency": ad.get("currency")
        }
        rows.append(row)
        state.setdefault("processed_ad_ids", []).append(ad_id)

        # batch write if large
        if len(rows) >= BATCH_WRITE_EVERY_N_ROWS:
            df_batch = pd.DataFrame(rows)
            append_to_csv(df_batch, out_csv)
            append_to_csv(df_batch, MERGED_CSV)
            rows.clear()
            save_state()
            gc.collect()

    # final flush
    if rows:
        df = pd.DataFrame(rows)
        append_to_csv(df, out_csv)
        append_to_csv(df, MERGED_CSV)
        save_state()
        gc.collect()
    state["processed_terms"].append(term)
    save_state()
    return True

def main():
    start_time = time.time()
    load_state()
    cleanup_old_images()
    if not ACCESS_TOKEN:
        logger.error("FB_ACCESS_TOKEN missing. Set in .env or env vars.")
        return
    driver = None
    processed_ads_since_restart = 0
    processed_terms = 0
    try:
        driver = init_driver(headless=True)
        # optional login
        if FB_EMAIL and FB_PASSWORD:
            try:
                driver.get("https://www.facebook.com/")
                wait = WebDriverWait(driver, 10)
                email_input = wait.until(EC.presence_of_element_located((By.ID, "email")))
                pass_input = driver.find_element(By.ID, "pass")
                email_input.send_keys(FB_EMAIL)
                pass_input.send_keys(FB_PASSWORD)
                driver.find_element(By.NAME, "login").click()
                WebDriverWait(driver, 10).until(EC.url_contains("facebook.com"))
                logger.info("Logged into Facebook (selenium session)")
            except Exception as e:
                logger.warning(f"Facebook login failed or not necessary: {e}")

        for term in SEARCH_TERMS_LIST:
            if shutting_down:
                break
            if term in state.get("processed_terms", []):
                logger.info(f"Skipping already processed term: {term}")
                continue
            if (time.time() - start_time) > MAX_RUNTIME_SECONDS:
                logger.info("Reached max runtime, exiting loop")
                break
            try:
                result = process_single_term(driver, term, only_vietnamese=True, max_pages=5)
                processed_terms += 1
                # restart driver periodically
                processed_ads_since_restart += 1  # we count terms as proxy; could count actual ads processed if tracked
                if processed_ads_since_restart >= MAX_ADS_PER_DRIVER_RESTART:
                    logger.info("Restarting webdriver to avoid memory leak")
                    safe_quit_driver(driver)
                    driver = init_driver(headless=True)
                    processed_ads_since_restart = 0
                    gc.collect()
                # light throttle to avoid being blocked
                time.sleep(0.6)
            except Exception as e:
                logger.exception(f"Error processing term '{term}': {e}")
                # small pause and continue
                time.sleep(5)
                continue

    finally:
        if driver:
            safe_quit_driver(driver)
        save_state()
        logger.info("Process finished or stopped")

if __name__ == "__main__":
    main()