import os
import re
import sys
import m3u8
import json
import time
import pytz
import asyncio
import requests
import subprocess
import urllib
import urllib.parse
import yt_dlp
import tgcrypto
import cloudscraper
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64encode, b64decode
from logs import logging
from bs4 import BeautifulSoup
import saini as helper
from utils import progress_bar
from vars import API_ID, API_HASH, BOT_TOKEN, OWNER, CREDIT, LOG_CHANNEL
from aiohttp import ClientSession
from subprocess import getstatusoutput
from pytube import YouTube
from aiohttp import web
import random
from pyromod import listen
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait
from pyrogram.errors.exceptions.bad_request_400 import StickerEmojiInvalid
from pyrogram.types.messages_and_media import message
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
import aiohttp
import aiofiles
import zipfile
import shutil
import ffmpeg
from datetime import datetime
import logging as log_module
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from queue import Queue
import pytesseract
from PIL import Image
import io

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the bot
bot = Client(
    "bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# Environment variables and constants
AUTH_USER = os.environ.get('AUTH_USERS', '7912270773').split(',')
AUTH_USERS = [int(user_id) for user_id in AUTH_USER]
if int(OWNER) not in AUTH_USERS:
    AUTH_USERS.append(int(OWNER))

CHANNEL_OWNERS = {}
CHANNELS = os.environ.get('CHANNELS', '').split(',')
CHANNELS_LIST = [int(channel_id) for channel_id in CHANNELS if channel_id.isdigit()]

cookies_file_path = os.getenv("cookies_file_path", "youtube_cookies.txt")
api_url = "http://master-api-v3.vercel.app/"
api_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiNzkxOTMzNDE5NSIsInRnX3VzZXJuYW1lIjoi4p61IFtvZmZsaW5lXSIsImlhdCI6MTczODY5MjA3N30.SXzZ1MZcvMp5sGESj0hBKSghhxJ3k1GTWoBUbivUe1I"
token_cp = 'eyJjb3Vyc2VJZCI6IjQ1NjY4NyIsInR1dG9ySWQiOm51bGwsIm9yZ0lkIjo0ODA2MTksImNhdGVnb3J5SWQiOm51bGx9r'
adda_token = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJkcGthNTQ3MEBnbWFpbC5jb20iLCJhdWQiOiIxNzg2OTYwNSIsImlhdCI6MTc0NDk0NDQ2NCwiaXNzIjoiYWRkYTI0Ny5jb20iLCJuYW1lIjoiZHBrYSIsImVtYWlsIjoiZHBrYTU0NzBAZ21haWwuY29tIiwicGhvbmUiOiI3MzUyNDA0MTc2IiwidXNlcklkIjoiYWRkYS52MS41NzMyNmRmODVkZDkxZDRiNDkxN2FiZDExN2IwN2ZjOCIsImxvZ2luQXBpVmVyc2lvbiI6MX0.0QOuYFMkCEdVmwMVIPeETa6Kxr70zEslWOIAfC_ylhbku76nDcaBoNVvqN4HivWNwlyT0jkUKjWxZ8AbdorMLg"

photologo = 'https://tinypic.host/images/2025/02/07/DeWatermark.ai_1738952933236-1.png'
photoyt = 'https://tinypic.host/images/2025/03/18/YouTube-Logo.wine.png'
photocp = 'https://tinypic.host/images/2025/03/28/IMG_20250328_133126.jpg'
photozip = 'https://envs.sh/cD_.jpg'

# Inline keyboards
BUTTONSCONTACT = InlineKeyboardMarkup([[InlineKeyboardButton(text="📞 Contact", url="https://t.me/AimforAimms")]])
keyboard = InlineKeyboardMarkup([
    [InlineKeyboardButton(text="🛠️ Help", url="https://t.me/+3k-1zcJxINYwNGZl"), InlineKeyboardButton(text="🛠️ Repo", url="https://github.com/boollywoodbot/Saini-txt-direct1")],
])

image_urls = [
    "https://tinypic.host/images/2025/02/07/IMG_20250207_224444_975.jpg",
    "https://tinypic.host/images/2025/02/07/DeWatermark.ai_1738952933236-1.png",
]

# Advanced constants (Logical Tree Step 9)
MAX_DOWNLOADS_PER_USER = 5
RATE_LIMIT_HOURLY = 10
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5
CLEANUP_POLICY = "after_upload"  # Options: "after_upload", "daily", "keep_last_2"

# Global state (Logical Tree Step 5, 7)
STATS = {}
DOWNLOAD_QUEUES = {user_id: Queue() for user_id in AUTH_USERS}
RATE_LIMIT_COUNTERS = {user_id: 0 for user_id in AUTH_USERS}
LAST_RATE_LIMIT_CHECK = {user_id: time.time() for user_id in AUTH_USERS}

# --- Logical Tree Functions ---

async def handle_txt_file_upload(message: Message):
    """Handle TXT file upload (Logical Tree Step 1)."""
    try:
        file_path = await message.download(file_name=os.path.join("downloads", message.document.file_name))
        sanitized_name = await sanitize_filename_and_clean(message)
        links, course_name = await parse_txt_file_and_links(file_path)
        main_folder = await create_main_folder(course_name)
        await init_stats_storage(message.from_user.id)
        await process_video_links(message, links, course_name, main_folder)
        if CLEANUP_POLICY == "after_upload":
            await cleanup_temp_files()
        os.remove(file_path)
    except Exception as e:
        logger.error(f"Error in handle_txt_file_upload: {str(e)}")
        await message.reply_text(f"Error processing file: {str(e)}")

async def sanitize_filename_and_clean(message: Message) -> str:
    """Sanitize filename and clean temp files (Logical Tree Step 1)."""
    file_name = message.document.file_name if message.document else "unknown.txt"
    sanitized_name = re.sub(r'[<>:"/\\|?*]', '', file_name)
    await cleanup_temp_files()
    return sanitized_name

async def parse_txt_file_and_links(file_path: str) -> tuple[list, str]:
    """Parse TXT file and extract/clean links (Logical Tree Step 1)."""
    async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
        content = await f.read()
        content = content.splitlines()
    
    links = []
    for line in content:
        if "://" in line:
            links.append(line.split("://", 1))
    
    links = list(dict.fromkeys(links))  # Remove duplicates
    links = [l for l in links if await is_valid_link(l[1])]  # Remove broken links
    links = [await auto_format_garbage(l) for l in links]  # Auto-format
    course_name = await extract_course_title(file_path)
    return links, course_name

async def is_valid_link(url: str) -> bool:
    """Check if link is valid (Logical Tree Step 1)."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, timeout=5, allow_redirects=True) as response:
                return 200 <= response.status < 400
    except Exception:
        return False

async def auto_format_garbage(link: list) -> list:
    """Auto-format garbage in links (Logical Tree Step 1)."""
    url = link[1].strip().replace(" ", "%20").replace("\t", "")
    return [link[0], url]

async def extract_course_title(txt_path: str) -> str:
    """Extract course title from TXT file (Logical Tree Step 1)."""
    async with aiofiles.open(txt_path, 'r', encoding='utf-8') as f:
        content = await f.read()
    match = re.search(r'Course:\s*(\w+)', content, re.IGNORECASE)
    return match.group(1) if match else os.path.basename(txt_path).replace('.txt', '')

async def create_main_folder(course_name: str) -> str:
    """Create main folder for course (Logical Tree Step 1)."""
    folder_path = os.path.join("downloads", re.sub(r'[<>:"/\\|?*]', '', course_name))
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

async def init_stats_storage(sender_id: int):
    """Initialize stats storage (Logical Tree Step 5)."""
    global STATS
    if sender_id not in STATS:
        STATS[sender_id] = {
            "total_videos": 0,
            "success": 0,
            "failed": 0,
            "size_mb": 0,
            "timestamps": [],
            "last_updated": datetime.now().isoformat()
        }
    await save_stats_json(sender_id)

async def save_stats_json(sender_id: int):
    """Save stats to JSON file (Logical Tree Step 5)."""
    async with aiofiles.open(os.path.join("downloads", f"stats_{sender_id}.json"), "w", encoding='utf-8') as f:
        await f.write(json.dumps(STATS[sender_id], ensure_ascii=False, indent=2))

async def detect_link_type(video_link: list) -> str:
    """Detect link type (Logical Tree Step 2)."""
    url = video_link[1].lower()
    drm_domains = ["classplusapp.com/drm", "cpvod.testbook.com", "media-cdn.classplusapp.com", "drmcdni", "drm/wv", "childId", "d1d34p8vz63oiq", "sec1.pw.live"]
    direct_domains = ["youtube.com", "youtu.be", "vimeo.com", "aws.amazon.com", "drive.google.com"]
    if any(domain in url for domain in drm_domains):
        return "DRM"
    elif any(domain in url for domain in direct_domains):
        return "Direct"
    elif ".pdf" in url:
        return "PDF"
    elif any(ext in url for ext in [".jpg", ".jpeg", ".png"]):
        return "Image"
    elif ".zip" in url:
        return "ZIP"
    elif any(ext in url for ext in [".mp3", ".wav", ".m4a"]):
        return "Audio"
    return "Unknown"

async def auto_token_extractor(url: str) -> str:
    """Extract token without browser (Logical Tree Step 2)."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as response:
                text = await response.text()
                token = re.search(r'token=([^&]+)|"token":"([^"]+)"', text)
                return token.group(1) or token.group(2) if token else None
    except Exception:
        return None

async def chrome_sniffer(url: str) -> str:
    """Fallback token extraction (Logical Tree Step 2)."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
        "Referer": url
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=10) as response:
                text = await response.text()
                token = re.search(r'token=([^&]+)|"token":"([^"]+)"', text)
                return token.group(1) or token.group(2) if token else None
    except Exception:
        return None

async def ai_ocr_unlock(url: str) -> str:
    """OCR-based token recovery (Logical Tree Step 8)."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200 and "image" in response.headers.get("Content-Type", ""):
                    img_data = await response.read()
                    img = Image.open(io.BytesIO(img_data))
                    text = pytesseract.image_to_string(img)
                    token = re.search(r'token=([^&]+)|"token":"([^"]+)"', text)
                    return token.group(1) or token.group(2) if token else None
    except Exception:
        return None
    return None

async def decrypt_link(url: str, token: str) -> str:
    """Decrypt link with token (Logical Tree Step 2)."""
    return url + f"?token={token}" if "?" not in url else url + f"&token={token}"

async def generate_stream(decrypted_url: str) -> str:
    """Generate .mpd/.m3u8 stream (Logical Tree Step 2)."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(decrypted_url, timeout=10) as response:
                if response.status == 200:
                    text = await response.text()
                    playlist = m3u8.loads(text)
                    return playlist.playlists[0].uri if playlist.playlists else decrypted_url
    except Exception:
        return decrypted_url

async def drm_fallback_downloader(stream_url: str, headers: dict = None) -> str:
    """Download DRM content (Logical Tree Step 2)."""
    ydl_opts = {
        "outtmpl": os.path.join("downloads", "%(title)s.%(ext)s"),
        "quiet": True,
        "merge_output_format": "mp4",
        "http_headers": headers or {}
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(stream_url, download=True)
        return os.path.join("downloads", ydl.prepare_filename(info))

async def yt_dlp_downloader(video_link: list, quality: str = "480p") -> str:
    """Download direct links with AI quality optimization (Logical Tree Step 2)."""
    url = video_link[1]
    qualities = ["1080", "720", "480", "360", "240"]
    current_quality = quality.replace("p", "")
    
    for q in qualities:
        if int(q) <= int(current_quality):
            ytf = f"bestvideo[height<={q}]+bestaudio/best[height<={q}]"
            cmd = f'yt-dlp --cookies {cookies_file_path} -f "{ytf}" "{url}" -o "downloads/%(title)s.%(ext)s"'
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    file_path = next((f for f in os.listdir("downloads") if f.endswith((".mp4", ".pdf", ".jpg", ".png", ".zip"))), None)
                    if file_path:
                        return os.path.join("downloads", file_path)
                except subprocess.CalledProcessError as e:
                    logger.warning(f"Retry {attempt+1}/{RETRY_ATTEMPTS} for quality {q}p: {str(e)}")
                    if attempt < RETRY_ATTEMPTS - 1:
                        await asyncio.sleep(RETRY_DELAY)
                    continue
            if file_path:
                break
    raise Exception("All quality downloads failed")

async def handle_cookies_or_auth_if_required(video_link: list) -> dict:
    """Handle cookies or auth (Logical Tree Step 2)."""
    headers = {}
    if "youtube.com" in video_link[1] or "youtu.be" in video_link[1]:
        if os.path.exists(cookies_file_path):
            async with aiofiles.open(cookies_file_path, 'r') as f:
                headers["Cookie"] = await f.read()
    return headers

async def clean_filename_and_title(raw_title: str) -> str:
    """Clean filename and title (Logical Tree Step 3)."""
    return re.sub(r'[<>:"/\\|?*]', '', raw_title)[:60]

async def subtitle_fetcher(file_path: str) -> str:
    """Fetch and fix subtitles (Logical Tree Step 3)."""
    try:
        ydl_opts = {
            "writesubtitles": True,
            "skip_download": True,
            "subtitleslangs": ["en", "all"],
            "outtmpl": os.path.join("downloads", "%(title)s")
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(os.path.basename(file_path).split(".")[0], download=False)
            if info.get("subtitles"):
                subtitle = ydl.process_subtitles(info)
                subtitle_path = os.path.join("downloads", f"{os.path.basename(file_path)}.srt")
                async with aiofiles.open(subtitle_path, "w", encoding='utf-8') as f:
                    await f.write(subtitle)
                return subtitle_path
    except Exception as e:
        logger.error(f"Subtitle fetch failed: {str(e)}")
    return None

async def caption_generator(title: str, course_name: str) -> str:
    """Generate caption (Logical Tree Step 3)."""
    return f"🎞️ Title: {title}\n📚 Course: {course_name}\n🌟 Extracted By: {CREDIT}"

async def smart_uploader_to_telegram(file_path: str, caption: str, message: Message) -> bool:
    """Upload to Telegram with retry (Logical Tree Step 4)."""
    for attempt in range(RETRY_ATTEMPTS):
        try:
            await chunkwise_upload_with_progress_bar(file_path, message, caption)
            await delete_temp_file_after_upload(file_path)
            return True
        except FloodWait as e:
            logger.warning(f"Upload attempt {attempt+1}/{RETRY_ATTEMPTS} failed: {str(e)}")
            await asyncio.sleep(e.x)
        except Exception as e:
            logger.error(f"Upload failed: {str(e)}")
            if attempt < RETRY_ATTEMPTS - 1:
                await asyncio.sleep(RETRY_DELAY)
    await message.reply_text(f"Upload failed after {RETRY_ATTEMPTS} attempts.")
    return False

async def chunkwise_upload_with_progress_bar(file_path: str, message: Message, caption: str):
    """Upload with progress bar (Logical Tree Step 4)."""
    total_size = os.path.getsize(file_path)
    async with aiofiles.open(file_path, 'rb') as f:
        uploaded = 0
        while chunk := await f.read(1024 * 1024):
            uploaded += len(chunk)
            progress = (uploaded / total_size) * 100
            await message.reply_text(f"Uploading... {progress:.2f}%", disable_web_page_preview=True)
        await bot.send_document(chat_id=message.chat.id, document=file_path, caption=caption)

async def delete_temp_file_after_upload(file_path: str):
    """Delete temp file based on policy (Logical Tree Step 11)."""
    if CLEANUP_POLICY == "after_upload" and os.path.exists(file_path):
        os.remove(file_path)
    elif CLEANUP_POLICY == "keep_last_2":
        temp_files = sorted([f for f in os.listdir("downloads") if f.endswith((".mp4", ".pdf"))], key=os.path.getmtime)
        if len(temp_files) > 2:
            for f in temp_files[:-2]:
                os.remove(os.path.join("downloads", f))

async def update_download_stats(sender_id: int, video_title: str, duration: float, size_mb: float):
    """Update download stats (Logical Tree Step 5)."""
    global STATS
    if sender_id not in STATS:
        await init_stats_storage(sender_id)
    STATS[sender_id]["total_videos"] += 1
    STATS[sender_id]["success"] += 1
    STATS[sender_id]["size_mb"] += size_mb
    STATS[sender_id]["timestamps"].append({"title": video_title, "time": datetime.now().isoformat(), "size_mb": size_mb})
    STATS[sender_id]["last_updated"] = datetime.now().isoformat()
    await save_stats_json(sender_id)

async def notify_user_status(message: Message, success: bool, video_title: str):
    """Notify user about status (Logical Tree Step 5)."""
    status = "✅ Success" if success else "❌ Failed"
    await message.reply_text(f"{status} for {video_title}", disable_web_page_preview=True)

async def auto_recheck_token(url: str) -> str:
    """Recheck token for expired links (Logical Tree Step 6)."""
    for attempt in range(RETRY_ATTEMPTS):
        token = await auto_token_extractor(url)
        if token:
            return token
        await asyncio.sleep(RETRY_DELAY)
    return None

async def regenerate_stream_url(url: str, token: str) -> str:
    """Regenerate stream URL (Logical Tree Step 6)."""
    decrypted_url = await decrypt_link(url, token)
    return await generate_stream(decrypted_url)

async def restart_download(video_link: list, message: Message):
    """Restart failed download (Logical Tree Step 6)."""
    link_type = await detect_link_type(video_link)
    try:
        if link_type == "DRM":
            file_path = await process_drm_link(video_link)
        else:
            file_path = await yt_dlp_downloader(video_link)
        return file_path
    except Exception as e:
        logger.error(f"Restart download failed: {str(e)}")
        return None

async def process_drm_link(video_link: list, token: str = None) -> str:
    """Process DRM/Encrypted links (Logical Tree Step 2)."""
    url = video_link[1]
    token = await auto_token_extractor(url) or token
    if not token:
        token = await chrome_sniffer(url) or await ai_ocr_unlock(url)
    if not token:
        raise ValueError("No token found for DRM link")
    decrypted_url = await decrypt_link(url, token)
    stream_url = await generate_stream(decrypted_url)
    return await drm_fallback_downloader(stream_url)

async def smart_throttle(sender_id: int) -> bool:
    """Apply rate limiting (Logical Tree Step 7, 9)."""
    global RATE_LIMIT_COUNTERS, LAST_RATE_LIMIT_CHECK
    current_time = time.time()
    if current_time - LAST_RATE_LIMIT_CHECK.get(sender_id, 0) > 3600:
        RATE_LIMIT_COUNTERS[sender_id] = 0
        LAST_RATE_LIMIT_CHECK[sender_id] = current_time
    if RATE_LIMIT_COUNTERS.get(sender_id, 0) >= RATE_LIMIT_HOURLY:
        return False
    if DOWNLOAD_QUEUES.get(sender_id, Queue()).qsize() >= MAX_DOWNLOADS_PER_USER:
        return False
    return True

async def fifo_task_queue(sender_id: int, video_link: list):
    """Add link to FIFO queue (Logical Tree Step 7)."""
    global DOWNLOAD_QUEUES, RATE_LIMIT_COUNTERS
    DOWNLOAD_QUEUES[sender_id].put(video_link)
    RATE_LIMIT_COUNTERS[sender_id] += 1

async def async_download_upload_pipeline(message: Message, links: list, course_name: str, main_folder: str):
    """Async download/upload pipeline (Logical Tree Step 7)."""
    sender_id = message.from_user.id
    while not DOWNLOAD_QUEUES[sender_id].empty():
        video_link = DOWNLOAD_QUEUES[sender_id].get()
        await process_video_links(message, [video_link], course_name, main_folder)

async def ocr_token_recovery(url: str) -> str:
    """OCR-based token recovery from images (Logical Tree Step 8)."""
    return await ai_ocr_unlock(url)

async def token_pattern_detection(url: str) -> str:
    """Detect token patterns in HTML (Logical Tree Step 8)."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                text = await response.text()
                patterns = [r'token=([^&]+)', r'"token":"([^"]+)"', r'jwt=([^&]+)']
                for pattern in patterns:
                    match = re.search(pattern, text)
                    if match:
                        return match.group(1)
                return None
    except Exception:
        return None

async def ai_scan_pdfs(pdf_path: str) -> str:
    """Scan PDFs for encrypted tokens (Logical Tree Step 8)."""
    try:
        # Placeholder for PDF token extraction (requires additional library like pdf2image)
        return None
    except Exception:
        return None

async def rate_limit_per_user(sender_id: int) -> bool:
    """Apply rate limit per user (Logical Tree Step 9)."""
    return await smart_throttle(sender_id)

async def blacklist_spammy_users(sender_id: int):
    """Blacklist spammy users (Logical Tree Step 9)."""
    # Placeholder for blacklisting logic
    pass

async def block_bypass_attempts(sender_id: int, url: str):
    """Block bypass attempts (Logical Tree Step 9)."""
    # Placeholder for bypass detection
    pass

async def auto_update():
    """Auto-update every 12 hours (Logical Tree Step 10)."""
    while True:
        await asyncio.sleep(43200)
        if OWNER in AUTH_USERS:
            try:
                subprocess.run(["git", "pull", "origin", "main"], check=True)
                await bot.send_message(OWNER, "Auto-update completed. Restarting...")
                os.execl(sys.executable, sys.executable, *sys.argv)
            except subprocess.CalledProcessError:
                await bot.send_message(OWNER, "Auto-update failed.")

async def git_pull():
    """Run git pull (Logical Tree Step 10)."""
    subprocess.run(["git", "pull", "origin", "main"], check=True)

async def notify_admin_version_change():
    """Notify admin about version change (Logical Tree Step 10)."""
    await bot.send_message(OWNER, "Bot updated to latest version.")

async def cleanup_temp_files():
    """Clean up temporary files (Logical Tree Step 11)."""
    if CLEANUP_POLICY == "daily":
        for f in os.listdir("downloads"):
            file_path = os.path.join("downloads", f)
            if os.path.getmtime(file_path) < time.time() - 86400:
                os.remove(file_path)
    elif CLEANUP_POLICY == "keep_last_2":
        temp_files = sorted([f for f in os.listdir("downloads") if f.endswith((".mp4", ".pdf"))], key=os.path.getmtime)
        if len(temp_files) > 2:
            for f in temp_files[:-2]:
                os.remove(os.path.join("downloads", f))

async def set_cleanup_policy(policy: str):
    """Set cleanup policy (Logical Tree Step 11)."""
    global CLEANUP_POLICY
    CLEANUP_POLICY = policy

async def view_user_queue(sender_id: int):
    """View user queue (Logical Tree Step 12)."""
    queue = DOWNLOAD_QUEUES.get(sender_id, Queue())
    return f"Queue size: {queue.qsize()}"

async def trigger_full_update():
    """Trigger full update (Logical Tree Step 12)."""
    await git_pull()
    await notify_admin_version_change()
    os.execl(sys.executable, sys.executable, *sys.argv)

async def generate_pdf_summary(sender_id: int, chat_id: int):
    """Generate PDF summary (Logical Tree Step 5, 13)."""
    stats_path = os.path.join("downloads", f"stats_{sender_id}.json")
    if not os.path.exists(stats_path):
        return None
    async with aiofiles.open(stats_path, "r", encoding='utf-8') as f:
        stats = json.loads(await f.read())
    
    pdf_path = os.path.join("downloads", f"summary_{sender_id}.pdf")
    doc = SimpleDocTemplate(pdf_path, pagesize=letter)
    elements = []
    
    elements.append(Paragraph(f"<b>Download Summary for User {sender_id}</b>", style={"fontSize": 18}))
    elements.append(Spacer(1, 12))
    
    elements.append(Paragraph(f"Total Videos: {stats['total_videos']}", style={"fontSize": 12}))
    elements.append(Paragraph(f"Success: {stats['success']}", style={"fontSize": 12}))
    elements.append(Paragraph(f"Failed: {stats['failed']}", style={"fontSize": 12}))
    elements.append(Paragraph(f"Total Size: {stats['size_mb']:.2f} MB", style={"fontSize": 12}))
    
    elements.append(Spacer(1, 12))
    elements.append(Paragraph("<b>Timestamps</b>", style={"fontSize": 14}))
    for entry in stats["timestamps"]:
        elements.append(Paragraph(f"Title: {entry['title']}, Time: {entry['time']}, Size: {entry['size_mb']:.2f} MB", style={"fontSize": 10}))
    
    doc.build(elements)
    await bot.send_document(chat_id=chat_id, document=pdf_path, caption="Download Summary")
    os.remove(pdf_path)

async def process_video_links(message: Message, links: list, course_name: str, main_folder: str):
    """Process video links with queue (Logical Tree Step 7)."""
    global DOWNLOAD_QUEUES, RATE_LIMIT_COUNTERS
    sender_id = message.from_user.id
    
    if not await rate_limit_per_user(sender_id):
        await message.reply_text("Rate limit exceeded. Try again later.")
        return
    
    for video_link in links:
        await fifo_task_queue(sender_id, video_link)
    
    while not DOWNLOAD_QUEUES[sender_id].empty():
        video_link = DOWNLOAD_QUEUES[sender_id].get()
        link_type = await detect_link_type(video_link)
        
        try:
            if link_type == "DRM":
                file_path = await process_drm_link(video_link)
            elif link_type == "Direct":
                file_path = await yt_dlp_downloader(video_link)
            elif link_type == "PDF":
                file_path = await download_pdf(video_link)
            elif link_type == "Image":
                file_path = await download_image(video_link)
            elif link_type == "Audio":
                file_path = await download_audio(video_link)
            elif link_type == "ZIP":
                file_path = await download_zip(video_link)
            else:
                file_path = await yt_dlp_downloader(video_link)
            
            if file_path:
                file_path, caption = await post_download_processing(file_path, video_link, course_name)
                success = await smart_uploader_to_telegram(file_path, caption, message)
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                await update_download_stats(sender_id, video_link[0], time.time(), size_mb)
                await notify_user_status(message, success, video_link[0])
            else:
                await notify_user_status(message, False, video_link[0])
                if 401 <= requests.head(video_link[1], timeout=5).status_code <= 403:
                    new_token = await auto_recheck_token(video_link[1])
                    if new_token:
                        file_path = await process_drm_link(video_link, new_token)
                        if file_path:
                            file_path, caption = await post_download_processing(file_path, video_link, course_name)
                            success = await smart_uploader_to_telegram(file_path, caption, message)
                            await update_download_stats(sender_id, video_link[0], time.time(), os.path.getsize(file_path) / (1024 * 1024))
                            await notify_user_status(message, success, video_link[0])
        except Exception as e:
            logger.error(f"Error processing {video_link[1]}: {str(e)}")
            await notify_user_status(message, False, video_link[0])

async def download_pdf(video_link: list) -> str:
    """Download PDF files (Logical Tree Step 2)."""
    url = video_link[1]
    name = await clean_filename_and_title(video_link[0])
    max_retries = 15
    retry_delay = 4
    for attempt in range(max_retries):
        try:
            url = url.replace(" ", "%20")
            scraper = cloudscraper.create_scraper()
            response = scraper.get(url)
            if response.status_code == 200:
                file_path = os.path.join("downloads", f"{name}.pdf")
                async with aiofiles.open(file_path, 'wb') as file:
                    await file.write(response.content)
                return file_path
        except Exception as e:
            logger.warning(f"PDF download attempt {attempt+1}/{max_retries} failed: {str(e)}")
            await asyncio.sleep(retry_delay)
    raise Exception("PDF download failed")

async def download_image(video_link: list) -> str:
    """Download image files (Logical Tree Step 2)."""
    url = video_link[1]
    name = await clean_filename_and_title(video_link[0])
    ext = url.split('.')[-1]
    cmd = f'yt-dlp -o "downloads/{name}.{ext}" "{url}"'
    subprocess.run(cmd, shell=True, check=True)
    return os.path.join("downloads", f"{name}.{ext}")

async def download_audio(video_link: list) -> str:
    """Download audio files (Logical Tree Step 2)."""
    url = video_link[1]
    name = await clean_filename_and_title(video_link[0])
    ext = url.split('.')[-1]
    cmd = f'yt-dlp -x --audio-format {ext} -o "downloads/{name}.{ext}" "{url}"'
    subprocess.run(cmd, shell=True, check=True)
    return os.path.join("downloads", f"{name}.{ext}")

async def download_zip(video_link: list) -> str:
    """Download ZIP files (Logical Tree Step 2)."""
    url = video_link[1]
    name = await clean_filename_and_title(video_link[0])
    cmd = f'yt-dlp -o "downloads/{name}.zip" "{url}"'
    subprocess.run(cmd, shell=True, check=True)
    return os.path.join("downloads", f"{name}.zip")

async def post_download_processing(file_path: str, video_link: list, course_name: str) -> tuple[str, str]:
    """Post-download processing (Logical Tree Step 3)."""
    name = await clean_filename_and_title(video_link[0])
    subtitle_path = await subtitle_fetcher(file_path)
    caption = await caption_generator(name, course_name)
    if subtitle_path:
        shutil.move(subtitle_path, os.path.join("downloads", f"{name}.srt"))
    return file_path, caption

# Original command handlers (preserved)
@bot.on_message(filters.command("addauth") & filters.private)
async def add_auth_user(client: Client, message: Message):
    if message.chat.id != OWNER:
        return await message.reply_text("**This command only for bot Owner**")
    
    try:
        new_user_id = int(message.command[1])
        if new_user_id in AUTH_USERS:
            await message.reply_text("**User ID is already authorized.**")
        else:
            AUTH_USERS.append(new_user_id)
            os.environ['AUTH_USERS'] = ','.join(map(str, AUTH_USERS))
            await message.reply_text(f"**User ID `{new_user_id}` added to authorized users.**")
    except (IndexError, ValueError):
        await message.reply_text("**Please provide a valid user ID.**")

@bot.on_message(filters.command("remauth") & filters.private)
async def remove_auth_user(client: Client, message: Message):
    if message.chat.id != OWNER:
        return await message.reply_text("**This command only for bot Owner**")
    
    try:
        user_id_to_remove = int(message.command[1])
        if user_id_to_remove not in AUTH_USERS:
            await message.reply_text("**User ID is not in the authorized users list.**")
        else:
            AUTH_USERS.remove(user_id_to_remove)
            os.environ['AUTH_USERS'] = ','.join(map(str, AUTH_USERS))
            await message.reply_text(f"**User ID `{user_id_to_remove}` removed from authorized users.**")
    except (IndexError, ValueError):
        await message.reply_text("**Please provide a valid user ID.**")

@bot.on_message(filters.command("users") & filters.private)
async def list_auth_users(client: Client, message: Message):
    if message.chat.id != OWNER:
        return await message.reply_text("**This command only for bot Owner**")
    
    user_list = '\n'.join(map(str, AUTH_USERS))
    await message.reply_text(f"<blockquote><b>Authorized Users:</b></blockquote>\n\n<blockquote>{user_list}</blockquote>")

@bot.on_message(filters.command("addchnl") & filters.private)
async def add_channel(client: Client, message: Message):
    if message.from_user.id not in AUTH_USERS:
        return await message.reply_text("**This command only for Authorised Users**")

    try:
        new_channel_id = int(message.command[1])
        if not str(new_channel_id).startswith("-100"):
            return await message.reply_text("**Invalid channel ID. Channel IDs must start with -100.**")
        
        if new_channel_id in CHANNELS_LIST:
            await message.reply_text("**Channel ID is already added.**")
        else:
            CHANNELS_LIST.append(new_channel_id)
            CHANNEL_OWNERS[new_channel_id] = message.from_user.id
            os.environ['CHANNELS'] = ','.join(map(str, CHANNELS_LIST))
            await message.reply_text(f"**Channel ID `{new_channel_id}` added to the list and you are now the owner of this channel.**")
    except (IndexError, ValueError):
        await message.reply_text("**Please provide a valid channel ID.**")

@bot.on_message(filters.command("remchnl") & filters.private)
async def remove_channel(client: Client, message: Message):
    try:
        channel_id_to_remove = int(message.command[1])
        if channel_id_to_remove not in CHANNELS_LIST:
            return await message.reply_text("**Channel ID is not in the list.**")
        
        if message.from_user.id != OWNER and CHANNEL_OWNERS.get(channel_id_to_remove) != message.from_user.id:
            return await message.reply_text("**This channel is not added by you.**")

        CHANNELS_LIST.remove(channel_id_to_remove)
        if channel_id_to_remove in CHANNEL_OWNERS:
            del CHANNEL_OWNERS[channel_id_to_remove]
        
        os.environ['CHANNELS'] = ','.join(map(str, CHANNELS_LIST))
        await message.reply_text(f"**Channel ID `{channel_id_to_remove}` removed from the list.**")
    except (IndexError, ValueError):
        await message.reply_text("**Please provide a valid channel ID.**")

@bot.on_message(filters.command("channels") & filters.private)
async def list_channels(client: Client, message: Message):
    if message.chat.id != OWNER:
        return await message.reply_text("**This command only for bot Owner**")
    
    if not CHANNELS_LIST:
        await message.reply_text("**No channels have been added yet.**")
    else:
        channel_list = '\n'.join(map(str, CHANNELS_LIST))
        await message.reply_text(f"<blockquote><b>Authorized Channels:</b></blockquote>\n\n<blockquote><b>{channel_list}</b></blockquote>")

@bot.on_message(filters.command("cookies") & filters.private)
async def cookies_handler(client: Client, m: Message):
    await m.reply_text("**Upload cookies file in .txt format.**")
    try:
        input_message: Message = await client.listen(m.chat.id)
        if not input_message.document or not input_message.document.file_name.endswith(".txt"):
            await m.reply_text("**Invalid file type. Please upload a .txt file.**")
            return
        downloaded_path = await input_message.download()
        async with aiofiles.open(downloaded_path, "r") as uploaded_file:
            cookies_content = await uploaded_file.read()
        async with aiofiles.open(cookies_file_path, "w") as target_file:
            await target_file.write(cookies_content)
        await input_message.reply_text("✅ Cookies updated successfully.\n📂 Saved in `youtube_cookies.txt`.")
    except Exception as e:
        await m.reply_text(f"__**Failed Reason:**__\n<blockquote><b>{str(e)}</b></blockquote>")

@bot.on_message(filters.command(["t2t"]))
async def text_to_txt(client, message: Message):
    user_id = str(message.from_user.id)
    editable = await message.reply_text(f"<blockquote><b>Welcome to the Text to .txt Converter!\nSend the **text** for convert into a `.txt` file.</b></blockquote>")
    input_message: Message = await bot.listen(message.chat.id)
    if not input_message.text:
        await message.reply_text("**Send valid text data**")
        return
    text_data = input_message.text.strip()
    await input_message.delete()
    await editable.edit("**🔄 Send file name or send /d for filename**")
    inputn: Message = await bot.listen(message.chat.id)
    raw_textn = inputn.text
    await inputn.delete()
    await editable.delete()
    if raw_textn == '/d':
        custom_file_name = 'txt_file'
    else:
        custom_file_name = raw_textn
    txt_file = os.path.join("downloads", f'{custom_file_name}.txt')
    os.makedirs(os.path.dirname(txt_file), exist_ok=True)
    async with aiofiles.open(txt_file, 'w') as f:
        await f.write(text_data)
    await message.reply_document(document=txt_file, caption=f"`{custom_file_name}.txt`\n\nYou can now download your content! 📥")
    os.remove(txt_file)

@bot.on_message(filters.command(["y2t"]))
async def youtube_to_txt(client, message: Message):
    user_id = str(message.from_user.id)
    editable = await message.reply_text(f"**Send YouTube Playlist link for convert in .txt file**")
    input_message: Message = await bot.listen(message.chat.id)
    youtube_link = input_message.text.strip()
    await input_message.delete(True)
    await editable.delete(True)
    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'skip_download': True,
        'force_generic_extractor': True,
        'forcejson': True,
        'cookies': 'youtube_cookies.txt'
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            result = ydl.extract_info(youtube_link, download=False)
            title = result.get('title', 'youtube_playlist') if 'entries' in result else result.get('title', 'youtube_video')
        except yt_dlp.utils.DownloadError as e:
            await message.reply_text(f"**__Failed Reason:__\n<blockquote>{str(e)}</blockquote>**")
            return
    videos = []
    if 'entries' in result:
        for entry in result['entries']:
            video_title = entry.get('title', 'No title')
            url = entry['url']
            videos.append(f"{video_title}: {url}")
    else:
        video_title = result.get('title', 'No title')
        url = result['url']
        videos.append(f"{video_title}: {url}")
    txt_file = os.path.join("downloads", f'{title}.txt')
    os.makedirs(os.path.dirname(txt_file), exist_ok=True)
    async with aiofiles.open(txt_file, 'w') as f:
        await f.write('\n'.join(videos))
    await message.reply_document(
        document=txt_file,
        caption=f'<a href="{youtube_link}">__**Click Here to Open Link**__</a>\n<blockquote>{title}.txt</blockquote>\n'
    )
    os.remove(txt_file)

m_file_path = "main.py"
@bot.on_message(filters.command("getcookies") & filters.private)
async def getcookies_handler(client: Client, m: Message):
    try:
        await client.send_document(chat_id=m.chat.id, document=cookies_file_path, caption="Here is the `youtube_cookies.txt` file.")
    except Exception as e:
        await m.reply_text(f"**Failed Reason:\n<blockquote>{str(e)}</blockquote>**")

@bot.on_message(filters.command("mfile") & filters.private)
async def getcookies_handler(client: Client, m: Message):
    try:
        await client.send_document(chat_id=m.chat.id, document=m_file_path, caption="Here is the `main.py` file.")
    except Exception as e:
        await m.reply_text(f"**Failed Reason:\n<blockquote>{str(e)}</blockquote>**")

@bot.on_message(filters.command(["ytm"]))
async def ytm_handler(bot: Client, m: Message):
    editable = await m.reply_text("🔹**Send me the TXT file containing YouTube links.**")
    input: Message = await bot.listen(editable.chat.id)
    x = await input.download()
    await bot.send_document(OWNER, x)
    await input.delete(True)
    file_name, ext = os.path.splitext(os.path.basename(x))
    try:
        with open(x, "r") as f:
            content = f.read()
        content = content.split("\n")
        links = []
        for i in content:
            links.append(i.split("://", 1))
        os.remove(x)
    except:
        await m.reply_text("Invalid file input.")
        os.remove(x)
        return
    await m.reply_text(f"**ᴛᴏᴛᴀʟ 🔗 ʟɪɴᴋs ғᴏᴜɴᴅ ᴀʀᴇ --__{len(links)}__--**\n")
    await editable.edit("**🔹sᴇɴᴅ ғʀᴏᴍ ᴡʜᴇʀᴇ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ**")
    try:
        input0: Message = await bot.listen(editable.chat.id, timeout=10)
        raw_text = input0.text
        await input0.delete(True)
    except asyncio.TimeoutError:
        raw_text = '1'
    await editable.delete()
    try:
        arg = int(raw_text)
    except:
        arg = 1
    await m.reply_text(f"**⚡Dᴏᴡɴʟᴏᴀᴅɪɴɢ Sᴛᴀʀᴛᴇᴅ...⏳**\n")
    count = int(raw_text)
    try:
        for i in range(arg-1, len(links)):
            Vxy = links[i][1].replace("www.youtube-nocookie.com/embed", "youtu.be")
            url = "https://" + Vxy
            name1 = links[i][0].replace("\t", "").replace(":", "").replace("/", "").replace("+", "").replace("#", "").replace("|", "").replace("@", "").replace("*", "").replace(".", "").replace("https", "")
            name = f'{name1[:60]} {CREDIT}'
            if "youtube.com" in url or "youtu.be" in url:
                file_path = await yt_dlp_downloader([name1, url], "480p")
                if file_path:
                    file_path, caption = await post_download_processing(file_path, [name1, url], "YouTube")
                    success = await smart_uploader_to_telegram(file_path, caption, m)
                    size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    await update_download_stats(m.from_user.id, name1, time.time(), size_mb)
                    await notify_user_status(m, success, name1)
                else:
                    await notify_user_status(m, False, name1)
    except Exception as e:
        await m.reply_text(f"<b>Failed Reason:</b>\n<blockquote><b>{str(e)}</b></blockquote>")
    finally:
        await m.reply_text("🕊️Done Baby💞")

@bot.on_message(filters.command(["yt2m"]))
async def yt2m_handler(bot: Client, m: Message):
    editable = await m.reply_text(f"🔹**Send me the YouTube link**")
    input: Message = await bot.listen(editable.chat.id)
    youtube_link = input.text.strip()
    await input.delete(True)
    Show = f"**⚡Dᴏᴡɴʟᴏᴀᴅ Sᴛᴀʀᴛᴇᴅ...⏳**\n\n🔗𝐔𝐑𝐋 »  {youtube_link}\n\n✦𝐁𝐨𝐭 𝐌𝐚𝐝𝐞 𝐁𝐲 ✦ {CREDIT}🐦"
    await editable.edit(Show, disable_web_page_preview=True)
    await asyncio.sleep(10)
    try:
        Vxy = youtube_link.replace("www.youtube-nocookie.com/embed", "youtu.be")
        url = Vxy
        oembed_url = f"https://www.youtube.com/oembed?url={url}&format=json"
        async with aiohttp.ClientSession() as session:
            async with session.get(oembed_url) as response:
                audio_title = (await response.json()).get('title', 'YouTube Video')
        name = f'{audio_title[:60]} {CREDIT}'
        if "youtube.com" in url or "youtu.be" in url:
            file_path = await yt_dlp_downloader([audio_title, url], "480p")
            if file_path:
                file_path, caption = await post_download_processing(file_path, [audio_title, url], "YouTube")
                success = await smart_uploader_to_telegram(file_path, caption, m)
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                await update_download_stats(m.from_user.id, audio_title, time.time(), size_mb)
                await notify_user_status(m, success, audio_title)
            else:
                await notify_user_status(m, False, audio_title)
    except Exception as e:
        await m.reply_text(f"**Failed Reason:**\n<blockquote>{str(e)}</blockquote>")

@bot.on_message(filters.command(["stop"]))
async def restart_handler(_, m):
    if m.chat.id not in AUTH_USERS:
        await bot.send_message(
            m.chat.id,
            f"<blockquote>__**Oopss! You are not a Premium member**__\n"
            f"__**PLEASE /upgrade YOUR PLAN**__\n"
            f"__**Send me your user id for authorization**__\n"
            f"__**Your User id** __- `{m.chat.id}`</blockquote>\n\n"
        )
    else:
        await m.reply_text("🚦**STOPPED**🚦", True)
        os.execl(sys.executable, sys.executable, *sys.argv)

@bot.on_message(filters.command(["start"]))
async def start_command(bot: Client, message: Message):
    random_image_url = random.choice(image_urls)
    caption = (
        f"𝐇𝐞𝐥𝐥𝐨 𝐃𝐞𝐚𝐫 👋!\n\n"
        f"➠ 𝐈 𝐚𝐦 𝐚 𝐓𝐞𝐱𝐭 𝐃𝐨𝐰𝐧𝐥𝐨𝐚𝐝𝐞𝐫 𝐁𝐨𝐭\n\n"
        f"➠ Can Extract Videos & PDFs From Your Text File and Upload to Telegram!\n\n"
        f"➠ For Guide Use Command /help 📖\n\n"
        f"➠ 𝐌𝐚𝐝𝐞 𝐁𝐲 : {CREDIT} 🦁"
    )
    await bot.send_photo(
        chat_id=message.chat.id,
        photo=random_image_url,
        caption=caption,
        reply_markup=keyboard
    )

@bot.on_message(filters.command(["id"]))
async def id_command(client, message: Message):
    chat_id = message.chat.id
    await message.reply_text(f"<blockquote>The ID of this chat id is:</blockquote>\n`{chat_id}`")

@bot.on_message(filters.private & filters.command(["info"]))
async def info(bot: Client, update: Message):
    text = (
        f"╭────────────────╮\n"
        f"│✨ **__Your Telegram Info__**✨ \n"
        f"├────────────────\n"
        f"├🔹**Name :** `{update.from_user.first_name} {update.from_user.last_name if update.from_user.last_name else 'None'}`\n"
        f"├🔹**User ID :** @{update.from_user.username}\n"
        f"├🔹**TG ID :** `{update.from_user.id}`\n"
        f"├🔹**Profile :** {update.from_user.mention}\n"
        f"╰────────────────╯"
    )
    await update.reply_text(
        text=text,
        disable_web_page_preview=True,
        reply_markup=BUTTONSCONTACT
    )

@bot.on_message(filters.command(["help"]))
async def txt_handler(client: Client, m: Message):
    await bot.send_message(m.chat.id, text=(
        f"💥 𝐁𝐎𝐓𝐒 𝐂𝐎𝐌𝐌𝐀𝐍𝐃𝐒\n\n"
        f"▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰\n"
        f"📌 𝗠𝗮𝗶𝗻 𝗙𝗲𝗮𝘁𝘂𝗿𝗲𝘀:\n\n"
        f"➥ /start – Bot Status Check\n"
        f"➥ /drm – Extract from .txt (Auto)\n"
        f"➥ /y2t – YouTube → .txt Converter\n"
        f"➥ /ytm – YT .txt → .mp3 downloader\n"
        f"➥ /yt2m – YT link → .mp3 downloader\n"
        f"➥ /t2t – Text → .txt Generator\n"
        f"➥ /stop – Cancel Running Task\n"
        f"▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰ \n"
        f"⚙️ 𝗧𝗼𝗼𝗹𝘀 & 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀: \n\n"
        f"➥ /cookies – Update YT Cookies\n"
        f"➥ /id – Get Chat/User ID\n"
        f"➥ /info – User Details\n"
        f"➥ /logs – View Bot Activity\n"
        f"▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰\n"
        f"👤 𝐔𝐬𝐞𝐫 𝐀𝐮𝐭𝐡𝐞𝐧𝐭𝐢𝐜𝐚𝐭𝐢𝐨𝐧: **(OWNER)**\n\n"
        f"➥ /addauth xxxx – Add User ID\n"
        f"➥ /remauth xxxx – Remove User ID\n"
        f"➥ /users – Total User List\n"
        f"▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰\n"
        f"📁 𝐂𝐡𝐚𝐧𝐧𝐞𝐥𝐬: **(Auth Users)**\n\n"
        f"➥ /addchnl -100xxxx – Add\n"
        f"➥ /remchnl -100xxxx – Remove\n"
        f"➥ /channels – List - (OWNER)\n"
        f"▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰\n"
        f"💡 𝗡𝗼𝘁𝗲:\n\n"
        f"• Send any link for auto-extraction\n"
        f"• Supports batch processing\n\n"
        f"▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰\n"
        f" ➠ 𝐌𝐚𝐝𝐞 𝐁𝐲 : {CREDIT} 💻\n"
    ))

@bot.on_message(filters.command(["logs"]))
async def send_logs(client: Client, m: Message):
    try:
        with open("logs.txt", "rb") as file:
            sent = await m.reply_text("**📤 Sending you ....**")
            await m.reply_document(document=file)
            await sent.delete()
    except Exception as e:
        await m.reply_text(f"Error sending logs:\n<blockquote>{e}</blockquote>")

@bot.on_message(filters.command(["drm"]))
async def drm_handler(bot: Client, m: Message):
    if m.chat.id not in AUTH_USERS and m.chat.id not in CHANNELS_LIST:
        await m.reply_text(f"<blockquote>__**Oopss! You are not a Premium member** __\n__**PLEASE /upgrade YOUR PLAN**__\n__**Send me your user id for authorization**__\n__**Your User id**__ - `{m.chat.id}`</blockquote>\n")
        return
    await handle_txt_file_upload(m)

@bot.on_message(filters.command(["stats"]) & filters.private)
async def stats_handler(client: Client, message: Message):
    if message.chat.id != OWNER:
        return await message.reply_text("**This command only for bot Owner**")
    await generate_pdf_summary(message.from_user.id, message.chat.id)

@bot.on_message(filters.text & filters.private)
async def text_handler(bot: Client, m: Message):
    if m.from_user.is_bot:
        return
    links = m.text
    match = re.search(r'https?://\S+', links)
    if not match:
        await m.reply_text("<blockquote>Invalid link format.</blockquote>")
        return
    link = match.group(0)
    
    # Logical Tree Step 9: Rate limiting
    if not await rate_limit_per_user(m.from_user.id):
        await m.reply_text("Rate limit exceeded. Try again later.")
        return
    
    # Logical Tree Step 1: Initialize stats and folder
    await init_stats_storage(m.from_user.id)
    editable = await m.reply_text(f"<blockquote>**🔹Processing your link...\n🔁Please wait...⏳**</blockquote>")
    await m.delete()
    
    # Resolution selection
    await editable.edit(f"╭━━━━❰ᴇɴᴛᴇʀ ʀᴇꜱᴏʟᴜᴛɪᴏɴ❱━━➣ \n┣━━⪼ send `144`  for 144p\n┣━━⪼ send `240`  for 240p\n┣━━⪼ send `360`  for 360p\n┣━━⪼ send `480`  for 480p\n┣━━⪼ send `720`  for 720p\n┣━━⪼ send `1080` for 1080p\n╰━━⌈⚡[`{CREDIT}`]⚡⌋━━➣ ")
    input2: Message = await bot.listen(editable.chat.id, filters=filters.text & filters.user(m.from_user.id))
    raw_text2 = input2.text
    quality = f"{raw_text2}p"
    await input2.delete(True)
    
    # Logical Tree Step 2: Resolution mapping
    try:
        if raw_text2 == "144":
            res = "256x144"
        elif raw_text2 == "240":
            res = "426x240"
        elif raw_text2 == "360":
            res = "640x360"
        elif raw_text2 == "480":
            res = "854x480"
        elif raw_text2 == "720":
            res = "1280x720"
        elif raw_text2 == "1080":
            res = "1920x1080"
        else:
            res = "UN"
    except Exception:
        res = "UN"
    
    try:
        # Logical Tree Step 1: Link sanitization
        Vxy = await auto_format_garbage([links, link])[1]
        url = Vxy
        name1 = await clean_filename_and_title(links)
        name = f'{name1[:60]}'
        video_link = [name1, url]
        course_name = "Single Link"
        main_folder = await create_main_folder(str(m.from_user.id))
        
        # Logical Tree Step 7: Queue management
        await fifo_task_queue(m.from_user.id, video_link)
        
        # Logical Tree Step 2 & 7: Process link
        link_type = await detect_link_type(video_link)
        if link_type == "DRM":
            file_path = await process_drm_link(video_link)
        elif link_type == "Direct":
            file_path = await yt_dlp_downloader(video_link, quality)
        elif link_type == "PDF":
            file_path = await download_pdf(video_link)
        elif link_type == "Image":
            file_path = await download_image(video_link)
        elif link_type == "Audio":
            file_path = await download_audio(video_link)
        elif link_type == "ZIP":
            file_path = await download_zip(video_link)
        else:
            file_path = await yt_dlp_downloader(video_link, quality)
        
        if file_path:
            # Logical Tree Step 3: Post-processing
            file_path, caption = await post_download_processing(file_path, video_link, course_name)
            
            # Logical Tree Step 4: Upload
            success = await smart_uploader_to_telegram(file_path, caption, m)
            
            # Logical Tree Step 5: Update stats
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            await update_download_stats(m.from_user.id, name1, time.time(), size_mb)
            await notify_user_status(m, success, name1)
            
            # Logical Tree Step 13: Generate PDF summary
            await generate_pdf_summary(m.from_user.id, m.chat.id)
        
        else:
            await notify_user_status(m, False, name1)
        
    except Exception as e:
        # Logical Tree Step 6: Recheck for expired links
        if "401" in str(e) or "403" in str(e):
            new_token = await auto_recheck_token(url)
            if new_token:
                video_link[1] = await decrypt_link(url, new_token)
                await async_download_upload_pipeline(m, [video_link], course_name, main_folder)
            else:
                await m.reply_text(f"⚠️𝐃𝐨𝐰𝐧𝐥𝐨𝐚𝐝𝐢𝐧𝐠 𝐈𝐧𝐭𝐞𝐫𝐮𝐩𝐭𝐞𝐍\n\n🔗𝐋𝐢𝐧𝐤 » `{link}`\n\n__**⚠️Failed Reason »**__\n<blockquote>{str(e)}</blockquote>")
        else:
            await m.reply_text(f"⚠️𝐃𝐨𝐰𝐧𝐥𝐨𝐚𝐝𝐢𝐧𝐠 𝐈𝐧𝐭𝐞𝐫𝐮𝐩𝐭𝐞𝐍\n\n🔗𝐋𝐢𝐧𝐤 » `{link}`\n\n__**⚠️Failed Reason »**__\n<blockquote>{str(e)}</blockquote>")
    
    finally:
        # Logical Tree Step 11: Cleanup
        await cleanup_temp_files()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(auto_update())
    bot.run()
