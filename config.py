# --- START OF FILE config.py ---
import os
import sys
import logging
from dotenv import load_dotenv
import discord
from typing import List, Optional, Set, Tuple, Dict
import json

log = logging.getLogger(__name__)

# --- Tải biến môi trường ---
load_dotenv()
# Trong config.py, gần các hằng số khác
MAX_CONCURRENT_CHANNEL_SCANS = int(os.getenv("MAX_CONCURRENT_CHANNEL_SCANS", "5"))
log.info(f"Số kênh/luồng quét đồng thời tối đa: {MAX_CONCURRENT_CHANNEL_SCANS}")
# --- Helper Function ---
# (Giữ nguyên các hàm helper: _parse_id_list, _parse_unicode_list, _parse_id, _load_quy_toc_anh_mapping)
def _parse_id_list(env_var_name: str) -> Set[int]:
    id_str = os.getenv(env_var_name)
    if not id_str: return set()
    try: return {int(item.strip()) for item in id_str.split(',') if item.strip().isdigit()}
    except ValueError: log.error(f"Lỗi phân tích danh sách ID trong biến môi trường '{env_var_name}'."); return set()

def _parse_unicode_list(env_var_name: str) -> Set[str]:
    emoji_str = os.getenv(env_var_name);
    if not emoji_str: return set()
    return {item.strip() for item in emoji_str.split(',') if item.strip()}

def _parse_id(env_var_name: str) -> Optional[int]:
    id_str = os.getenv(env_var_name)
    if id_str and id_str.isdigit(): return int(id_str)
    elif id_str: log.warning(f"Giá trị không hợp lệ cho ID trong biến môi trường '{env_var_name}'.");
    return None

QUY_TOC_ANH_FILE = "quy_toc_anh.json"
def _load_quy_toc_anh_mapping() -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not os.path.exists(QUY_TOC_ANH_FILE): log.warning(f"File ảnh quý tộc '{QUY_TOC_ANH_FILE}' không tồn tại."); return mapping
    try:
        with open(QUY_TOC_ANH_FILE, 'r', encoding='utf-8') as f: mapping = json.load(f)
        if not isinstance(mapping, dict): log.error(f"File '{QUY_TOC_ANH_FILE}' không chứa JSON object hợp lệ."); return {}
        valid_mapping = {}
        for key, value in mapping.items():
            if key.isdigit() and isinstance(value, str): valid_mapping[key] = value
            else: log.warning(f"Bỏ qua cặp key-value không hợp lệ trong '{QUY_TOC_ANH_FILE}': key='{key}'")
        log.info(f"Đã tải {len(valid_mapping)} mapping ảnh quý tộc từ '{QUY_TOC_ANH_FILE}'."); return valid_mapping
    except json.JSONDecodeError: log.error(f"Lỗi giải mã JSON trong file '{QUY_TOC_ANH_FILE}'."); return {}
    except Exception as e: log.error(f"Lỗi không xác định khi đọc file '{QUY_TOC_ANH_FILE}': {e}", exc_info=True); return {}

# --- Lấy các giá trị cấu hình ---
BOT_TOKEN = os.getenv("DISCORD_TOKEN", "YOUR_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_USER_ID_STR = os.getenv("ADMIN_USER_ID")
ADMIN_USER_ID = int(ADMIN_USER_ID_STR) if ADMIN_USER_ID_STR and ADMIN_USER_ID_STR.isdigit() else None
COMMAND_PREFIX = os.getenv("COMMAND_PREFIX", "!")
MIN_MESSAGE_COUNT_FOR_REPORT = int(os.getenv("MIN_MESSAGE_COUNT_FOR_REPORT", 100))
FINAL_STICKER_ID_STR = os.getenv("FINAL_STICKER_ID")
FINAL_STICKER_ID = int(FINAL_STICKER_ID_STR) if FINAL_STICKER_ID_STR and FINAL_STICKER_ID_STR.isdigit() else None
BOT_NAME = os.getenv("BOT_NAME", "Shiromi")
ENABLE_REACTION_SCAN = os.getenv("ENABLE_REACTION_SCAN", "False").lower() == "true"
WEBSITE_BASE_URL = os.getenv("WEBSITE_BASE_URL", "http://localhost:3000")

# --- Deep Scan Enhancement Configs ---
TRACKED_ROLE_GRANT_IDS: Set[int] = _parse_id_list("TRACKED_ROLE_GRANT_IDS")
log.info(f"IDs Role Grant được theo dõi: {TRACKED_ROLE_GRANT_IDS if TRACKED_ROLE_GRANT_IDS else 'Không có'}")
DM_REPORT_RECIPIENT_ROLE_ID_STR = os.getenv("DM_REPORT_RECIPIENT_ROLE_ID")
DM_REPORT_RECIPIENT_ROLE_ID: Optional[int] = None
if DM_REPORT_RECIPIENT_ROLE_ID_STR and DM_REPORT_RECIPIENT_ROLE_ID_STR.isdigit():
    DM_REPORT_RECIPIENT_ROLE_ID = int(DM_REPORT_RECIPIENT_ROLE_ID_STR)
    log.info(f"ID Role nhận DM báo cáo: {DM_REPORT_RECIPIENT_ROLE_ID}")
else: log.info("Không cấu hình Role nhận DM báo cáo.")
BOOSTER_THANKYOU_ROLE_IDS: Set[int] = _parse_id_list("BOOSTER_THANKYOU_ROLE_IDS")
log.info(f"IDs Role được cảm ơn trong DM: {BOOSTER_THANKYOU_ROLE_IDS if BOOSTER_THANKYOU_ROLE_IDS else 'Không có'}")
REACTION_UNICODE_EXCEPTIONS: Set[str] = _parse_unicode_list("REACTION_UNICODE_EXCEPTIONS")
log.info(f"Unicode Reactions được phép trong BXH: {REACTION_UNICODE_EXCEPTIONS if REACTION_UNICODE_EXCEPTIONS else 'Không có'}")
ADMIN_ROLE_IDS_FILTER: Set[int] = _parse_id_list("ADMIN_ROLE_IDS_FILTER")
log.info(f"IDs Role Admin bổ sung cần lọc khỏi BXH: {ADMIN_ROLE_IDS_FILTER if ADMIN_ROLE_IDS_FILTER else 'Không có'}")
QUY_TOC_ANH_MAPPING: Dict[str, str] = _load_quy_toc_anh_mapping()

# --- THÊM CONFIG MỚI ---
EXCLUDED_CATEGORY_IDS: Set[int] = _parse_id_list("EXCLUDED_CATEGORY_IDS")
log.info(f"IDs Category bị loại trừ khỏi quét: {EXCLUDED_CATEGORY_IDS if EXCLUDED_CATEGORY_IDS else 'Không có'}")
# ---------------------

# --- Cấu hình Kênh Báo cáo và Sticker/Emoji ---
REPORT_CHANNEL_ID: Optional[int] = _parse_id("REPORT_CHANNEL_ID")
log.info(f"ID Kênh gửi báo cáo: {REPORT_CHANNEL_ID}" if REPORT_CHANNEL_ID else "Gửi báo cáo vào kênh gốc.")
INTERMEDIATE_STICKER_ID: Optional[int] = _parse_id("INTERMEDIATE_STICKER_ID")
log.info(f"ID Sticker trung gian (A): {INTERMEDIATE_STICKER_ID}" if INTERMEDIATE_STICKER_ID else "Không có sticker trung gian (A).")
LEAST_STICKER_ID: Optional[int] = _parse_id("LEAST_STICKER_ID")
log.info(f"ID Sticker 'Ít Nhất' (B): {LEAST_STICKER_ID}" if LEAST_STICKER_ID else "Không có sticker 'ít nhất' (B).")
MOST_STICKER_ID: Optional[int] = _parse_id("MOST_STICKER_ID")
log.info(f"ID Sticker 'Nhiều Nhất' (C): {MOST_STICKER_ID}" if MOST_STICKER_ID else "Không có sticker 'nhiều nhất' (C).")
FINAL_DM_EMOJI: str = os.getenv("FINAL_DM_EMOJI", "🎉")
log.info(f"Emoji cuối DM: {FINAL_DM_EMOJI}")

# --- Cấu hình Audit Log Actions ---
AUDIT_LOG_ACTIONS_TO_TRACK_STR = os.getenv("AUDIT_LOG_ACTIONS_TO_TRACK")
AUDIT_LOG_ACTIONS_TO_TRACK = None
if AUDIT_LOG_ACTIONS_TO_TRACK is None:
    AUDIT_LOG_ACTIONS_TO_TRACK = [
        discord.AuditLogAction.kick, discord.AuditLogAction.ban, discord.AuditLogAction.unban,
        discord.AuditLogAction.member_role_update, discord.AuditLogAction.member_update,
        discord.AuditLogAction.message_delete, discord.AuditLogAction.thread_create,
        discord.AuditLogAction.member_move, discord.AuditLogAction.member_disconnect,
    ]
log.info(f"Audit log actions được theo dõi: {[a.name for a in AUDIT_LOG_ACTIONS_TO_TRACK]}")

# --- Kiểm tra cấu hình quan trọng ---
def check_critical_config():
    critical_missing = []
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN": critical_missing.append("DISCORD_TOKEN")
    if not DATABASE_URL: critical_missing.append("DATABASE_URL")
    if not ADMIN_USER_ID: critical_missing.append("ADMIN_USER_ID")
    if not WEBSITE_BASE_URL or WEBSITE_BASE_URL == "http://localhost:3000":
        log.warning("Biến môi trường WEBSITE_BASE_URL chưa được đặt hoặc đang dùng giá trị mặc định.")

    if critical_missing:
        print("[LỖI CẤU HÌNH NGHIÊM TRỌNG] Thiếu các biến môi trường sau:")
        for missing in critical_missing: print(f"- {missing}")
        sys.exit(1)

# --- END OF FILE config.py ---