# --- START OF FILE config.py ---
import os
import sys
import logging
from dotenv import load_dotenv
import discord
from typing import List, Optional, Set, Tuple 

log = logging.getLogger(__name__)

# --- Tải biến môi trường ---
load_dotenv()

# --- Helper Function ---
def _parse_id_list(env_var_name: str) -> Set[int]:
    """Phân tích chuỗi ID cách nhau bởi dấu phẩy từ biến môi trường thành set các integer."""
    id_str = os.getenv(env_var_name)
    if not id_str:
        return set()
    try:
        return {int(item.strip()) for item in id_str.split(',') if item.strip().isdigit()}
    except ValueError:
        log.error(f"Lỗi phân tích danh sách ID trong biến môi trường '{env_var_name}'. Vui lòng kiểm tra định dạng (chỉ chứa số, cách nhau bởi dấu phẩy).")
        return set()

def _parse_unicode_list(env_var_name: str) -> Set[str]:
    """Phân tích chuỗi unicode emojis cách nhau bởi dấu phẩy."""
    emoji_str = os.getenv(env_var_name)
    if not emoji_str:
        return set()
    return {item.strip() for item in emoji_str.split(',') if item.strip()}


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

# --- Deep Scan Enhancement Configs ---
TRACKED_ROLE_GRANT_IDS: Set[int] = _parse_id_list("TRACKED_ROLE_GRANT_IDS")
log.info(f"IDs Role Grant được theo dõi: {TRACKED_ROLE_GRANT_IDS if TRACKED_ROLE_GRANT_IDS else 'Không có'}")

DM_REPORT_RECIPIENT_ROLE_ID_STR = os.getenv("DM_REPORT_RECIPIENT_ROLE_ID")
DM_REPORT_RECIPIENT_ROLE_ID: Optional[int] = None
if DM_REPORT_RECIPIENT_ROLE_ID_STR and DM_REPORT_RECIPIENT_ROLE_ID_STR.isdigit():
    DM_REPORT_RECIPIENT_ROLE_ID = int(DM_REPORT_RECIPIENT_ROLE_ID_STR)
    log.info(f"ID Role nhận DM báo cáo: {DM_REPORT_RECIPIENT_ROLE_ID}")
else:
    log.info("Không cấu hình Role nhận DM báo cáo.")

BOOSTER_THANKYOU_ROLE_IDS: Set[int] = _parse_id_list("BOOSTER_THANKYOU_ROLE_IDS")
log.info(f"IDs Role được cảm ơn trong DM: {BOOSTER_THANKYOU_ROLE_IDS if BOOSTER_THANKYOU_ROLE_IDS else 'Không có'}")

REACTION_UNICODE_EXCEPTIONS: Set[str] = _parse_unicode_list("REACTION_UNICODE_EXCEPTIONS")
log.info(f"Unicode Reactions được phép trong BXH: {REACTION_UNICODE_EXCEPTIONS if REACTION_UNICODE_EXCEPTIONS else 'Không có (chỉ custom emoji server)'}")

ADMIN_ROLE_IDS_FILTER: Set[int] = _parse_id_list("ADMIN_ROLE_IDS_FILTER")
log.info(f"IDs Role Admin bổ sung cần lọc khỏi BXH: {ADMIN_ROLE_IDS_FILTER if ADMIN_ROLE_IDS_FILTER else 'Không có'}")


# --- Cấu hình Audit Log Actions ---
# (Giữ nguyên phần này)
AUDIT_LOG_ACTIONS_TO_TRACK_STR = os.getenv("AUDIT_LOG_ACTIONS_TO_TRACK")
AUDIT_LOG_ACTIONS_TO_TRACK = None
if AUDIT_LOG_ACTIONS_TO_TRACK is None:
    AUDIT_LOG_ACTIONS_TO_TRACK = [

        discord.AuditLogAction.kick, discord.AuditLogAction.ban, discord.AuditLogAction.unban,
        discord.AuditLogAction.member_role_update, discord.AuditLogAction.member_update,
        discord.AuditLogAction.message_delete, # Có thể hữu ích để xem mod xóa gì
        discord.AuditLogAction.thread_create, # Để đếm user tạo thread
        discord.AuditLogAction.member_move, discord.AuditLogAction.member_disconnect,
    ]
log.info(f"Audit log actions được theo dõi: {[a.name for a in AUDIT_LOG_ACTIONS_TO_TRACK]}")


# --- Kiểm tra cấu hình quan trọng ---
def check_critical_config():
    """Kiểm tra các cấu hình bắt buộc và thoát nếu thiếu."""
    critical_missing = []
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN":
        critical_missing.append("DISCORD_TOKEN")
    if not DATABASE_URL:
        critical_missing.append("DATABASE_URL")
    if not ADMIN_USER_ID: # Chủ bot vẫn là critical
         critical_missing.append("ADMIN_USER_ID")

    if critical_missing:
        print("[LỖI CẤU HÌNH NGHIÊM TRỌNG] Thiếu các biến môi trường sau:")
        for missing in critical_missing:
            print(f"- {missing}")
        sys.exit(1)

# --- END OF FILE config.py ---
