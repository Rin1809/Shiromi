# --- START OF FILE config.py ---
import os
import sys
import logging
from dotenv import load_dotenv
import discord # Cần cho AuditLogAction

log = logging.getLogger(__name__)

# --- Tải biến môi trường ---
load_dotenv()

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

# --- Cấu hình Audit Log Actions ---
AUDIT_LOG_ACTIONS_TO_TRACK_STR = os.getenv("AUDIT_LOG_ACTIONS_TO_TRACK")
AUDIT_LOG_ACTIONS_TO_TRACK = None

if AUDIT_LOG_ACTIONS_TO_TRACK_STR:
    action_names = [name.strip().lower() for name in AUDIT_LOG_ACTIONS_TO_TRACK_STR.split(',') if name.strip()]
    AUDIT_LOG_ACTIONS_TO_TRACK = []
    invalid_actions = []
    for name in action_names:
        try:
            action_enum = getattr(discord.AuditLogAction, name)
            AUDIT_LOG_ACTIONS_TO_TRACK.append(action_enum)
        except AttributeError:
            invalid_actions.append(name)
    if invalid_actions:
        log.warning(f"Loại action audit log không hợp lệ trong .env: {', '.join(invalid_actions)}")
    if not AUDIT_LOG_ACTIONS_TO_TRACK:
        log.warning("Không có action audit log hợp lệ nào được cấu hình trong .env, sử dụng default.")
        AUDIT_LOG_ACTIONS_TO_TRACK = None # Reset để dùng default bên dưới
else:
    log.info("Không có cấu hình AUDIT_LOG_ACTIONS_TO_TRACK trong .env, sử dụng default.")

# Default actions nếu không có hoặc không hợp lệ trong .env
if AUDIT_LOG_ACTIONS_TO_TRACK is None:
    AUDIT_LOG_ACTIONS_TO_TRACK = [
        discord.AuditLogAction.kick, discord.AuditLogAction.ban, discord.AuditLogAction.unban,
        discord.AuditLogAction.member_role_update, discord.AuditLogAction.member_update,
        discord.AuditLogAction.role_create, discord.AuditLogAction.role_delete, discord.AuditLogAction.role_update,
        discord.AuditLogAction.channel_create, discord.AuditLogAction.channel_delete, discord.AuditLogAction.channel_update,
        discord.AuditLogAction.invite_create, discord.AuditLogAction.invite_delete, discord.AuditLogAction.invite_update,
        discord.AuditLogAction.webhook_create, discord.AuditLogAction.webhook_delete, discord.AuditLogAction.webhook_update,
        discord.AuditLogAction.message_delete, discord.AuditLogAction.message_bulk_delete,
        discord.AuditLogAction.thread_create, discord.AuditLogAction.thread_delete, discord.AuditLogAction.thread_update,
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
    # Thêm kiểm tra ADMIN_USER_ID nếu nó là bắt buộc
    # if not ADMIN_USER_ID:
    #     critical_missing.append("ADMIN_USER_ID")

    if critical_missing:
        # Dùng print ở đây vì log có thể chưa được cấu hình khi hàm này chạy
        print("[LỖI CẤU HÌNH NGHIÊM TRỌNG] Thiếu các biến môi trường sau:")
        for missing in critical_missing:
            print(f"- {missing}")
        sys.exit(1)

# --- END OF FILE config.py ---