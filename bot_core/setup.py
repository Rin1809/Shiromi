# --- START OF FILE bot_core/setup.py ---
import sys
import logging
import discord
from discord.ext import commands
from rich.logging import RichHandler

import config # Cần import config để đọc cài đặt
import discord_logging # Cần import để lấy handler

log = logging.getLogger(__name__)

MIN_PYTHON = (3, 8)

def check_python_version():
    """Kiểm tra phiên bản Python."""
    if sys.version_info < MIN_PYTHON:
        print(f"Lỗi: Yêu cầu Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]} trở lên.")
        print(f"Phiên bản hiện tại: {sys.version}")
        sys.exit(1)

def create_intents() -> discord.Intents:
    """Tạo và cấu hình Intents cho bot."""
    log.debug("Đang tạo Intents...")
    intents = discord.Intents.default()

    # Intents cơ bản thường cần
    intents.guilds = True           # Để nhận thông tin về server bot tham gia
    intents.messages = True         # Để nhận tin nhắn (cần cho lệnh)
    intents.guild_messages = True   # Để nhận tin nhắn trong server

    # Intents đặc quyền (Privileged Intents) - Cần bật trên Developer Portal
    intents.members = True          # Để nhận thông tin members, join/leave events
    intents.message_content = True  # Để đọc nội dung tin nhắn
    intents.presences = True        # Để xem trạng thái online/offline/game của members

    # Intents tùy chọn khác
    intents.invites = True          # Để lấy thông tin invite
    intents.voice_states = True     # Để xem ai đang trong voice channel, join/leave voice
    if config.ENABLE_REACTION_SCAN: # Chỉ bật nếu cấu hình yêu cầu
        intents.reactions = True
        log.info("[bold yellow]Đã bật Reaction Intent để quét biểu cảm.[/bold yellow]")

    # Kiểm tra các Intents quan trọng
    if not all([intents.guilds, intents.members, intents.message_content]):
         # Dùng print vì log có thể chưa cấu hình xong
         print("[CRITICAL LỖI] Thiếu các Privileged Intents quan trọng (Guilds, Members, Message Content)! Bot không thể hoạt động đúng.")
         sys.exit(1)
    if config.ENABLE_REACTION_SCAN and not intents.reactions:
        print("[CRITICAL LỖI] ENABLE_REACTION_SCAN bật nhưng thiếu Reaction Intent!")
        sys.exit(1)

    # Cảnh báo nếu thiếu intent tùy chọn nhưng quan trọng
    if not intents.presences:
        log.warning("[bold yellow]Cảnh báo:[/bold yellow] Presences Intent đang tắt. Thông tin trạng thái user có thể không đầy đủ.")
    if not intents.voice_states:
        log.warning("[bold yellow]Cảnh báo:[/bold yellow] Voice States Intent đang tắt. Thông tin kênh voice có thể không đầy đủ.")

    log.debug("Tạo Intents hoàn tất.")
    return intents

def configure_logging():
    """Cấu hình hệ thống logging cho toàn bộ ứng dụng."""
    log.info("Đang cấu hình logging...")
    check_python_version() # Kiểm tra Python trước khi làm gì khác

    # ---- Handlers ----
    # 1. Rich Handler cho Console (log đẹp)
    rich_handler = RichHandler(
        rich_tracebacks=True,
        markup=True,
        show_path=False, # Không hiển thị đường dẫn file cho gọn
        log_time_format="[%H:%M:%S]"
    )
    rich_handler.setLevel(logging.INFO) # Chỉ hiện INFO trở lên trên console

    # 2. Discord Queue Handler (đưa log vào queue để gửi lên Discord)
    # Handler này lấy từ module discord_logging
    discord_queue_handler = discord_logging.DiscordLogHandler()
    discord_queue_handler.setLevel(logging.DEBUG) # Gửi cả DEBUG log lên Discord

    # ---- Root Logger ----
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG) # Mức thấp nhất để bắt mọi log

    # Xóa handlers cũ nếu có (phòng trường hợp re-run)
    if root_logger.hasHandlers():
        log.debug(f"Xóa {len(root_logger.handlers)} handler(s) cũ khỏi root logger.")
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

    # Thêm handlers mới
    root_logger.addHandler(rich_handler)
    root_logger.addHandler(discord_queue_handler)
    log.debug("Đã thêm RichHandler và DiscordLogHandler vào root logger.")

    # ---- Cấu hình Loggers của Thư viện ----
    # Giảm độ chi tiết của log từ discord.py và asyncpg
    logging.getLogger("discord").setLevel(logging.INFO)
    logging.getLogger("discord.http").setLevel(logging.WARNING) # Log HTTP của discord ít hơn
    logging.getLogger("asyncpg").setLevel(logging.WARNING)

    # Thông báo cấu hình hoàn tất
    log.info("[bold green]Cấu hình logging hoàn tất.[/]")
    # Log ví dụ các cấp độ
    log.debug("Đây là log DEBUG.")
    log.info("Đây là log INFO.")
    log.warning("Đây là log WARNING.")
    log.error("Đây là log ERROR.")
    # log.critical("Đây là log CRITICAL.") # Tránh log critical không cần thiết

# --- END OF FILE bot_core/setup.py ---