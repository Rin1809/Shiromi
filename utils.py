# --- START OF FILE utils.py ---
import discord
from discord.ext import commands
import datetime
import time
from dotenv import load_dotenv
import os
from typing import Optional, Union, List, Any, Dict
import logging
import re
# import markdown # Bỏ comment nếu thực sự dùng

log = logging.getLogger(__name__)

# --- Cấu hình Emoji (Load từ .env hoặc fallback) ---
# Load dotenv chỉ một lần ở đây
load_dotenv()

# Lấy giá trị từ env hoặc dùng fallback
EMOJI_IDS = {
    "stats": os.getenv("EMOJI_STATS", "📊"),
    "text_channel": os.getenv("EMOJI_TEXT_CHANNEL", "📄"),
    "voice_channel": os.getenv("EMOJI_VOICE_CHANNEL", "🔊"),
    "user_activity": os.getenv("EMOJI_USER_ACTIVITY", "👥"),
    "boost": os.getenv("EMOJI_BOOST", "<:boost:123>"), # ID placeholder ngắn gọn
    "boost_animated": os.getenv("EMOJI_BOOST_ANIMATED", "<a:boost:123>"),
    "error": os.getenv("EMOJI_ERROR", "‼️"), # Emoji lỗi nổi bật hơn
    "success": os.getenv("EMOJI_SUCCESS", "✅"),
    "loading": os.getenv("EMOJI_LOADING", "⏳"),
    "clock": os.getenv("EMOJI_CLOCK", "⏱️"),
    "calendar": os.getenv("EMOJI_CALENDAR", "📅"),
    "crown": os.getenv("EMOJI_CROWN", "👑"),
    "members": os.getenv("EMOJI_MEMBERS", "👥"),
    "bot_tag": os.getenv("EMOJI_BOT_TAG", "🤖"),
    "role": os.getenv("EMOJI_ROLE", "<:role:123>"),
    "id_card": os.getenv("EMOJI_ID_CARD", "🆔"),
    "shield": os.getenv("EMOJI_SHIELD", "🛡️"),
    "lock": os.getenv("EMOJI_LOCK", "🔐"),
    "bell": os.getenv("EMOJI_BELL", "🔔"),
    "rules": os.getenv("EMOJI_RULES", "📜"),
    "megaphone": os.getenv("EMOJI_MEGAPHONE", "📢"),
    "zzz": os.getenv("EMOJI_AFK", "💤"), # Đổi key cho AFK
    "star": os.getenv("EMOJI_STAR_FEATURE", "✨"),
    "online": os.getenv("EMOJI_STATUS_ONLINE", "🟢"),
    "idle": os.getenv("EMOJI_STATUS_IDLE", "🌙"),
    "dnd": os.getenv("EMOJI_STATUS_DND", "⛔"),
    "offline": os.getenv("EMOJI_STATUS_OFFLINE", "⚫"),
    "info": os.getenv("EMOJI_INFO", "ℹ️"),
    "category": os.getenv("EMOJI_CATEGORY", "📁"),
    "stage": os.getenv("EMOJI_STAGE_CHANNEL", "🎤"),
    "forum": os.getenv("EMOJI_FORUM_CHANNEL", "💬"),
    "invite": os.getenv("EMOJI_INVITE", "🔗"),
    "webhook": os.getenv("EMOJI_WEBHOOK", "<:webhook:123>"),
    "integration": os.getenv("EMOJI_INTEGRATION", "🔌"),
    "csv_file": os.getenv("EMOJI_CSV_FILE", "💾"),
    "json_file": os.getenv("EMOJI_JSON_FILE", "<:json:123>"),
    "mention": os.getenv("EMOJI_MENTION", "@"),
    "hashtag": os.getenv("EMOJI_HASHTAG", "#️⃣"),
    "thread": os.getenv("EMOJI_THREAD", "<:thread:123>"),
    "warning": os.getenv("EMOJI_WARNING", "⚠️"),
    "reaction": os.getenv("EMOJI_REACTION", "👍"),
    "link": os.getenv("EMOJI_LINK", "🔗"),
    "image": os.getenv("EMOJI_IMAGE", "🖼️"),
    "sticker": os.getenv("EMOJI_STICKER", "✨"), # Emoji sticker mặc định
    "award": os.getenv("EMOJI_AWARD", "🏆"),
    "reply": os.getenv("EMOJI_REPLY", "↪️"), # Emoji cho reply
}

_emoji_cache: Dict[str, str] = {} # Cache để tránh tìm kiếm emoji liên tục
_bot_ref_for_emoji: Optional[discord.Client] = None # Tham chiếu bot để lấy emoji guild

def set_bot_reference_for_emoji(bot: discord.Client):
    """Lưu tham chiếu đến bot để sử dụng trong get_emoji."""
    global _bot_ref_for_emoji
    _bot_ref_for_emoji = bot
    log.debug(f"Tham chiếu bot đã được đặt cho utils. Bot ID: {bot.user.id if bot and bot.user else 'N/A'}")
    # Xóa cache cũ khi bot thay đổi (ví dụ: reconnect với emoji khác)
    _emoji_cache.clear()

def get_emoji(name: str, bot: Optional[discord.Client] = None) -> str:
    """
    Lấy chuỗi emoji dựa trên tên.
    Ưu tiên emoji tùy chỉnh từ .env (nếu bot có thể truy cập).
    Nếu không, dùng fallback unicode hoặc placeholder.
    """
    target_bot = bot if bot else _bot_ref_for_emoji
    fallback = EMOJI_IDS.get(name, "❓") # Lấy giá trị từ dict (có thể là custom hoặc unicode)

    # Nếu fallback là unicode hoặc không có bot, trả về ngay
    if not isinstance(fallback, str) or not fallback.startswith(("<:", "<a:")) or not target_bot:
        return str(fallback) # Đảm bảo trả về string

    # Nếu là emoji custom và có bot instance
    cache_key = f"{target_bot.user.id if target_bot.user else 'unknown'}_{name}"
    if cache_key in _emoji_cache:
        return _emoji_cache[cache_key]

    # Cố gắng tìm emoji custom trong cache của bot
    try:
        # Phân tích chuỗi emoji custom (vd: <:name:id>)
        partial_emoji = discord.PartialEmoji.from_str(fallback)
        if partial_emoji.id:
            found_emoji = target_bot.get_emoji(partial_emoji.id)
            if found_emoji:
                result = str(found_emoji)
                _emoji_cache[cache_key] = result # Lưu vào cache
                return result
            else:
                 log.debug(f"Không tìm thấy emoji ID {partial_emoji.id} cho '{name}' trong cache.")
        # Tạm bỏ qua tìm bằng tên vì không đáng tin cậy và chậm
        # if partial_emoji.name: ...
    except ValueError:
        log.warning(f"Chuỗi emoji '{name}' không hợp lệ: {fallback}")
    except Exception as e:
        log.debug(f"Lỗi khi lấy/parse emoji '{name}' từ bot cache: {e}")

    # Nếu không tìm thấy trong cache bot, dùng fallback (là chuỗi custom ban đầu)
    # Không cache fallback này vì nó có thể trở nên hợp lệ sau này
    # _emoji_cache[cache_key] = fallback
    log.debug(f"Không tìm thấy emoji '{name}' trong cache của bot. Sử dụng fallback string: {fallback}")
    return fallback


# --- Các hàm tiện ích khác ---

def format_timedelta(delta: Optional[datetime.timedelta], high_precision=False) -> str:
    """Định dạng timedelta thành chuỗi thân thiện."""
    if not isinstance(delta, datetime.timedelta):
        return "N/A"

    try:
        total_seconds = delta.total_seconds()
        # Xử lý trường hợp âm (có thể xảy ra do lỗi logic)
        if total_seconds < 0:
             log.warning(f"format_timedelta nhận giá trị âm: {delta}")
             return "TG âm?"

        total_seconds = int(total_seconds) # Làm tròn xuống giây

        days, remainder = divmod(total_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)

        parts = []
        # Chỉ hiển thị ngày nếu dùng high_precision và có ngày
        if high_precision and days > 0:
            parts.append(f"{days} ngày")

        # Gộp giờ của ngày vào giờ nếu không hiển thị ngày riêng
        current_hours = hours + (days * 24 if not (high_precision and days > 0) else 0)
        if current_hours > 0:
            parts.append(f"{current_hours} giờ")

        if minutes > 0:
            parts.append(f"{minutes} phút")

        # Luôn hiển thị giây nếu không có đơn vị lớn hơn
        if seconds > 0 or not parts:
            # Hiển thị ms nếu < 1 giây và high_precision
            if high_precision and total_seconds < 1 and delta.microseconds > 0:
                ms = delta.microseconds // 1000
                parts.append(f"{ms} ms" if ms > 0 else "<1 giây")
            else:
                parts.append(f"{seconds} giây")

        return " ".join(parts) if parts else "0 giây"

    except Exception as e:
        log.warning(f"Lỗi format timedelta '{delta}': {e}")
        return "Lỗi TG"


def format_discord_time(dt_obj: Optional[datetime.datetime], style='f') -> str:
    """Định dạng datetime thành timestamp Discord. Ưu tiên format_dt."""
    if not isinstance(dt_obj, datetime.datetime):
        return "N/A"

    try:
        # discord.utils.format_dt xử lý timezone và locale tốt hơn
        return discord.utils.format_dt(dt_obj, style=style)
    except Exception as e:
        # Fallback nếu format_dt lỗi (hiếm khi xảy ra với datetime hợp lệ)
        log.warning(f"Lỗi discord.utils.format_dt cho '{dt_obj}' style '{style}': {e}. Thử fallback...")
        try:
            # Chuẩn hóa về UTC cho fallback strftime
            if dt_obj.tzinfo is None:
                dt_utc = dt_obj.replace(tzinfo=datetime.timezone.utc)
            else:
                dt_utc = dt_obj.astimezone(datetime.timezone.utc)
            return dt_utc.strftime('%d/%m/%Y %H:%M UTC')
        except Exception as e_fallback:
            log.error(f"Lỗi fallback strftime cho '{dt_obj}': {e_fallback}")
            return "Lỗi Ngày"


async def fetch_user_data(guild: Optional[discord.Guild], user_id: int, *, bot_ref: Optional[discord.Client] = None) -> Optional[Union[discord.Member, discord.User]]:
    """
    Lấy dữ liệu User hoặc Member một cách hiệu quả.
    Ưu tiên cache -> fetch member -> fetch user.
    """
    if not isinstance(user_id, int):
        log.warning(f"fetch_user_data nhận user_id không phải int: {user_id} ({type(user_id)})")
        return None

    user: Optional[Union[discord.Member, discord.User]] = None

    # 1. Thử lấy từ cache của guild (nếu có guild)
    if guild:
        user = guild.get_member(user_id)
        if user:
            # log.debug(f"Tìm thấy member {user_id} trong cache guild {guild.id}.")
            return user

    # 2. Thử fetch member từ guild (nếu có guild và cache miss)
    if guild:
        try:
            # log.debug(f"Cache miss member {user_id} guild {guild.id}, đang fetch...")
            user = await guild.fetch_member(user_id)
            # log.debug(f"Fetch member {user_id} guild {guild.id} thành công.")
            return user
        except discord.NotFound:
            # log.debug(f"Member {user_id} không tìm thấy trong guild {guild.id} khi fetch.")
            user = None # Member không có trong guild
        except discord.HTTPException as e:
            # Chỉ log lỗi nếu không phải 404 Not Found hoặc 403 Forbidden (có thể do intent thiếu)
            if e.status not in [404, 403]:
                log.warning(f"HTTP Lỗi fetch member {user_id} guild {guild.id}: {e.status} {e.text}")
            user = None
        except Exception as e:
            log.error(f"Lỗi không xác định fetch member {user_id} guild {guild.id}: {e}", exc_info=False)
            user = None

    # 3. Thử fetch user global (nếu không tìm thấy member hoặc không có guild)
    # Dùng bot_ref được truyền vào hoặc _bot_ref_for_emoji
    effective_bot = bot_ref if bot_ref else _bot_ref_for_emoji
    if not user and effective_bot and isinstance(effective_bot, (discord.Client, commands.Bot)):
        try:
            # log.debug(f"Không tìm thấy member {user_id}, đang fetch user global...")
            user = await effective_bot.fetch_user(user_id)
            # log.debug(f"Fetch user global {user_id} thành công.")
            return user
        except discord.NotFound:
            # log.debug(f"User {user_id} không tìm thấy global.")
            user = None # User không tồn tại
        except discord.HTTPException as e:
            if e.status != 404:
                log.warning(f"HTTP Lỗi fetch user {user_id} global: {e.status} {e.text}")
            user = None
        except Exception as e:
            log.error(f"Lỗi không xác định fetch user {user_id} global: {e}", exc_info=False)
            user = None

    # log.debug(f"Không thể fetch dữ liệu cho user {user_id}.")
    return user # Trả về None nếu không tìm thấy


def map_status(status: Optional[discord.Status], bot: Optional[discord.Client] = None) -> str:
    """Chuyển đổi discord.Status thành chuỗi có emoji."""
    e = lambda name: get_emoji(name, bot)
    if status is None:
        return f"{e('offline')} Không rõ" # Trạng thái không xác định

    status_map = {
        discord.Status.online: f"{e('online')} Online",
        discord.Status.idle: f"{e('idle')} Idle",
        discord.Status.dnd: f"{e('dnd')} DND",
        discord.Status.offline: f"{e('offline')} Offline",
        discord.Status.invisible: f"{e('offline')} Invisible", # Cũng coi là offline
    }
    # Fallback nếu có trạng thái lạ
    return status_map.get(status, f"{e('error')} Unknown ({status})")


def get_channel_type_emoji(channel_like: Optional[Union[discord.abc.GuildChannel, discord.Thread, discord.ChannelType, str]], bot: Optional[discord.Client] = None) -> str:
    """Lấy emoji tương ứng với loại kênh/thread."""
    e = lambda name: get_emoji(name, bot)
    if channel_like is None:
        return "❓"

    channel_type_enum: Optional[discord.ChannelType] = None

    # Xác định ChannelType từ các loại input khác nhau
    if isinstance(channel_like, (discord.abc.GuildChannel, discord.Thread)):
        channel_type_enum = channel_like.type
    elif isinstance(channel_like, discord.ChannelType):
        channel_type_enum = channel_like
    elif isinstance(channel_like, str):
        # Thử khớp tên enum trước
        try:
            channel_type_enum = discord.ChannelType[channel_like.lower().replace(' ', '_')]
        except KeyError:
            # Fallback khớp string đơn giản nếu tên enum không đúng
            cl = channel_like.lower()
            if 'text' in cl: channel_type_enum = discord.ChannelType.text
            elif 'voice' in cl: channel_type_enum = discord.ChannelType.voice
            elif 'stage' in cl: channel_type_enum = discord.ChannelType.stage_voice
            elif 'forum' in cl: channel_type_enum = discord.ChannelType.forum
            elif 'thread' in cl: channel_type_enum = discord.ChannelType.public_thread # Mặc định là public thread
            elif 'category' in cl: channel_type_enum = discord.ChannelType.category
            elif 'news' in cl or 'announcement' in cl: channel_type_enum = discord.ChannelType.news
            else: channel_type_enum = None # Không xác định được

    # Map ChannelType đã xác định với emoji
    if channel_type_enum is not None:
        type_emoji_map = {
            discord.ChannelType.text: e('text_channel'),
            discord.ChannelType.voice: e('voice_channel'),
            discord.ChannelType.category: e('category'),
            discord.ChannelType.stage_voice: e('stage'),
            discord.ChannelType.forum: e('forum'),
            discord.ChannelType.public_thread: e('thread'),
            discord.ChannelType.private_thread: e('thread'),
            discord.ChannelType.news_thread: e('thread'),
            discord.ChannelType.news: e('megaphone'), # Kênh Announcement/News
        }
        return type_emoji_map.get(channel_type_enum, "❓") # Fallback nếu type lạ

    return "❓" # Trả về mặc định nếu không xác định được type


def sanitize_for_csv(value: Any) -> str:
    """Chuẩn hóa giá trị để ghi vào CSV một cách an toàn."""
    if value is None:
        return ""
    # Chuyển đổi thành string, xóa null byte và xuống dòng không cần thiết
    text_str = str(value).replace('\x00', '').replace('\r', '').replace('\n', ' ')
    # Thoát dấu ngoặc kép bằng cách nhân đôi chúng
    text_str = text_str.replace('"', '""')
    # Nếu chuỗi chứa dấu phẩy, dấu ngoặc kép, hoặc bắt đầu/kết thúc bằng khoảng trắng,
    # thì bao nó trong dấu ngoặc kép.
    if ',' in text_str or '"' in text_str or text_str.startswith(' ') or text_str.endswith(' '):
        return f'"{text_str}"'
    return text_str


def parse_slowmode(slowmode_str: Union[str, int, None]) -> int:
    """Trích xuất số giây từ chuỗi slowmode (vd: "5 giây")."""
    if isinstance(slowmode_str, int):
        return slowmode_str
    if not isinstance(slowmode_str, str):
        return 0
    # Tìm tất cả chữ số trong chuỗi
    num_part = ''.join(filter(str.isdigit, slowmode_str))
    return int(num_part) if num_part else 0


def parse_bitrate(bitrate_str: Union[str, int, None]) -> int:
    """Trích xuất số bps từ chuỗi bitrate (vd: "64 kbps")."""
    if isinstance(bitrate_str, int):
        return bitrate_str # Giả sử đã là bps
    if not isinstance(bitrate_str, str):
        return 0
    num_part = ''.join(filter(str.isdigit, bitrate_str))
    bps = int(num_part) if num_part else 0
    # Chuyển đổi kbps sang bps nếu cần
    if "kbps" in bitrate_str.lower():
        bps *= 1000
    return bps


def create_progress_bar(percentage: float, length: int = 20) -> str:
    """Tạo thanh tiến trình dạng text đơn giản."""
    if not 0 <= percentage <= 100:
        percentage = max(0.0, min(100.0, percentage)) # Đảm bảo trong khoảng 0-100

    length = max(1, length) # Đảm bảo độ dài tối thiểu là 1
    filled_length = min(length, int(length * percentage / 100.0))
    bar = '█' * filled_length + '-' * (length - filled_length)
    return f"[{bar}] {percentage:.1f}%"


def escape_markdown(text: Optional[str]) -> str:
    """Thoát các ký tự đặc biệt của Markdown bằng hàm của discord.py."""
    if text is None:
        return ""
    # Sử dụng hàm có sẵn của discord.py để đảm bảo đúng chuẩn
    return discord.utils.escape_markdown(str(text))

# --- END OF FILE utils.py ---