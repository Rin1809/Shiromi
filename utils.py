# --- START OF FILE utils.py ---
import discord
from discord.ext import commands
import datetime
import time
from dotenv import load_dotenv
import os
from typing import Optional, Union, List, Any, Dict, Set, Counter, Tuple
import logging
import re
from collections import Counter # <<< THÊM IMPORT
import asyncio # <<< THÊM IMPORT

log = logging.getLogger(__name__)

# --- Cấu hình Emoji (Load từ .env hoặc fallback) ---
load_dotenv()
# Định nghĩa EMOJI_IDS từ .env hoặc giá trị mặc định
EMOJI_IDS = {
    "stats": os.getenv("EMOJI_STATS", "📊"),
    "text_channel": os.getenv("EMOJI_TEXT_CHANNEL", "📄"),
    "voice_channel": os.getenv("EMOJI_VOICE_CHANNEL", "🔊"),
    "user_activity": os.getenv("EMOJI_USER_ACTIVITY", "👥"),
    "boost": os.getenv("EMOJI_BOOST", "<:g_hCastoCozy:1360103927009378456>"),
    "boost_animated": os.getenv("EMOJI_BOOST_ANIMATED", "<a:Eru_shika:1260952522882027582>"),
    "error": os.getenv("EMOJI_ERROR", "⚠️"),
    "success": os.getenv("EMOJI_SUCCESS", "✅"),
    "loading": os.getenv("EMOJI_LOADING", "⏳"),
    "clock": os.getenv("EMOJI_CLOCK", "⏱️"),
    "calendar": os.getenv("EMOJI_CALENDAR", "📅"),
    "crown": os.getenv("EMOJI_CROWN", "👑"),
    "members": os.getenv("EMOJI_MEMBERS", "👥"),
    "bot_tag": os.getenv("EMOJI_BOT_TAG", "🤖"),
    "role": os.getenv("EMOJI_ROLE", "<:a_cann:1360113811788398652>"),
    "id_card": os.getenv("EMOJI_ID_CARD", "🆔"),
    "shield": os.getenv("EMOJI_SHIELD", "🛡️"),
    "lock": os.getenv("EMOJI_LOCK", "🔐"),
    "bell": os.getenv("EMOJI_BELL", "🔔"),
    "rules": os.getenv("EMOJI_RULES", "📜"),
    "megaphone": os.getenv("EMOJI_MEGAPHONE", "📢"),
    "zzz": os.getenv("EMOJI_AFK", "💤"), # Sửa tên key thành zzz nếu dùng EMOJI_AFK
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
    "webhook": os.getenv("EMOJI_WEBHOOK", "<:webhook:your_webhook_emoji_id>"), # Giữ nguyên ID placeholder nếu không có trong .env
    "integration": os.getenv("EMOJI_INTEGRATION", "🔌"),
    "csv_file": os.getenv("EMOJI_CSV_FILE", "💾"),
    "json_file": os.getenv("EMOJI_JSON_FILE", "<:json:12345>"), # Giữ nguyên ID placeholder nếu không có trong .env
    "mention": os.getenv("EMOJI_MENTION", "@"),
    "hashtag": os.getenv("EMOJI_HASHTAG", "#"),
    "thread": os.getenv("EMOJI_THREAD", "<a:z_1049623938931630101:1274398186508783649>"),
    "warning": os.getenv("EMOJI_WARNING", "⚠️"),
    "reaction": os.getenv("EMOJI_REACTION", "👍"),
    "link": os.getenv("EMOJI_LINK", "🔗"),
    "image": os.getenv("EMOJI_IMAGE", "🖼️"),
    "sticker": os.getenv("EMOJI_STICKER", "✨"),
    "award": os.getenv("EMOJI_AWARD", "🏆"),
    "reply": os.getenv("EMOJI_REPLY", "↪️"), # Thêm key reply nếu chưa có
}


_emoji_cache: Dict[str, str] = {}
_bot_ref_for_emoji: Optional[discord.Client] = None

def set_bot_reference_for_emoji(bot: discord.Client):
    global _bot_ref_for_emoji
    _bot_ref_for_emoji = bot
    log.debug(f"Tham chiếu bot đã được đặt cho utils. Bot ID: {bot.user.id if bot and bot.user else 'N/A'}")
    _emoji_cache.clear()

def get_emoji(name: str, bot: Optional[discord.Client] = None) -> str:
    target_bot = bot if bot else _bot_ref_for_emoji
    fallback = EMOJI_IDS.get(name, "❓")
    if not isinstance(fallback, str) or not fallback.startswith(("<:", "<a:")) or not target_bot:
        return str(fallback)
    cache_key = f"{target_bot.user.id if target_bot.user else 'unknown'}_{name}"
    if cache_key in _emoji_cache: return _emoji_cache[cache_key]
    try:
        partial_emoji = discord.PartialEmoji.from_str(fallback)
        if partial_emoji.id:
            found_emoji = target_bot.get_emoji(partial_emoji.id)
            if found_emoji: _emoji_cache[cache_key] = str(found_emoji); return str(found_emoji)
            else: log.debug(f"Không tìm thấy emoji ID {partial_emoji.id} cho '{name}' trong cache.")
    except ValueError: log.warning(f"Chuỗi emoji '{name}' không hợp lệ: {fallback}")
    except Exception as e: log.debug(f"Lỗi khi lấy/parse emoji '{name}' từ bot cache: {e}")
    log.debug(f"Không tìm thấy emoji '{name}' trong cache của bot. Sử dụng fallback string: {fallback}")
    return fallback

# --- Các hàm tiện ích khác ---
def format_timedelta(delta: Optional[datetime.timedelta], high_precision=False) -> str:
    if not isinstance(delta, datetime.timedelta): return "N/A"
    try:
        total_seconds = delta.total_seconds()
        if total_seconds < 0: log.warning(f"format_timedelta nhận giá trị âm: {delta}"); return "TG âm?"
        total_seconds = int(total_seconds)
        days, remainder = divmod(total_seconds, 86400); hours, remainder = divmod(remainder, 3600); minutes, seconds = divmod(remainder, 60)
        parts = []
        if high_precision and days > 0: parts.append(f"{days} ngày")
        current_hours = hours + (days * 24 if not (high_precision and days > 0) else 0)
        if current_hours > 0: parts.append(f"{current_hours} giờ")
        if minutes > 0: parts.append(f"{minutes} phút")
        if seconds > 0 or not parts:
            if high_precision and total_seconds < 1 and hasattr(delta, 'microseconds') and delta.microseconds > 0: # Check microseconds exists
                ms = delta.microseconds // 1000; parts.append(f"{ms} ms" if ms > 0 else "<1 giây")
            else: parts.append(f"{seconds} giây")
        return " ".join(parts) if parts else "0 giây"
    except Exception as e: log.warning(f"Lỗi format timedelta '{delta}': {e}"); return "Lỗi TG"

def format_discord_time(dt_obj: Optional[datetime.datetime], style='f') -> str:
    if not isinstance(dt_obj, datetime.datetime): return "N/A"
    try: return discord.utils.format_dt(dt_obj, style=style)
    except Exception as e:
        log.warning(f"Lỗi discord.utils.format_dt cho '{dt_obj}' style '{style}': {e}. Thử fallback...")
        try:
            if dt_obj.tzinfo is None: dt_utc = dt_obj.replace(tzinfo=datetime.timezone.utc)
            else: dt_utc = dt_obj.astimezone(datetime.timezone.utc)
            return dt_utc.strftime('%d/%m/%Y %H:%M UTC')
        except Exception as e_fallback: log.error(f"Lỗi fallback strftime cho '{dt_obj}': {e_fallback}"); return "Lỗi Ngày"

async def fetch_user_data(guild: Optional[discord.Guild], user_id: int, *, bot_ref: Optional[discord.Client] = None) -> Optional[Union[discord.Member, discord.User]]:
    if not isinstance(user_id, int): log.warning(f"fetch_user_data nhận user_id không phải int: {user_id} ({type(user_id)})"); return None
    user: Optional[Union[discord.Member, discord.User]] = None
    if guild: user = guild.get_member(user_id);
    if user: return user
    if guild:
        try: user = await guild.fetch_member(user_id); return user
        except discord.NotFound: user = None
        except discord.HTTPException as e:
            if e.status not in [404, 403]: log.warning(f"HTTP Lỗi fetch member {user_id} guild {guild.id}: {e.status} {e.text}")
            user = None
        except Exception as e: log.error(f"Lỗi không xác định fetch member {user_id} guild {guild.id}: {e}", exc_info=False); user = None
    effective_bot = bot_ref if bot_ref else _bot_ref_for_emoji
    if not user and effective_bot and isinstance(effective_bot, (discord.Client, commands.Bot)):
        try: user = await effective_bot.fetch_user(user_id); return user
        except discord.NotFound: user = None
        except discord.HTTPException as e:
            if e.status != 404: log.warning(f"HTTP Lỗi fetch user {user_id} global: {e.status} {e.text}")
            user = None
        except Exception as e: log.error(f"Lỗi không xác định fetch user {user_id} global: {e}", exc_info=False); user = None
    return user

def map_status(status: Optional[discord.Status], bot: Optional[discord.Client] = None) -> str:
    e = lambda name: get_emoji(name, bot)
    if status is None: return f"{e('offline')} Không rõ"
    status_map = { discord.Status.online: f"{e('online')} Online", discord.Status.idle: f"{e('idle')} Idle", discord.Status.dnd: f"{e('dnd')} DND", discord.Status.offline: f"{e('offline')} Offline", discord.Status.invisible: f"{e('offline')} Invisible" }
    return status_map.get(status, f"{e('error')} Unknown ({status})")

def get_channel_type_emoji(channel_like: Optional[Union[discord.abc.GuildChannel, discord.Thread, discord.ChannelType, str]], bot: Optional[discord.Client] = None) -> str:
    e = lambda name: get_emoji(name, bot)
    if channel_like is None: return "❓"
    channel_type_enum: Optional[discord.ChannelType] = None
    if isinstance(channel_like, (discord.abc.GuildChannel, discord.Thread)): channel_type_enum = channel_like.type
    elif isinstance(channel_like, discord.ChannelType): channel_type_enum = channel_like
    elif isinstance(channel_like, str):
        try: channel_type_enum = discord.ChannelType[channel_like.lower().replace(' ', '_')]
        except KeyError:
            cl = channel_like.lower()
            if 'text' in cl: channel_type_enum = discord.ChannelType.text
            elif 'voice' in cl: channel_type_enum = discord.ChannelType.voice
            elif 'stage' in cl: channel_type_enum = discord.ChannelType.stage_voice
            elif 'forum' in cl: channel_type_enum = discord.ChannelType.forum
            elif 'thread' in cl: channel_type_enum = discord.ChannelType.public_thread
            elif 'category' in cl: channel_type_enum = discord.ChannelType.category
            elif 'news' in cl or 'announcement' in cl: channel_type_enum = discord.ChannelType.news
            else: channel_type_enum = None
    if channel_type_enum is not None:
        type_emoji_map = { discord.ChannelType.text: e('text_channel'), discord.ChannelType.voice: e('voice_channel'), discord.ChannelType.category: e('category'), discord.ChannelType.stage_voice: e('stage'), discord.ChannelType.forum: e('forum'), discord.ChannelType.public_thread: e('thread'), discord.ChannelType.private_thread: e('thread'), discord.ChannelType.news_thread: e('thread'), discord.ChannelType.news: e('megaphone') }
        return type_emoji_map.get(channel_type_enum, "❓")
    return "❓"

def sanitize_for_csv(value: Any) -> str:
    if value is None: return ""
    text_str = str(value).replace('\x00', '').replace('\r', '').replace('\n', ' ')
    text_str = text_str.replace('"', '""')
    if ',' in text_str or '"' in text_str or text_str.startswith(' ') or text_str.endswith(' '):
        return f'"{text_str}"'
    return text_str

def parse_slowmode(slowmode_str: Union[str, int, None]) -> int:
    if isinstance(slowmode_str, int): return slowmode_str
    if not isinstance(slowmode_str, str): return 0
    num_part = ''.join(filter(str.isdigit, slowmode_str))
    return int(num_part) if num_part else 0

def parse_bitrate(bitrate_str: Union[str, int, None]) -> int:
    if isinstance(bitrate_str, int): return bitrate_str
    if not isinstance(bitrate_str, str): return 0
    num_part = ''.join(filter(str.isdigit, bitrate_str))
    bps = int(num_part) if num_part else 0
    if "kbps" in bitrate_str.lower(): bps *= 1000
    return bps

def create_progress_bar(percentage: float, length: int = 20) -> str:
    if not 0 <= percentage <= 100: percentage = max(0.0, min(100.0, percentage))
    length = max(1, length)
    filled_length = min(length, int(length * percentage / 100.0))
    bar = '█' * filled_length + '-' * (length - filled_length)
    return f"[{bar}] {percentage:.1f}%"

def escape_markdown(text: Optional[str]) -> str:
    if text is None: return ""
    return discord.utils.escape_markdown(str(text))


def get_user_rank(
    user_id: int,
    ranking_data: Dict[str, Dict[int, int]], # Dict chứa hạng đã tính toán
    rank_key: str # Key của BXH cần lấy hạng (vd: 'messages', 'tracked_role_123')
) -> Optional[int]:
    """Lấy thứ hạng của user từ dữ liệu xếp hạng đã chuẩn bị."""
    return ranking_data.get(rank_key, {}).get(user_id)

# --- THÊM HÀM HELPER _fetch_user_dict ---
async def _fetch_user_dict(guild: discord.Guild, user_ids: List[int], bot: Union[discord.Client, commands.Bot]) -> Dict[int, Optional[Union[discord.Member, discord.User]]]:
    """Fetch a list of users/members efficiently and return a dictionary."""
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    if not user_ids: return user_cache

    # Loại bỏ ID trùng lặp và không hợp lệ
    valid_user_ids = list(set(uid for uid in user_ids if isinstance(uid, int)))

    # Tối ưu: Lấy từ cache guild trước nếu có thể
    remaining_ids = []
    for uid in valid_user_ids:
        member = guild.get_member(uid)
        if member:
            user_cache[uid] = member
        else:
            remaining_ids.append(uid)

    # Fetch những user còn lại
    if remaining_ids:
        log.debug(f"Fetching {len(remaining_ids)} remaining users for dict cache...")
        fetch_tasks = [fetch_user_data(guild, user_id, bot_ref=bot) for user_id in remaining_ids]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        for idx, result in enumerate(results):
            user_id = remaining_ids[idx]
            if isinstance(result, (discord.User, discord.Member)):
                user_cache[user_id] = result
            else:
                user_cache[user_id] = None # Mark as not found or error
            if isinstance(result, Exception):
                log.debug(f"Failed to fetch user {user_id} for dict: {result}")
    return user_cache
# --- KẾT THÚC THÊM HÀM HELPER _fetch_user_dict ---

# --- THÊM HÀM HELPER FETCH STICKER DICT ---
async def _fetch_sticker_dict(sticker_ids: List[int], bot: Union[discord.Client, commands.Bot]) -> Dict[int, str]:
    """Fetch sticker names efficiently and return a dictionary {id: name}."""
    sticker_cache: Dict[int, str] = {}
    if not sticker_ids or not bot:
        return sticker_cache

    # Loại bỏ ID trùng lặp và không hợp lệ
    unique_sticker_ids = list(set(sid for sid in sticker_ids if isinstance(sid, int)))
    if not unique_sticker_ids: return sticker_cache

    async def fetch_sticker_name(sticker_id):
        try:
            sticker = await bot.fetch_sticker(sticker_id)
            return sticker_id, sticker.name if sticker else "Unknown/Deleted"
        except discord.NotFound:
            return sticker_id, "Unknown/Deleted"
        except Exception as e:
            log.debug(f"Failed to fetch sticker {sticker_id}: {e}")
            return sticker_id, "Fetch Error"

    log.debug(f"Fetching names for {len(unique_sticker_ids)} unique stickers...")
    fetch_tasks = [fetch_sticker_name(sid) for sid in unique_sticker_ids]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    for res in results:
        if isinstance(res, tuple):
            sticker_id, name = res
            sticker_cache[sticker_id] = name
        elif isinstance(res, Exception):
            # Lỗi không mong muốn, không lưu vào cache
            pass
    log.debug(f"Sticker name fetch complete. Cache size: {len(sticker_cache)}")
    return sticker_cache
# --- KẾT THÚC HÀM HELPER FETCH STICKER DICT ---

# --- THÊM HÀM LẤY TIMEZONE OFFSET ---
# Biến toàn cục để lưu offset đã tính (tránh tính lại nhiều lần)
local_timezone_offset_hours: Optional[int] = None

def get_local_timezone_offset() -> int:
    """Trả về offset timezone local so với UTC tính bằng giờ."""
    global local_timezone_offset_hours
    if local_timezone_offset_hours is None:
        try:
            # time.timezone trả về offset tính bằng giây phía TÂY UTC (nên cần đảo dấu)
            local_offset_seconds = time.timezone
            # Chia cho 3600 để đổi sang giờ, dùng round để xử lý offset 30 phút
            local_timezone_offset_hours = round(local_offset_seconds / -3600)
            log.info(f"Xác định timezone offset của bot: UTC{local_timezone_offset_hours:+d}")
        except Exception as tz_err:
            log.warning(f"Không thể xác định timezone offset của bot: {tz_err}. Mặc định về UTC (0).")
            local_timezone_offset_hours = 0 # Fallback về UTC nếu lỗi
    return local_timezone_offset_hours
# --- KẾT THÚC HÀM LẤY TIMEZONE OFFSET ---

# --- END OF FILE utils.py ---