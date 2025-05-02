# --- START OF FILE utils.py ---
import discord
from discord.ext import commands
import datetime
import time
from dotenv import load_dotenv
import os
from typing import Optional, Union, List # Thêm List
import logging
import re # Thêm re

log = logging.getLogger(__name__)

# --- Cấu hình Emoji ---
load_dotenv()
# Cập nhật danh sách emoji đầy đủ hơn
EMOJI_IDS = {
    "stats": os.getenv("EMOJI_STATS", "📊"),
    "text_channel": os.getenv("EMOJI_TEXT_CHANNEL", "📄"),
    "voice_channel": os.getenv("EMOJI_VOICE_CHANNEL", "🔊"),
    "user_activity": os.getenv("EMOJI_USER_ACTIVITY", "👥"),
    "boost": os.getenv("EMOJI_BOOST", "<:boost:123>"), # Placeholder
    "boost_animated": os.getenv("EMOJI_BOOST_ANIMATED", "<a:boost_animated:123>"), # Placeholder
    "error": os.getenv("EMOJI_ERROR", "‼️"), # Changed from ⚠️
    "success": os.getenv("EMOJI_SUCCESS", "✅"),
    "loading": os.getenv("EMOJI_LOADING", "⏳"),
    "clock": os.getenv("EMOJI_CLOCK", "⏱️"),
    "calendar": os.getenv("EMOJI_CALENDAR", "📅"),
    "crown": os.getenv("EMOJI_CROWN", "👑"),
    "members": os.getenv("EMOJI_MEMBERS", "👥"),
    "bot_tag": os.getenv("EMOJI_BOT_TAG", "🤖"),
    "role": os.getenv("EMOJI_ROLE", "<:role:123>"), # Placeholder
    "id_card": os.getenv("EMOJI_ID_CARD", "🆔"),
    "shield": os.getenv("EMOJI_SHIELD", "🛡️"),
    "lock": os.getenv("EMOJI_LOCK", "🔐"),
    "bell": os.getenv("EMOJI_BELL", "🔔"),
    "rules": os.getenv("EMOJI_RULES", "📜"),
    "megaphone": os.getenv("EMOJI_MEGAPHONE", "📢"),
    "zzz": os.getenv("EMOJI_AFK", "💤"),
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
    "webhook": os.getenv("EMOJI_WEBHOOK", "<:webhook:123>"), # Placeholder
    "integration": os.getenv("EMOJI_INTEGRATION", "🔌"),
    "csv_file": os.getenv("EMOJI_CSV_FILE", "💾"),
    "json_file": os.getenv("EMOJI_JSON_FILE", "<:json:12345>"), # Placeholder
    "mention": os.getenv("EMOJI_MENTION", "@"),
    "hashtag": os.getenv("EMOJI_HASHTAG", "#️⃣"), # Changed from #
    "thread": os.getenv("EMOJI_THREAD", "<:thread:12345>"), # Placeholder
    "warning": os.getenv("EMOJI_WARNING", "⚠️"),
    "reaction": os.getenv("EMOJI_REACTION", "👍"),
    "link": os.getenv("EMOJI_LINK", "🔗"),
    "image": os.getenv("EMOJI_IMAGE", "🖼️"),
    "sticker": os.getenv("EMOJI_STICKER", "✨"),
    "award": os.getenv("EMOJI_AWARD", "🏆"),
}

_emoji_cache = {}
_bot_ref_for_emoji = None

def set_bot_reference_for_emoji(bot: discord.Client):
    """Lưu tham chiếu đến đối tượng bot để get_emoji có thể truy cập emojis."""
    global _bot_ref_for_emoji
    _bot_ref_for_emoji = bot
    log.debug(f"Tham chiếu bot đã được đặt cho utils. Bot ID: {bot.user.id if bot and bot.user else 'N/A'}")

def get_emoji(name: str, bot: Optional[discord.Client] = None) -> str:
    """
    Lấy emoji tùy chỉnh hoặc fallback.
    Ưu tiên bot được truyền vào, sau đó là bot ref global.
    """
    target_bot = bot if bot else _bot_ref_for_emoji
    fallback = EMOJI_IDS.get(name, "❓")

    if isinstance(fallback, str) and (not fallback.startswith(("<:", "<a:")) or not target_bot):
        return fallback

    if isinstance(fallback, str) and (fallback.startswith("<:") or fallback.startswith("<a:")) and target_bot:
        cache_key = f"{target_bot.user.id if target_bot.user else 'unknown'}_{name}"
        if cache_key in _emoji_cache:
            return _emoji_cache[cache_key]

        emoji_id_match = re.search(r':(\d+)>$', fallback)
        emoji_id = int(emoji_id_match.group(1)) if emoji_id_match else None

        found_emoji = None
        if emoji_id:
             try:
                 found_emoji = target_bot.get_emoji(emoji_id)
             except Exception as e:
                 log.debug(f"Lỗi khi lấy emoji ID {emoji_id} cho '{name}': {e}")
                 found_emoji = None

        if not found_emoji:
             emoji_name_match = re.search(r'<a?:([^:]+):\d+>$', fallback)
             emoji_name = emoji_name_match.group(1) if emoji_name_match else name
             found_emoji = discord.utils.get(target_bot.emojis, name=emoji_name)

        if found_emoji:
            result = str(found_emoji)
            _emoji_cache[cache_key] = result
            return result
        else:
             _emoji_cache[cache_key] = fallback
             log.debug(f"Không tìm thấy emoji '{name}' (ID: {emoji_id}, Name: {emoji_name if 'emoji_name' in locals() else 'N/A'}) trong cache của bot. Sử dụng fallback: {fallback}")
             return fallback

    return "❓"


def format_timedelta(delta: Optional[datetime.timedelta], high_precision=False) -> str:
    """Định dạng timedelta thành chuỗi đọc được."""
    try:
        if not isinstance(delta, datetime.timedelta):
            return "N/A"
        total_seconds = int(delta.total_seconds())
        if total_seconds < 0: return "N/A"

        days, remainder = divmod(total_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)

        parts = []
        if high_precision and days > 0:
            parts.append(f"{days} ngày")
        current_hours = hours + (days * 24 if not (high_precision and days > 0) else 0)
        if current_hours > 0:
            parts.append(f"{current_hours} giờ")
        if minutes > 0:
            parts.append(f"{minutes} phút")
        if seconds > 0 or (not parts and total_seconds == 0):
            parts.append(f"{seconds} giây")
        elif not parts and total_seconds != 0:
             if high_precision and delta.microseconds > 0:
                 ms = delta.microseconds // 1000
                 if ms > 0: return f"~{ms} ms"
                 else: return "<1 giây"
             else: return "<1 giây"
        elif not parts and total_seconds == 0:
             return "0 giây"

        return " ".join(parts) if parts else "0 giây"
    except (AttributeError, TypeError, ValueError):
        log.warning(f"Không thể định dạng timedelta '{delta}'", exc_info=False)
        return "N/A"

def format_discord_time(dt_obj: Optional[datetime.datetime], style='f') -> str:
    """Định dạng datetime object thành timestamp Discord."""
    if isinstance(dt_obj, datetime.datetime):
        try:
            utc_dt = dt_obj.astimezone(datetime.timezone.utc) if dt_obj.tzinfo else dt_obj.replace(tzinfo=datetime.timezone.utc)
            return discord.utils.format_dt(utc_dt, style=style)
        except Exception as e:
            log.warning(f"Không thể định dạng datetime '{dt_obj}' bằng discord.utils.format_dt: {e}", exc_info=False)
            try:
                return dt_obj.strftime('%d/%m/%Y %H:%M UTC') # Fallback format
            except: return "Ngày không hợp lệ"
    return "N/A"

async def fetch_user_data(
    guild: Optional[discord.Guild],
    user_id: int,
    *,
    bot_ref: Optional[discord.Client] = None
) -> Optional[Union[discord.Member, discord.User]]:
    """
    Lấy dữ liệu User hoặc Member. Ưu tiên lấy Member từ Guild nếu có.
    Nếu không tìm thấy Member, thử fetch User toàn cục qua bot_ref.
    """
    user: Optional[Union[discord.Member, discord.User]] = None

    if guild:
        user = guild.get_member(user_id)

    if not user and guild:
        try:
            user = await guild.fetch_member(user_id)
        except discord.NotFound:
            user = None
        except discord.HTTPException as e:
            if e.status != 404:
                log.warning(f"HTTPException (Status {e.status}) khi fetch member {user_id} từ guild {guild.id}: {e.text}")
            user = None
        except Exception as e:
            log.error(f"Lỗi không xác định khi fetch member {user_id} từ guild {guild.id}: {e}", exc_info=False)
            user = None

    if not user and bot_ref and isinstance(bot_ref, (discord.Client, commands.Bot)):
        try:
            user = await bot_ref.fetch_user(user_id)
        except discord.NotFound:
            user = None
        except discord.HTTPException as e:
            if e.status != 404:
                log.warning(f"HTTPException (Status {e.status}) khi fetch user {user_id} toàn cục: {e.text}")
            user = None
        except Exception as e:
            log.error(f"Lỗi không xác định khi fetch user {user_id} toàn cục: {e}", exc_info=False)
            user = None
    elif not user and not bot_ref:
         log.debug(f"Không thể fetch user toàn cục cho {user_id}: bot_ref không khả dụng.")

    return user


def map_status(status: Optional[discord.Status], bot: Optional[discord.Client] = None) -> str:
    """Chuyển đổi trạng thái discord thành chuỗi có emoji."""
    e = lambda name: get_emoji(name, bot)
    if status is None:
         return f"{e('offline')} Không rõ/Ngoài Server"

    status_map = {
        discord.Status.online: f"{e('online')} Online",
        discord.Status.idle: f"{e('idle')} Idle",
        discord.Status.dnd: f"{e('dnd')} Do Not Disturb",
        discord.Status.offline: f"{e('offline')} Offline",
        discord.Status.invisible: f"{e('offline')} Invisible",
    }
    return status_map.get(status, f"{e('error')} Trạng thái không xác định")

def get_channel_type_emoji(channel: Optional[Union[discord.abc.GuildChannel, discord.Thread, str]], bot: Optional[discord.Client] = None) -> str:
    """Lấy emoji tương ứng với loại kênh hoặc tên loại kênh."""
    e = lambda name: get_emoji(name, bot)
    if channel is None: return "❓"

    channel_type_enum = None
    if isinstance(channel, discord.abc.GuildChannel): channel_type_enum = channel.type
    elif isinstance(channel, discord.Thread): channel_type_enum = channel.type
    elif isinstance(channel, str):
        try: channel_type_enum = discord.ChannelType[channel]
        except KeyError: pass

    if channel_type_enum == discord.ChannelType.text: return e('text_channel')
    if channel_type_enum == discord.ChannelType.voice: return e('voice_channel') # Added voice
    if channel_type_enum == discord.ChannelType.category: return e('category')
    if channel_type_enum == discord.ChannelType.stage_voice: return e('stage')
    if channel_type_enum == discord.ChannelType.forum: return e('forum')
    if channel_type_enum in (discord.ChannelType.public_thread, discord.ChannelType.private_thread, discord.ChannelType.news_thread): return e('thread')
    if channel_type_enum == discord.ChannelType.news: return e('megaphone')

    # Fallback check for strings if enum mapping failed or wasn't possible
    if isinstance(channel, str):
        if channel == 'text': return e('text_channel')
        if channel == 'voice': return e('voice_channel')
        if channel == 'category': return e('category')
        if channel == 'stage_voice': return e('stage')
        if channel == 'forum': return e('forum')
        if 'thread' in channel: return e('thread')
        if channel == 'news': return e('megaphone')

    return "❓"


def sanitize_for_csv(text: any) -> str:
    """Chuẩn hóa text để ghi vào CSV, tránh lỗi encoding hoặc ký tự đặc biệt."""
    if text is None:
        return ""
    text_str = str(text).replace('\x00', '').replace('\r', '').replace('\n', ' ')
    text_str = text_str.replace('"', '""')
    if ',' in text_str or '"' in text_str or '\n' in text_str:
         return f'"{text_str}"'
    return text_str

def parse_slowmode(slowmode_str: str) -> int:
    """Trích xuất số giây từ chuỗi slowmode."""
    if isinstance(slowmode_str, int): return slowmode_str
    if not isinstance(slowmode_str, str): return 0
    num_part = ''.join(filter(str.isdigit, slowmode_str))
    return int(num_part) if num_part else 0

def parse_bitrate(bitrate_str: str) -> int:
    """Trích xuất số bps từ chuỗi bitrate."""
    if isinstance(bitrate_str, int): return bitrate_str
    if not isinstance(bitrate_str, str): return 0
    num_part = ''.join(filter(str.isdigit, bitrate_str))
    bps = int(num_part) if num_part else 0
    if "kbps" in bitrate_str.lower():
        bps *= 1000
    return bps

# --- END OF FILE utils.py ---