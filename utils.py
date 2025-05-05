# --- START OF FILE utils.py ---
import discord
from discord.ext import commands
import datetime
import time
from dotenv import load_dotenv
import os
from typing import Optional, Union, List, Any, Dict, Set, Counter, Tuple, Callable # Thêm Callable
import logging
import re
from collections import Counter, defaultdict, OrderedDict
import asyncio
import math
import unicodedata
import collections
import config

log = logging.getLogger(__name__)

# --- Cấu hình Emoji ---
load_dotenv()
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
    "webhook": os.getenv("EMOJI_WEBHOOK", "<:webhook:12345>"),
    "integration": os.getenv("EMOJI_INTEGRATION", "🔌"),
    "csv_file": os.getenv("EMOJI_CSV_FILE", "💾"),
    "json_file": os.getenv("EMOJI_JSON_FILE", "<:json:12345>"),
    "mention": os.getenv("EMOJI_MENTION", "@"),
    "hashtag": os.getenv("EMOJI_HASHTAG", "#"),
    "thread": os.getenv("EMOJI_THREAD", "<a:z_1049623938931630101:1274398186508783649>"),
    "warning": os.getenv("EMOJI_WARNING", "⚠️"),
    "reaction": os.getenv("EMOJI_REACTION", "👍"),
    "link": os.getenv("EMOJI_LINK", "🔗"),
    "image": os.getenv("EMOJI_IMAGE", "🖼️"),
    "sticker": os.getenv("EMOJI_STICKER", "✨"),
    "award": os.getenv("EMOJI_AWARD", "🏆"),
    "reply": os.getenv("EMOJI_REPLY", "↪️"),
}

_emoji_cache: Dict[str, str] = {}
_bot_ref_for_emoji: Optional[discord.Client] = None

def set_bot_reference_for_emoji(bot: discord.Client):
    global _bot_ref_for_emoji
    _bot_ref_for_emoji = bot
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
    except ValueError: log.warning(f"Chuỗi emoji '{name}' không hợp lệ: {fallback}")
    except Exception as e: log.debug(f"Lỗi khi lấy/parse emoji '{name}' từ bot cache: {e}")
    return fallback

def format_timedelta(delta: Optional[datetime.timedelta], high_precision=False) -> str:
    if not isinstance(delta, datetime.timedelta): return "N/A"
    try:
        total_seconds = delta.total_seconds()
        if total_seconds < 0: return "TG âm?"
        total_seconds = int(total_seconds)
        days, remainder = divmod(total_seconds, 86400); hours, remainder = divmod(remainder, 3600); minutes, seconds = divmod(remainder, 60)
        parts = []
        if high_precision and days > 0: parts.append(f"{days} ngày")
        current_hours = hours + (days * 24 if not (high_precision and days > 0) else 0)
        if current_hours > 0: parts.append(f"{current_hours} giờ")
        if minutes > 0: parts.append(f"{minutes} phút")
        if seconds > 0 or not parts:
            if high_precision and total_seconds < 1 and hasattr(delta, 'microseconds') and delta.microseconds > 0:
                ms = delta.microseconds // 1000; parts.append(f"{ms} ms" if ms > 0 else "<1 giây")
            else: parts.append(f"{seconds} giây")
        return " ".join(parts) if parts else "0 giây"
    except Exception as e: log.warning(f"Lỗi format timedelta '{delta}': {e}"); return "Lỗi TG"

def format_discord_time(dt_obj: Optional[datetime.datetime], style='f') -> str:
    if not isinstance(dt_obj, datetime.datetime): return "N/A"
    try: return discord.utils.format_dt(dt_obj, style=style)
    except Exception as e:
        try:
            if dt_obj.tzinfo is None: dt_utc = dt_obj.replace(tzinfo=datetime.timezone.utc)
            else: dt_utc = dt_obj.astimezone(datetime.timezone.utc)
            return dt_utc.strftime('%d/%m/%Y %H:%M UTC')
        except Exception as e_fallback: log.error(f"Lỗi fallback strftime cho '{dt_obj}': {e_fallback}"); return "Lỗi Ngày"

async def fetch_user_data(guild: Optional[discord.Guild], user_id: int, *, bot_ref: Optional[discord.Client] = None) -> Optional[Union[discord.Member, discord.User]]:
    if not isinstance(user_id, int): return None
    user: Optional[Union[discord.Member, discord.User]] = None
    if guild: user = guild.get_member(user_id);
    if user: return user
    if guild:
        try: user = await guild.fetch_member(user_id); return user
        except (discord.NotFound, discord.HTTPException): user = None
        except Exception as e: log.error(f"Lỗi fetch member {user_id} guild {guild.id}: {e}", exc_info=False); user = None
    effective_bot = bot_ref if bot_ref else _bot_ref_for_emoji
    if not user and effective_bot and isinstance(effective_bot, (discord.Client, commands.Bot)):
        try: user = await effective_bot.fetch_user(user_id); return user
        except (discord.NotFound, discord.HTTPException): user = None
        except Exception as e: log.error(f"Lỗi fetch user {user_id} global: {e}", exc_info=False); user = None
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

def get_user_rank(user_id: int, ranking_data: Dict[str, Dict[int, int]], rank_key: str) -> Optional[int]:
    return ranking_data.get(rank_key, {}).get(user_id)

async def _fetch_user_dict(guild: discord.Guild, user_ids: List[int], bot: Union[discord.Client, commands.Bot]) -> Dict[int, Optional[Union[discord.Member, discord.User]]]:
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    if not user_ids: return user_cache
    valid_user_ids = list(set(uid for uid in user_ids if isinstance(uid, int)))
    remaining_ids = []
    for uid in valid_user_ids:
        member = guild.get_member(uid)
        if member: user_cache[uid] = member
        else: remaining_ids.append(uid)
    if remaining_ids:
        fetch_tasks = [fetch_user_data(guild, user_id, bot_ref=bot) for user_id in remaining_ids]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        for idx, result in enumerate(results):
            user_id = remaining_ids[idx]
            if isinstance(result, (discord.User, discord.Member)): user_cache[user_id] = result
            else: user_cache[user_id] = None
            if isinstance(result, Exception): log.debug(f"Fetch user {user_id} failed: {result}")
    return user_cache

async def _fetch_sticker_dict(sticker_ids: List[int], bot: Union[discord.Client, commands.Bot]) -> Dict[int, str]:
    sticker_cache: Dict[int, str] = {}
    if not sticker_ids or not bot: return sticker_cache
    unique_sticker_ids = list(set(sid for sid in sticker_ids if isinstance(sid, int)))
    if not unique_sticker_ids: return sticker_cache
    async def fetch_sticker_name(sticker_id):
        try:
            sticker = await bot.fetch_sticker(sticker_id)
            return sticker_id, sticker.name if sticker else "Unknown/Deleted"
        except discord.NotFound: return sticker_id, "Unknown/Deleted"
        except Exception as e: log.debug(f"Fetch sticker {sticker_id} failed: {e}"); return sticker_id, "Fetch Error"
    fetch_tasks = [fetch_sticker_name(sid) for sid in unique_sticker_ids]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    for res in results:
        if isinstance(res, tuple): sticker_cache[res[0]] = res[1]
        elif isinstance(res, Exception): pass
    return sticker_cache

local_timezone_offset_hours: Optional[int] = None
def get_local_timezone_offset() -> int:
    global local_timezone_offset_hours
    if local_timezone_offset_hours is None:
        try:
            local_offset_seconds = time.timezone
            local_timezone_offset_hours = round(local_offset_seconds / -3600)
            log.info(f"Xác định timezone offset: UTC{local_timezone_offset_hours:+d}")
        except Exception as tz_err:
            log.warning(f"Không thể xác định timezone offset: {tz_err}. Mặc định UTC (0).")
            local_timezone_offset_hours = 0
    return local_timezone_offset_hours

async def create_vertical_text_bar_chart(
    sorted_data: List[Tuple[Any, Union[int, float]]],
    key_formatter: Optional[Callable] = None,
    top_n: int = 5,
    max_chart_height: int = 10,
    bar_width: int = 1,
    bar_spacing: int = 2,
    chart_title: Optional[str] = None,
    show_legend: bool = True,
    value_formatter: Optional[Callable] = None
) -> str:
    if not sorted_data: return ""
    data_to_chart = sorted_data[:top_n]
    if not data_to_chart: return ""
    numeric_values = [val for _, val in data_to_chart if isinstance(val, (int, float))]
    if not numeric_values: return "```text\n(Không có dữ liệu số)\n```"
    max_value = max(numeric_values) if numeric_values else 0
    if max_value <= 0: return "```text\n(Giá trị không dương)\n```"

    bar_heights = []
    for _, value in data_to_chart:
        height = 0
        if isinstance(value, (int, float)) and value > 0:
            height = max(1, round((value / max_value) * max_chart_height))
        bar_heights.append(int(height))

    chart_lines = []; bar_char = '█'; space_char = ' '; item_total_width = bar_width + bar_spacing
    for h in range(max_chart_height, 0, -1):
        line = ""
        for i in range(len(data_to_chart)):
            if bar_heights[i] >= h: line += (bar_char * bar_width) + (space_char * bar_spacing)
            else: line += (space_char * bar_width) + (space_char * bar_spacing)
        chart_lines.append(line.rstrip())

    axis_width = len(data_to_chart) * item_total_width - bar_spacing
    chart_lines.append('─' * axis_width)

    label_line = ""
    for i in range(len(data_to_chart)):
        rank_str = f"#{i+1}"; label_line += rank_str.center(item_total_width)
    chart_lines.append(label_line.rstrip())

    legend_lines = []
    if show_legend:
        legend_lines.append(""); key_format_tasks = []
        keys_to_format = [key for key, _ in data_to_chart]
        key_formatter = key_formatter or str # Default to str if no formatter

        for key in keys_to_format:
             if asyncio.iscoroutinefunction(key_formatter): key_format_tasks.append(key_formatter(key))
             elif callable(key_formatter):
                 try: result = key_formatter(key); key_format_tasks.append(asyncio.sleep(0, result=result)) # Wrap sync result
                 except Exception as e: log.debug(f"Lỗi key_formatter sync {key}: {e}"); key_format_tasks.append(asyncio.sleep(0, result=f"Lỗi ({key})"))
             else: key_format_tasks.append(asyncio.sleep(0, result=str(key)))

        formatted_keys = await asyncio.gather(*key_format_tasks, return_exceptions=True)
        value_formatter = value_formatter or (lambda v: f"{v:,}" if isinstance(v, (int, float)) else str(v))

        for i, (key, value) in enumerate(data_to_chart):
            key_str = "Error"; value_str = "Error"
            if i < len(formatted_keys):
                if isinstance(formatted_keys[i], Exception): key_str = f"Lỗi ({key})"
                else: key_str = str(formatted_keys[i])
            try: value_str = value_formatter(value)
            except Exception as e: log.debug(f"Lỗi value_formatter {value}: {e}")

            max_key_len = 30
            key_display = (key_str[:max_key_len] + '…') if len(key_str) > max_key_len else key_str
            legend_lines.append(f"`#{i+1}`: {key_display} ({value_str})")

    output_lines = []
    if chart_title: output_lines.append(f"📊 **{chart_title} (Top {len(data_to_chart)})**")
    output_lines.extend(chart_lines)
    if legend_lines: output_lines.extend(legend_lines)

    return "```text\n" + "\n".join(output_lines) + "\n```"

async def _format_user_tree_line(
    rank: int, user_id: int, main_value: Any, main_unit_singular: str, main_unit_plural: str,
    guild: discord.Guild, user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]],
    secondary_info: Optional[str] = None, tertiary_info: Optional[str] = None
) -> List[str]:
    lines = []; rank_prefix = f"`#{rank:02d}`"
    user_obj = user_cache.get(user_id)
    user_mention = user_obj.mention if user_obj else f"`{user_id}`"
    user_display_name = f" ({escape_markdown(user_obj.display_name)})" if user_obj else " (Unknown/Left)"
    lines.append(f"{rank_prefix} {user_mention}{user_display_name}")
    if isinstance(main_value, (int, float)):
        main_value_formatted = f"{main_value:,}"; main_unit = main_unit_plural if main_value != 1 else main_unit_singular
    else: main_value_formatted = str(main_value); main_unit = main_unit_plural
    if isinstance(main_value, str) and any(s in main_value for s in ["ngày", "giờ", "phút", "giây", "/", ":"]): lines.append(f"  `└` **{main_value_formatted}**")
    else:
        if isinstance(main_value, (int, float)) and main_value == 0: lines.append(f"  `└` **{main_value_formatted}** {main_unit}")
        elif main_value_formatted: lines.append(f"  `└` **{main_value_formatted}** {main_unit}")
    if secondary_info: lines.append(f"  `└` {secondary_info}")
    if tertiary_info: lines.append(f"  `└` {tertiary_info}")
    lines.append("")
    return lines

async def create_user_leaderboard_embed(
    title: str,
    counts: Optional[Union[collections.Counter, Dict[int, Any], collections.OrderedDict]],
    value_key: Optional[str],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot],
    limit: int,
    item_name_singular: str,
    item_name_plural: str,
    e: Callable,
    color: discord.Color,
    filter_admins: bool,
    sort_ascending: bool = False,
    secondary_info_getter: Optional[Callable] = None,
    tertiary_info_getter: Optional[Callable] = None,
    minimum_value: Optional[Union[int, float]] = None,
    show_bar_chart: bool = True
) -> Optional[discord.Embed]:
    if not counts: return None
    processed_users = []
    if isinstance(counts, (collections.Counter, collections.OrderedDict)):
        source_items = list(counts.items())
        if isinstance(counts, collections.Counter): source_items.sort(key=lambda item: item[1], reverse=not sort_ascending)
        for uid, count_val in source_items:
             if isinstance(count_val, (int, float)):
                 if minimum_value is None or (count_val >= minimum_value if not sort_ascending else count_val <= minimum_value):
                      processed_users.append((uid, count_val))
             elif isinstance(count_val, str):
                 if minimum_value is None: processed_users.append((uid, count_val))
    elif isinstance(counts, dict) and value_key:
        temp_list = []
        for uid, data in counts.items():
            if not data.get('is_bot', False):
                count_val = data.get(value_key, 0)
                if isinstance(count_val, (int, float)):
                    if minimum_value is None or (count_val >= minimum_value if not sort_ascending else count_val <= minimum_value):
                        temp_list.append((uid, count_val))
        temp_list.sort(key=lambda item: item[1], reverse=not sort_ascending)
        processed_users = temp_list
    else: return None

    if not processed_users: return None
    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [(uid, count_val) for uid, count_val in processed_users if (not filter_admins or not isinstance(uid, int) or not admin_ids_to_filter or uid not in admin_ids_to_filter)]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users); users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await _fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' and not sort_ascending else '📉'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin."
    if minimum_value is not None:
        op = ">=" if not sort_ascending else "<="
        desc_prefix += f" Chỉ tính user có {op} {minimum_value} {item_name_plural}."

    bar_chart_str = ""
    if show_bar_chart:
        numeric_chart_data = [(uid, val) for uid, val in filtered_sorted_users[:5] if isinstance(val, (int, float))]
        if numeric_chart_data:
            chart_user_ids = [uid for uid, _ in numeric_chart_data]
            async def format_user_key_for_legend(user_id):
                user_obj = user_cache.get(user_id)
                return escape_markdown(user_obj.display_name) if user_obj else f"ID:{user_id}"
            bar_chart_str = await create_vertical_text_bar_chart(
                sorted_data=numeric_chart_data, key_formatter=format_user_key_for_legend,
                top_n=5, max_chart_height=8, bar_width=1, bar_spacing=2,
                chart_title="Top 5", show_legend=True
            )

    description_lines = [desc_prefix]
    if bar_chart_str: description_lines.append(bar_chart_str)
    description_lines.append("")
    for rank, (user_id, count_val) in enumerate(users_to_display, 1):
        secondary_info = None; tertiary_info_final = None
        if secondary_info_getter:
            try:
                result = secondary_info_getter(user_id, counts)
                if asyncio.iscoroutine(result): secondary_info = await result
                else: secondary_info = result
            except Exception as e_sec: log.warning(f"Lỗi secondary_info_getter '{title}' {user_id}: {e_sec}")
        if tertiary_info_getter:
            try:
                result = tertiary_info_getter(user_id, counts)
                if asyncio.iscoroutine(result): tertiary_info_final = await result
                else: tertiary_info_final = result
            except Exception as e_tert: log.warning(f"Lỗi tertiary_info_getter '{title}' {user_id}: {e_tert}")
        lines = await _format_user_tree_line(rank, user_id, count_val, item_name_singular, item_name_plural, guild, user_cache, secondary_info=secondary_info, tertiary_info=tertiary_info_final)
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    final_description = "\n".join(description_lines)
    if len(final_description) > 4096:
        cutoff_point = final_description.rfind('\n', 0, 4080)
        if cutoff_point != -1: final_description = final_description[:cutoff_point] + "\n[...]"
        else: final_description = final_description[:4090] + "\n[...]"
    embed.description = final_description

    footer_text = ""; footer_add = ""
    if tertiary_info_getter:
        try:
            result = tertiary_info_getter(None, None)
            if asyncio.iscoroutine(result): footer_text = await result
            else: footer_text = result
        except Exception: pass
    if total_users_in_lb > limit: footer_add = f"... và {total_users_in_lb - limit} người dùng khác."
    final_footer = f"{footer_add} | {footer_text}" if footer_add and footer_text else (footer_add or footer_text)
    if final_footer: embed.set_footer(text=final_footer.strip(" | "))
    return embed
# --- END OF FILE utils.py ---