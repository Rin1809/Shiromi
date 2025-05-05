# --- START OF FILE reporting/embeds_user.py ---
import discord
import datetime
import math
import logging
import collections
import asyncio
import time
from typing import List, Dict, Any, Optional, Union, Tuple, Set
from discord.ext import commands
from collections import Counter, defaultdict

# Relative import
# Sử dụng import tuyệt đối
import utils
import config

log = logging.getLogger(__name__)

# --- Constants ---
# Giữ nguyên hoặc điều chỉnh LIMIT nếu cần cho layout mới
TOP_ACTIVE_USERS_LIMIT = 15
TOP_OLDEST_MEMBERS_LIMIT = 10 # Giữ giới hạn thấp hơn cho embed này vì nhiều chi tiết
TOP_LINK_USERS_LIMIT = 15
TOP_IMAGE_USERS_LIMIT = 15
TOP_EMOJI_USERS_LIMIT = 15
TOP_STICKER_USERS_LIMIT = 15
TOP_MENTIONED_USERS_LIMIT = 15
TOP_MENTIONING_USERS_LIMIT = 15
TOP_REPLIERS_LIMIT = 15
TOP_REACTION_RECEIVED_USERS_LIMIT = 15
TOP_ACTIVITY_SPAN_USERS_LIMIT = 15
TOP_THREAD_CREATORS_LIMIT = 15
TOP_DISTINCT_CHANNEL_USERS_LIMIT = 10 # Giảm vì embed này cũng nhiều chi tiết

# --- HÀM HELPER TẠO DÒNG USER CHO CÂY ---
# (Hàm này có thể dùng chung cho nhiều embed)
async def _format_user_tree_line(
    rank: int,
    user_id: int,
    main_value: Any, # Giá trị chính (số tin nhắn, link, string date, string span, etc.)
    main_unit_singular: str, # Đơn vị số ít
    main_unit_plural: str, # Đơn vị số nhiều
    guild: discord.Guild,
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]],
    secondary_info: Optional[str] = None, # Thông tin phụ (vd: Top Emoji)
    tertiary_info: Optional[str] = None # Thông tin phụ khác (vd: Last Seen)
) -> List[str]:
    """Tạo các dòng cho một user trong cây leaderboard."""
    lines = []
    podium_emojis = ["🥇", "🥈", "🥉"]
    rank_prefix = podium_emojis[rank-1] if rank <= 3 else f"#{rank:02d}"

    user_obj = user_cache.get(user_id)
    user_mention = user_obj.mention if user_obj else f"`{user_id}`"
    user_display_name = f" ({utils.escape_markdown(user_obj.display_name)})" if user_obj else " (Unknown/Left)"

    lines.append(f"{rank_prefix} {user_mention}{user_display_name}")

    # --- SỬA LỖI ValueError: ---
    # Dòng thông tin chính
    # Chỉ áp dụng định dạng ',' nếu main_value là số
    if isinstance(main_value, (int, float)):
        main_value_formatted = f"{main_value:,}"
        main_unit = main_unit_plural if main_value != 1 else main_unit_singular
    else:
        # Nếu không phải số, giữ nguyên giá trị (đã là string)
        main_value_formatted = str(main_value)
        # Đơn vị có thể không cần thiết hoặc cần logic khác nếu giá trị là string
        # Ví dụ: nếu main_value là timedelta string, đơn vị có thể bỏ qua
        # Ở đây tạm thời vẫn giữ logic đơn vị cũ, nhưng có thể cần điều chỉnh
        main_unit = main_unit_plural # Mặc định dùng số nhiều cho string

    # Bỏ đơn vị nếu giá trị đã là string được format đẹp (vd: timedelta, date)
    if isinstance(main_value, str) and ("ngày" in main_value or "giờ" in main_value or "phút" in main_value or "giây" in main_value or "/" in main_value or ":" in main_value):
        lines.append(f"  `└` **{main_value_formatted}**") # Bỏ đơn vị
    else:
        lines.append(f"  `└` **{main_value_formatted}** {main_unit}")
    # --- KẾT THÚC SỬA LỖI ---

    # Dòng thông tin phụ (nếu có)
    if secondary_info:
        lines.append(f"  `└` {secondary_info}") # Đã có emoji/định dạng từ hàm gọi

    # Dòng thông tin phụ thứ 3 (nếu có)
    if tertiary_info:
        lines.append(f"  `└` {tertiary_info}")

    # Thêm dòng trống ngăn cách
    lines.append("") # Thêm dòng trống sau mỗi user

    return lines

# --- Các hàm tạo Embed User cụ thể (Giữ nguyên phần lớn) ---

async def create_top_active_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    """Tạo embed top N user hoạt động nhiều nhất (theo số tin nhắn)."""
    e = lambda name: utils.get_emoji(name, bot)
    # Bỏ '#' ở đầu title
    title = f"{e('stats')} BXH User Gửi Tin Nhắn Nhiều Nhất"
    limit = TOP_ACTIVE_USERS_LIMIT
    filter_admins = True
    color = discord.Color.orange()
    item_name_singular="tin nhắn"
    item_name_plural="tin nhắn"

    message_counts = collections.Counter({
        uid: data['message_count']
        for uid, data in user_activity.items()
        if not data.get('is_bot', False) and data.get('message_count', 0) > 0
    })
    if not message_counts: return None

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, count) for uid, count in message_counts.most_common()
        if (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True) # Thêm lọc bot ở đây
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin*"
    description_lines = [desc_prefix, ""] # Thêm dòng trống

    for rank, (user_id, count) in enumerate(users_to_display, 1):
        # Lấy thông tin phụ: Last Seen
        user_act_data = user_activity.get(user_id)
        last_seen_str = ""
        if user_act_data:
            last_seen = user_act_data.get('last_seen')
            last_seen_str = f"• Lần cuối hoạt động: {utils.format_discord_time(last_seen, 'R')}" if last_seen else ""

        lines = await _format_user_tree_line(
            rank, user_id, count, item_name_singular, item_name_plural,
            guild, user_cache,
            secondary_info=last_seen_str if last_seen_str else None # Chỉ thêm nếu có
        )
        description_lines.extend(lines)

    # Xóa dòng trống cuối cùng nếu có
    if description_lines and description_lines[-1] == "":
        description_lines.pop()

    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"

    if total_users_in_lb > limit:
        embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")

    return embed

async def create_top_link_posters_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Bỏ '#' ở đầu title
    title = f"{e('link')} BXH User Gửi Nhiều Link Nhất"
    limit = TOP_LINK_USERS_LIMIT
    filter_admins = True
    color = discord.Color.dark_blue()
    item_name_singular="link"
    item_name_plural="links"

    if not counts: return None
    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, count) for uid, count in counts.most_common()
        if count > 0 and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin*"
    description_lines = [desc_prefix, ""]

    for rank, (user_id, count) in enumerate(users_to_display, 1):
        lines = await _format_user_tree_line(
            rank, user_id, count, item_name_singular, item_name_plural, guild, user_cache
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    if total_users_in_lb > limit: embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")
    return embed

async def create_top_image_posters_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Bỏ '#' ở đầu title
    title = f"{e('image')} BXH User Gửi Ảnh Nhiều Nhất"
    limit = TOP_IMAGE_USERS_LIMIT
    filter_admins = True
    color = discord.Color.dark_green()
    item_name_singular="ảnh"
    item_name_plural="ảnh"

    if not counts: return None
    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, count) for uid, count in counts.most_common()
        if count > 0 and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin*"
    description_lines = [desc_prefix, ""]

    for rank, (user_id, count) in enumerate(users_to_display, 1):
        lines = await _format_user_tree_line(
            rank, user_id, count, item_name_singular, item_name_plural, guild, user_cache
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    if total_users_in_lb > limit: embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")
    return embed

async def create_top_custom_emoji_users_embed(
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    limit = TOP_EMOJI_USERS_LIMIT
    filter_admins = True
    # Bỏ '#' ở đầu title
    title = f"{e('mention')} BXH User Dùng Emoji Của Server Nhiều Nhất"
    color = discord.Color.dark_gold()
    item_name_singular = "emoji"
    item_name_plural = "emojis"

    user_detailed_counts: Dict[int, Counter] = scan_data.get("user_custom_emoji_content_counts", {})
    user_total_counts = collections.Counter({
        uid: sum(ecounts.values())
        for uid, ecounts in user_detailed_counts.items()
        if sum(ecounts.values()) > 0
    })
    if not user_total_counts: return None

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, total_count) for uid, total_count in user_total_counts.most_common()
        if (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)
    emoji_cache: Dict[int, discord.Emoji] = scan_data.get("server_emojis_cache", {})

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin*"
    description_lines = [desc_prefix, ""]

    for rank, (user_id, total_count) in enumerate(users_to_display, 1):
        secondary_info = None
        user_specific_counts = user_detailed_counts.get(user_id, Counter())
        if user_specific_counts:
            try:
                most_used_id, top_count = user_specific_counts.most_common(1)[0]
                emoji_obj = emoji_cache.get(most_used_id) or bot.get_emoji(most_used_id)
                if emoji_obj: secondary_info = f"• Top: {str(emoji_obj)} ({top_count:,})"
                else: secondary_info = f"• Top ID: `{most_used_id}` ({top_count:,})"
            except (ValueError, IndexError): pass
            except Exception as e_find: log.warning(f"Lỗi tìm top emoji cho user {user_id}: {e_find}")

        lines = await _format_user_tree_line(
            rank, user_id, total_count, item_name_singular, item_name_plural,
            guild, user_cache, secondary_info=secondary_info
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    if total_users_in_lb > limit: embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")
    return embed

async def create_top_sticker_users_embed(
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    limit = TOP_STICKER_USERS_LIMIT
    filter_admins = True
    # Bỏ '#' ở đầu title
    title = f"{e('sticker')} BXH User Gửi Sticker Nhiều Nhất"
    color = discord.Color.dark_purple()
    item_name_singular = "sticker"
    item_name_plural = "stickers"

    user_detailed_counts: Dict[int, Counter] = scan_data.get("user_sticker_id_counts", {})
    user_total_counts: Counter = scan_data.get("user_sticker_counts", Counter())
    if not user_total_counts: return None

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, total_count) for uid, total_count in user_total_counts.most_common()
        if total_count > 0 and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    # Fetch sticker names
    sticker_ids_to_fetch_names = set()
    for user_id, _ in users_to_display:
        user_specific_counts = user_detailed_counts.get(user_id, Counter())
        if user_specific_counts:
            try:
                most_used_id_str, _ = user_specific_counts.most_common(1)[0]
                if most_used_id_str.isdigit(): sticker_ids_to_fetch_names.add(int(most_used_id_str))
            except (ValueError, IndexError): pass
    sticker_name_cache: Dict[int, str] = {}
    if sticker_ids_to_fetch_names:
        sticker_name_cache = await utils._fetch_sticker_dict(list(sticker_ids_to_fetch_names), bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin*"
    description_lines = [desc_prefix, ""]

    for rank, (user_id, total_count) in enumerate(users_to_display, 1):
        secondary_info = None
        user_specific_counts_display = user_detailed_counts.get(user_id, Counter())
        if user_specific_counts_display:
            try:
                most_used_id_str_display, top_count = user_specific_counts_display.most_common(1)[0]
                if most_used_id_str_display.isdigit():
                    sticker_id = int(most_used_id_str_display)
                    sticker_name = sticker_name_cache.get(sticker_id, "...")
                    secondary_info = f"• Top: '{utils.escape_markdown(sticker_name)}' ({top_count:,})"
                else:
                    secondary_info = f"• Top ID: `{most_used_id_str_display}` ({top_count:,})"
            except (ValueError, IndexError): pass
            except Exception as e_find: log.warning(f"Lỗi tìm top sticker cho user {user_id}: {e_find}")

        lines = await _format_user_tree_line(
            rank, user_id, total_count, item_name_singular, item_name_plural,
            guild, user_cache, secondary_info=secondary_info
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    if total_users_in_lb > limit: embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")
    return embed

async def create_top_mentioned_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Bỏ '#' ở đầu title
    title = f"{e('mention')} BXH User Được Nhắc Tên Nhiều Nhất"
    limit = TOP_MENTIONED_USERS_LIMIT
    filter_admins = False # Không lọc admin ở BXH này
    color = discord.Color.purple()
    item_name_singular="lần"
    item_name_plural="lần"

    if not counts: return None

    filtered_sorted_users = [
        (uid, count) for uid, count in counts.most_common()
        if count > 0 and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot.*" # Không lọc admin
    description_lines = [desc_prefix, ""]

    for rank, (user_id, count) in enumerate(users_to_display, 1):
        lines = await _format_user_tree_line(
            rank, user_id, count, item_name_singular, item_name_plural, guild, user_cache
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    if total_users_in_lb > limit: embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")
    return embed

async def create_top_mentioning_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Bỏ '#' ở đầu title
    title = f"{e('mention')} Top User Hay Nhắc Tên Người Khác Nhất"
    limit = TOP_MENTIONING_USERS_LIMIT
    filter_admins = True
    color = discord.Color.dark_purple()
    item_name_singular="lần nhắc"
    item_name_plural="lần nhắc"

    if not counts: return None
    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, count) for uid, count in counts.most_common()
        if count > 0 and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin*"
    description_lines = [desc_prefix, ""]

    for rank, (user_id, count) in enumerate(users_to_display, 1):
        lines = await _format_user_tree_line(
            rank, user_id, count, item_name_singular, item_name_plural, guild, user_cache
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    if total_users_in_lb > limit: embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")
    return embed

async def create_top_repliers_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Bỏ '#' ở đầu title
    title = f"{e('reply')} BXH User Hay Trả Lời Tin Nhắn Nhất"
    limit = TOP_REPLIERS_LIMIT
    filter_admins = True
    color = discord.Color.blue()
    item_name_singular="lần trả lời"
    item_name_plural="lần trả lời"

    if not counts: return None
    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, count) for uid, count in counts.most_common()
        if count > 0 and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin*"
    description_lines = [desc_prefix, ""]

    for rank, (user_id, count) in enumerate(users_to_display, 1):
        lines = await _format_user_tree_line(
            rank, user_id, count, item_name_singular, item_name_plural, guild, user_cache
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    if total_users_in_lb > limit: embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")
    return embed

async def create_top_reaction_received_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Bỏ '#' ở đầu title
    title = f"{e('reaction')} BXH User Nhận Reactions Nhiều Nhất"
    limit = TOP_REACTION_RECEIVED_USERS_LIMIT
    filter_admins = False # Không lọc admin
    color = discord.Color.gold()
    item_name_singular="reaction"
    item_name_plural="reactions"
    footer_note="Emoji được thả trên tin nhắn."

    if not counts: return None

    filtered_sorted_users = [
        (uid, count) for uid, count in counts.most_common()
        if count > 0 and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot.*"
    description_lines = [desc_prefix, ""]

    for rank, (user_id, count) in enumerate(users_to_display, 1):
        lines = await _format_user_tree_line(
            rank, user_id, count, item_name_singular, item_name_plural, guild, user_cache
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"

    footer_text = footer_note
    if total_users_in_lb > limit:
        footer_text = f"... và {total_users_in_lb - limit} người dùng khác. | {footer_note}"
    embed.set_footer(text=footer_text)

    return embed

async def create_top_distinct_channel_users_embed(
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    """Embed top user hoạt động trong nhiều kênh/luồng khác nhau nhất, hiển thị top kênh/luồng."""
    e = lambda name: utils.get_emoji(name, bot)
    limit = TOP_DISTINCT_CHANNEL_USERS_LIMIT
    filter_admins = True
    # Bỏ '#' ở đầu title
    title = f"🗺️ BXH {limit} \"Người Đa Năng\" Nhất"
    color=discord.Color.dark_teal()
    item_name_singular="kênh/luồng"
    item_name_plural="kênh/luồng"

    user_distinct_counts: Counter = scan_data.get("user_distinct_channel_counts", Counter())
    user_channel_msg_counts: Dict[int, Dict[int, int]] = scan_data.get('user_channel_message_counts', {})
    if not user_distinct_counts: return None

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, distinct_count) for uid, distinct_count in user_distinct_counts.most_common()
        if distinct_count > 0 and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Hoạt động trong nhiều kênh/chủ đề khác nhau nhất. Không tính bot."
    if filter_admins: desc_prefix += " Không tính admin*"
    description_lines = [desc_prefix, ""]

    for rank, (user_id, distinct_count) in enumerate(users_to_display, 1):
        # Lấy top kênh
        secondary_info = None
        user_specific_channel_counts = user_channel_msg_counts.get(user_id, {})
        if user_specific_channel_counts:
            sorted_channels = sorted(user_specific_channel_counts.items(), key=lambda item: item[1], reverse=True)[:2] # Lấy top 2
            if sorted_channels:
                channel_details_line = []
                for loc_id, msg_count in sorted_channels:
                    channel_obj = guild.get_channel_or_thread(loc_id)
                    channel_mention = channel_obj.mention if channel_obj else f"`ID:{loc_id}`"
                    channel_type_emoji = utils.get_channel_type_emoji(channel_obj, bot) if channel_obj else "❓"
                    channel_details_line.append(f"{channel_type_emoji}{channel_mention}({msg_count:,})") # Bỏ dấu cách
                secondary_info = f"• Top: {', '.join(channel_details_line)}"

        lines = await _format_user_tree_line(
            rank, user_id, distinct_count, item_name_singular, item_name_plural,
            guild, user_cache, secondary_info=secondary_info
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"

    footer_text="Top kênh/luồng hiển thị dựa trên số tin nhắn."
    if total_users_in_lb > limit:
        footer_text = f"... và {total_users_in_lb - limit} người dùng khác. | {footer_text}"
    embed.set_footer(text=footer_text)
    return embed

async def create_top_activity_span_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    limit = TOP_ACTIVITY_SPAN_USERS_LIMIT
    filter_admins = False # Thường không cần lọc admin ở đây
    # Bỏ '#' ở đầu title
    title = f"{e('clock')} BXH User Hoạt Động Lâu Nhất Server"
    color=discord.Color.dark_grey()
    item_name_singular="span" # Đơn vị sẽ được format bởi timedelta
    item_name_plural="span"

    user_spans: List[Tuple[int, datetime.timedelta]] = []
    for user_id, data in user_activity.items():
        if data.get('is_bot', False): continue
        span_seconds = data.get('activity_span_seconds', 0.0)
        if span_seconds > 0:
             user_spans.append((user_id, datetime.timedelta(seconds=span_seconds)))
    if not user_spans: return None
    user_spans.sort(key=lambda item: item[1], reverse=True)

    filtered_sorted_users = [
        (uid, span) for uid, span in user_spans
        if (not filter_admins or not isinstance(uid, int)) # Lọc admin nếu cần (hiện đang False)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, span in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji}{title}", color=color)
    desc_prefix = "*Dựa trên khoảng TG giữa tin nhắn đầu và cuối trong lần quét. Đã lọc bot.*"
    if filter_admins: desc_prefix += " Đã lọc admin*"
    description_lines = [desc_prefix, ""]

    for rank, (user_id, span) in enumerate(users_to_display, 1):
        span_str = utils.format_timedelta(span) # Format timedelta đẹp
        # Lấy thông tin phụ: Last Seen
        user_act_data = user_activity.get(user_id)
        last_seen_str = ""
        if user_act_data:
            last_seen = user_act_data.get('last_seen')
            last_seen_str = f"• Seen: {utils.format_discord_time(last_seen, 'R')}" if last_seen else ""

        lines = await _format_user_tree_line(
            rank, user_id, span_str, item_name_singular, item_name_plural,
            guild, user_cache, secondary_info=last_seen_str if last_seen_str else None
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    if total_users_in_lb > limit: embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")
    return embed

async def create_top_thread_creators_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Bỏ '#' ở đầu title
    title = f"{e('thread')} Top User Tạo Thread"
    limit = TOP_THREAD_CREATORS_LIMIT
    filter_admins = True
    color = discord.Color.dark_magenta()
    item_name_singular="thread"
    item_name_plural="threads"
    footer_note="Yêu cầu quyền View Audit Log và theo dõi thread_create."

    if not counts: return None
    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, count) for uid, count in counts.most_common()
        if count > 0 and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin*"
    description_lines = [desc_prefix, ""]

    for rank, (user_id, count) in enumerate(users_to_display, 1):
        lines = await _format_user_tree_line(
            rank, user_id, count, item_name_singular, item_name_plural, guild, user_cache
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"

    footer_text = footer_note
    if total_users_in_lb > limit:
        footer_text = f"... và {total_users_in_lb - limit} người dùng khác. | {footer_note}"
    embed.set_footer(text=footer_text)

    return embed

async def create_top_booster_embed(
    boosters: List[discord.Member],
    bot: discord.Client,
    scan_end_time: datetime.datetime
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    limit = 15
    # Bỏ '#' ở đầu title
    title = f"{e('boost')} Top Booster Bền Bỉ"
    color=discord.Color(0xf47fff)
    item_name_singular="boost duration"
    item_name_plural="boost duration"

    if not boosters: return None

    user_cache = {m.id: m for m in boosters}
    users_to_display = boosters[:limit]
    total_users_in_lb = len(boosters)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Sắp xếp theo thời gian boost server lâu nhất.*"
    description_lines = [desc_prefix, ""]

    for rank, member in enumerate(users_to_display, 1):
        user_id = member.id
        boost_duration_str = "N/A"
        boost_start_str = ""
        if member.premium_since:
            boost_start_str = f"• Boost từ: {utils.format_discord_time(member.premium_since, 'D')}"
            try:
                scan_end_time_aware = scan_end_time if scan_end_time.tzinfo else scan_end_time.replace(tzinfo=datetime.timezone.utc)
                premium_since_aware = member.premium_since if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc)
                if scan_end_time_aware >= premium_since_aware:
                    boost_duration = scan_end_time_aware - premium_since_aware
                    boost_duration_str = utils.format_timedelta(boost_duration)
                else: boost_duration_str = "Lỗi TG"
            except Exception as td_err: log.warning(f"Lỗi tính time boost {user_id}: {td_err}"); boost_duration_str = "Lỗi TG"

        lines = await _format_user_tree_line(
            rank, user_id, boost_duration_str, item_name_singular, item_name_plural,
            member.guild, user_cache, secondary_info=boost_start_str if boost_start_str else None
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    if total_users_in_lb > limit: embed.set_footer(text=f"... và {total_users_in_lb - limit} booster khác.")
    return embed

async def create_top_oldest_members_embed(
    oldest_members_data: List[Dict[str, Any]],
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot],
    limit: int = TOP_OLDEST_MEMBERS_LIMIT
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Bỏ '#' ở đầu title
    title = f"{e('calendar')} BXH Thành Viên Già Nhất Server"
    color=discord.Color.dark_grey()
    item_name_singular="member"
    item_name_plural="members"

    if not oldest_members_data: return None

    user_activity = scan_data.get("user_activity", {})
    user_most_active_channel = scan_data.get("user_most_active_channel", {})
    users_to_display = oldest_members_data[:limit]
    user_ids_to_fetch = [data['id'] for data in users_to_display if 'id' in data]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = f"*Dựa trên ngày tham gia server. Hiển thị top {limit}.*"
    description_lines = [desc_prefix, ""]

    for rank, data in enumerate(users_to_display, 1):
        user_id = data.get('id')
        if not user_id: continue

        # Thông tin chính: Ngày tham gia
        joined_at = data.get('joined_at')
        main_value_str = utils.format_discord_time(joined_at, 'D') if joined_at else "N/A"

        # Thông tin phụ 1: Thời gian trong server
        time_in_server_str = ""
        if isinstance(joined_at, datetime.datetime):
            try:
                join_aware = joined_at.astimezone(datetime.timezone.utc) if joined_at.tzinfo else joined_at.replace(tzinfo=datetime.timezone.utc)
                if now_utc >= join_aware: time_in_server_str = f"• TG: {utils.format_timedelta(now_utc - join_aware)}"
            except Exception: pass

        # Thông tin phụ 2: Last seen và kênh hay ở
        tertiary_info_parts = []
        user_act_data = user_activity.get(user_id)
        if user_act_data:
            last_seen = user_act_data.get('last_seen')
            if last_seen: tertiary_info_parts.append(f"Seen: {utils.format_discord_time(last_seen, 'R')}")
            most_active_data = user_most_active_channel.get(user_id)
            if most_active_data:
                loc_id, _ = most_active_data
                channel_obj = guild.get_channel_or_thread(loc_id)
                if channel_obj: tertiary_info_parts.append(f"Top kênh: {channel_obj.mention}")
        tertiary_info = " • ".join(tertiary_info_parts) if tertiary_info_parts else None

        lines = await _format_user_tree_line(
            rank, user_id, main_value_str, item_name_singular, item_name_plural,
            guild, user_cache,
            secondary_info=time_in_server_str if time_in_server_str else None,
            tertiary_info=tertiary_info
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    # Không cần footer ... và ... nữa vì limit đã ghi trong title/desc
    return embed

# --- START HELPER FUNCTION (Giữ nguyên) ---
async def _fetch_sample_message(
    guild: discord.Guild,
    user_id: int,
    timestamp: Optional[datetime.datetime],
    channels_to_check: Set[int],
    most_active_channel_id: Optional[int],
    fetch_mode: str = 'around' # 'around', 'before', 'after', 'latest', 'oldest'
) -> Optional[discord.Message]:
    """Helper để fetch một tin nhắn mẫu."""
    if not guild: return None
    if not timestamp and fetch_mode not in ['latest', 'oldest']:
        return None

    channel_ids_ordered = []
    if most_active_channel_id and most_active_channel_id in channels_to_check:
        channel_ids_ordered.append(most_active_channel_id)
    other_channels = list(channels_to_check - {most_active_channel_id})
    channel_ids_ordered.extend(other_channels[:5])

    if not channel_ids_ordered:
        return None

    for channel_id in channel_ids_ordered:
        channel = guild.get_channel(channel_id)
        if not channel or not isinstance(channel, (discord.TextChannel, discord.VoiceChannel, discord.Thread)):
            continue
        # <<< SỬA LỖI TIỀM NĂNG: Kiểm tra xem channel có bị xóa không >>>
        if channel.is_deleted():
             log.debug(f"Skipping fetch in channel {channel_id} for user {user_id}: Channel is deleted.")
             continue
        # <<< KẾT THÚC SỬA LỖI >>>
        if not channel.permissions_for(guild.me).read_message_history:
            log.debug(f"Skipping fetch in channel {channel_id} for user {user_id}: Missing Read History perms.")
            continue

        try:
            history_params = {'limit': 1}
            if fetch_mode == 'around' and timestamp: history_params['around'] = timestamp
            elif fetch_mode == 'before' and timestamp: history_params['before'] = timestamp
            elif fetch_mode == 'after' and timestamp: history_params['after'] = timestamp
            elif fetch_mode == 'latest': pass
            elif fetch_mode == 'oldest': history_params['oldest_first'] = True

            async for msg in channel.history(**history_params):
                if msg.author.id == user_id:
                    log.debug(f"Fetched sample message {msg.id} for user {user_id} (mode: {fetch_mode}) in channel {channel_id}")
                    return msg
        except discord.NotFound:
            log.debug(f"Channel {channel_id} not found during sample message fetch.")
        except discord.Forbidden:
            log.debug(f"Forbidden to fetch history in channel {channel_id} for user {user_id}.")
        except discord.HTTPException as http_err:
            log.warning(f"HTTP Error {http_err.status} fetching history in {channel_id} for user {user_id}.")
        except Exception as e:
            log.error(f"Unknown error fetching sample message in {channel_id} for user {user_id}: {e}", exc_info=False)

    log.debug(f"Could not find sample message for user {user_id} (mode: {fetch_mode}) after checking {len(channel_ids_ordered)} channels.")
    return None
# --- END HELPER FUNCTION ---

# --- END OF FILE reporting/embeds_user.py ---