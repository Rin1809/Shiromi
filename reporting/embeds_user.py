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
import unicodedata # <<< THÊM IMPORT NÀY Ở ĐẦU FILE

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
TOP_REACTION_GIVERS_LIMIT = 15 # Giữ nguyên LIMIT mới
TOP_ACTIVITY_SPAN_USERS_LIMIT = 15
TOP_THREAD_CREATORS_LIMIT = 15
TOP_DISTINCT_CHANNEL_USERS_LIMIT = 10 # Giảm vì embed này cũng nhiều chi tiết

# Thêm LIMIT cho BXH "Ít Nhất" (có thể dùng chung hoặc riêng)
LEAST_ACTIVE_USERS_LIMIT = 10
LEAST_LINK_USERS_LIMIT = 10
LEAST_IMAGE_USERS_LIMIT = 10
LEAST_EMOJI_USERS_LIMIT = 10
LEAST_STICKER_USERS_LIMIT = 10
LEAST_MENTIONED_USERS_LIMIT = 10
LEAST_MENTIONING_USERS_LIMIT = 10
LEAST_REPLIERS_LIMIT = 10
LEAST_REACTION_RECEIVED_USERS_LIMIT = 10
LEAST_REACTION_GIVERS_LIMIT = 10
LEAST_ACTIVITY_SPAN_USERS_LIMIT = 10
LEAST_THREAD_CREATORS_LIMIT = 10
LEAST_DISTINCT_CHANNEL_USERS_LIMIT = 10


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
    podium_emojis = ["🥇", "🥈", "🥉"] # Podium vẫn giữ cho top list
    # Sử dụng rank number cho list "ít nhất" hoặc rank > 3
    rank_prefix = f"`#{rank:02d}`" # Mặc định dùng rank number

    user_obj = user_cache.get(user_id)
    user_mention = user_obj.mention if user_obj else f"`{user_id}`"
    user_display_name = f" ({utils.escape_markdown(user_obj.display_name)})" if user_obj else " (Unknown/Left)"

    lines.append(f"{rank_prefix} {user_mention}{user_display_name}")

    # Dòng thông tin chính
    if isinstance(main_value, (int, float)):
        main_value_formatted = f"{main_value:,}"
        # Logic đơn vị cần xem xét kỹ hơn cho list "ít nhất"
        # Nếu giá trị là 1, dùng số ít, còn lại dùng số nhiều (giữ nguyên)
        main_unit = main_unit_plural if main_value != 1 else main_unit_singular
    else:
        main_value_formatted = str(main_value)
        main_unit = main_unit_plural # Mặc định dùng số nhiều cho string

    # Bỏ đơn vị nếu giá trị đã là string được format đẹp
    if isinstance(main_value, str) and ("ngày" in main_value or "giờ" in main_value or "phút" in main_value or "giây" in main_value or "/" in main_value or ":" in main_value):
        lines.append(f"  `└` **{main_value_formatted}**") # Bỏ đơn vị
    else:
        # Xử lý trường hợp giá trị chính là 0 (cho BXH ít nhất)
        if isinstance(main_value, (int, float)) and main_value == 0:
             lines.append(f"  `└` **{main_value_formatted}** {main_unit}")
        # Xử lý các trường hợp khác (bao gồm số > 0 và string không phải thời gian)
        elif main_value_formatted: # Chỉ thêm dòng nếu giá trị không rỗng
             lines.append(f"  `└` **{main_value_formatted}** {main_unit}")
        # Bỏ qua nếu giá trị rỗng (không nên xảy ra nhưng đề phòng)


    # Dòng thông tin phụ (nếu có)
    if secondary_info:
        lines.append(f"  `└` {secondary_info}")

    # Dòng thông tin phụ thứ 3 (nếu có)
    if tertiary_info:
        lines.append(f"  `└` {tertiary_info}")

    # Thêm dòng trống ngăn cách
    lines.append("")

    return lines

# --- HÀM HELPER CHUNG CHO TẠO BXH USER ---
# --- HÀM HELPER CHUNG CHO TẠO BXH USER ---
async def _create_user_leaderboard_embed(
    title: str,
    counts: Optional[Union[collections.Counter, Dict[int, Any]]],
    value_key: Optional[str], # Key để lấy giá trị từ dict (nếu counts là dict)
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot],
    limit: int,
    item_name_singular: str,
    item_name_plural: str,
    e: callable, # Hàm lambda lấy emoji
    color: discord.Color,
    filter_admins: bool,
    sort_ascending: bool = False, # Thêm cờ sắp xếp tăng dần
    secondary_info_getter: Optional[callable] = None, # Hàm lấy thông tin phụ
    tertiary_info_getter: Optional[callable] = None, # Thêm getter thứ 3
    minimum_value: Optional[Union[int, float]] = None # Thêm minimum_value
) -> Optional[discord.Embed]:
    """Hàm helper chung để tạo embed leaderboard dạng cây cho user."""

    if not counts:
        log.debug(f"Bỏ qua tạo embed '{title}': Không có dữ liệu counts.")
        return None

    # --- Chuẩn bị dữ liệu và lọc ---
    processed_counts = collections.Counter()
    # Chỉ xử lý Counter hoặc Dict với value_key
    if isinstance(counts, collections.Counter):
        # Lọc giá trị tối thiểu nếu có
        if minimum_value is not None:
            processed_counts = collections.Counter({
                uid: count for uid, count in counts.items()
                if isinstance(count, (int, float)) and count >= minimum_value
            })
        else:
            processed_counts = counts.copy() # Chỉ copy nếu không lọc min
    elif isinstance(counts, dict) and value_key:
         processed_counts = collections.Counter({
             uid: data.get(value_key, 0)
             for uid, data in counts.items()
             if not data.get('is_bot', False) # Lọc bot ở đây
                and isinstance(data.get(value_key), (int, float))
                and (minimum_value is None or data.get(value_key, 0) >= minimum_value) # Lọc min
         })
    else:
        log.warning(f"Dữ liệu không hợp lệ cho embed '{title}'.")
        return None

    if not processed_counts:
        log.debug(f"Bỏ qua tạo embed '{title}': Không có dữ liệu sau khi xử lý/lọc bot/giá trị min.")
        return None

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    # Lọc admin và sắp xếp
    filtered_users = [
        (uid, count) for uid, count in processed_counts.items()
        if (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           # Không cần kiểm tra bot nữa vì đã lọc ở trên
    ]

    # Sắp xếp dựa trên cờ sort_ascending
    filtered_sorted_users = sorted(filtered_users, key=lambda item: item[1], reverse=not sort_ascending)

    if not filtered_sorted_users:
        log.debug(f"Bỏ qua tạo embed '{title}': Không có user hợp lệ sau khi lọc.")
        return None

    # --- Tạo Embed ---
    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' and not sort_ascending else '📉' # Emoji khác cho list "ít nhất"
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin*"
    if minimum_value is not None: desc_prefix += f" Chỉ tính user có >= {minimum_value} {item_name_plural}."
    description_lines = [desc_prefix, ""]

    for rank, (user_id, count_val) in enumerate(users_to_display, 1):
        # --- SỬA LỖI AWAIT ---
        secondary_info = None
        if secondary_info_getter:
            try:
                # Luôn await nếu getter được cung cấp (giả định nó là async)
                secondary_info = await secondary_info_getter(user_id, counts)
            except Exception as e_sec:
                log.warning(f"Lỗi khi gọi secondary_info_getter cho user {user_id} trong '{title}': {e_sec}")
                secondary_info = None

        tertiary_info_final = None
        if tertiary_info_getter:
            try:
                # Xử lý tương tự cho tertiary_info_getter
                if asyncio.iscoroutinefunction(tertiary_info_getter):
                    tertiary_info_final = await tertiary_info_getter(user_id, counts)
                elif callable(tertiary_info_getter): # Nếu là hàm thường hoặc lambda
                     tertiary_info_final = tertiary_info_getter(user_id, counts)
                # Bỏ qua nếu không phải callable
            except Exception as e_tert:
                log.warning(f"Lỗi khi gọi tertiary_info_getter cho user {user_id} trong '{title}': {e_tert}")
                tertiary_info_final = None
        # --- KẾT THÚC SỬA LỖI AWAIT ---

        lines = await _format_user_tree_line(
            rank, user_id, count_val, item_name_singular, item_name_plural,
            guild, user_cache, secondary_info=secondary_info, tertiary_info=tertiary_info_final
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    if total_users_in_lb > limit: embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")

    return embed


# --- CÁC HÀM TẠO EMBED "NHIỀU NHẤT" (DÙNG HELPER MỚI) ---

async def create_top_active_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Hàm lấy thông tin phụ: Last Seen
    async def get_last_seen(user_id, data_source):
        user_act_data = data_source.get(user_id)
        if user_act_data:
            last_seen = user_act_data.get('last_seen')
            return f"• Lần cuối HĐ: {utils.format_discord_time(last_seen, 'R')}" if last_seen else None
        return None

    return await _create_user_leaderboard_embed(
        title=f"{e('stats')} BXH User Gửi Tin Nhắn Nhiều Nhất",
        counts=user_activity,
        value_key='message_count', # Lấy giá trị từ 'message_count' trong dict user_activity
        guild=guild,
        bot=bot,
        limit=TOP_ACTIVE_USERS_LIMIT,
        item_name_singular="tin nhắn",
        item_name_plural="tin nhắn",
        e=e,
        color=discord.Color.orange(),
        filter_admins=True,
        secondary_info_getter=get_last_seen,
        minimum_value=1 # Cần ít nhất 1 tin
    )

async def create_top_link_posters_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('link')} BXH User Gửi Nhiều Link Nhất",
        counts=counts,
        value_key=None, # Counter không cần value_key
        guild=guild,
        bot=bot,
        limit=TOP_LINK_USERS_LIMIT,
        item_name_singular="link",
        item_name_plural="links",
        e=e,
        color=discord.Color.dark_blue(),
        filter_admins=True,
        minimum_value=1
    )

async def create_top_image_posters_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('image')} BXH User Gửi Ảnh Nhiều Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=TOP_IMAGE_USERS_LIMIT,
        item_name_singular="ảnh",
        item_name_plural="ảnh",
        e=e,
        color=discord.Color.dark_green(),
        filter_admins=True,
        minimum_value=1
    )

async def create_top_custom_emoji_users_embed(
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    user_detailed_counts: Dict[int, Counter] = scan_data.get("user_custom_emoji_content_counts", {})
    user_total_counts = collections.Counter({
        uid: sum(ecounts.values())
        for uid, ecounts in user_detailed_counts.items()
        if sum(ecounts.values()) > 0
    })
    emoji_cache: Dict[int, discord.Emoji] = scan_data.get("server_emojis_cache", {})

    async def get_top_emoji(user_id, data_source):
        user_specific_counts = data_source.get(user_id, Counter())
        if user_specific_counts:
            try:
                most_used_id, top_count = user_specific_counts.most_common(1)[0]
                emoji_obj = emoji_cache.get(most_used_id) or bot.get_emoji(most_used_id)
                if emoji_obj: return f"• Top: {str(emoji_obj)} ({top_count:,})"
                else: return f"• Top ID: `{most_used_id}` ({top_count:,})"
            except (ValueError, IndexError): pass
        return None

    return await _create_user_leaderboard_embed(
        title=f"{e('mention')} BXH User Dùng Emoji Của Server Nhiều Nhất",
        counts=user_total_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=TOP_EMOJI_USERS_LIMIT,
        item_name_singular="emoji",
        item_name_plural="emojis",
        e=e,
        color=discord.Color.dark_gold(),
        filter_admins=True,
        secondary_info_getter=lambda uid, _: get_top_emoji(uid, user_detailed_counts), # Truyền dict chi tiết
        minimum_value=1
    )

async def create_top_sticker_users_embed(
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    user_detailed_counts: Dict[int, Counter] = scan_data.get("user_sticker_id_counts", {})
    user_total_counts: Counter = scan_data.get("user_sticker_counts", Counter())
    sticker_ids_to_fetch_names = set()
    # Lấy sticker ID từ tất cả user trong counter để fetch tên 1 lần
    for user_id, _ in user_total_counts.items(): # Chỉ lấy top N để fetch?
         user_specific_counts = user_detailed_counts.get(user_id, Counter())
         if user_specific_counts:
             try:
                 # Chỉ lấy top 1 sticker của user để fetch tên
                 if user_specific_counts:
                     most_used_id_str, _ = user_specific_counts.most_common(1)[0]
                     if most_used_id_str.isdigit(): sticker_ids_to_fetch_names.add(int(most_used_id_str))
             except (ValueError, IndexError): pass
    sticker_name_cache: Dict[int, str] = {}
    if sticker_ids_to_fetch_names:
        sticker_name_cache = await utils._fetch_sticker_dict(list(sticker_ids_to_fetch_names), bot)

    async def get_top_sticker(user_id, data_source):
        user_specific_counts_display = data_source.get(user_id, Counter())
        if user_specific_counts_display:
            try:
                most_used_id_str_display, top_count = user_specific_counts_display.most_common(1)[0]
                if most_used_id_str_display.isdigit():
                    sticker_id = int(most_used_id_str_display)
                    sticker_name = sticker_name_cache.get(sticker_id, "...")
                    return f"• Top: '{utils.escape_markdown(sticker_name)}' ({top_count:,})"
                else:
                    return f"• Top ID: `{most_used_id_str_display}` ({top_count:,})"
            except (ValueError, IndexError): pass
        return None

    return await _create_user_leaderboard_embed(
        title=f"{e('sticker')} BXH User Gửi Sticker Nhiều Nhất",
        counts=user_total_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=TOP_STICKER_USERS_LIMIT,
        item_name_singular="sticker",
        item_name_plural="stickers",
        e=e,
        color=discord.Color.dark_purple(),
        filter_admins=True,
        secondary_info_getter=lambda uid, _: get_top_sticker(uid, user_detailed_counts),
        minimum_value=1
    )

async def create_top_mentioned_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('mention')} BXH User Được Nhắc Tên Nhiều Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=TOP_MENTIONED_USERS_LIMIT,
        item_name_singular="lần",
        item_name_plural="lần",
        e=e,
        color=discord.Color.purple(),
        filter_admins=False, # Không lọc admin
        minimum_value=1
    )

async def create_top_mentioning_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('mention')} Top User Hay Nhắc Tên Người Khác Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=TOP_MENTIONING_USERS_LIMIT,
        item_name_singular="lần nhắc",
        item_name_plural="lần nhắc",
        e=e,
        color=discord.Color.dark_purple(),
        filter_admins=True,
        minimum_value=1
    )

async def create_top_repliers_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('reply')} BXH User Hay Trả Lời Tin Nhắn Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=TOP_REPLIERS_LIMIT,
        item_name_singular="lần trả lời",
        item_name_plural="lần trả lời",
        e=e,
        color=discord.Color.blue(),
        filter_admins=True,
        minimum_value=1
    )

async def create_top_reaction_received_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot],
    user_emoji_received_counts: Optional[defaultdict] = None,
    scan_data: Optional[Dict[str, Any]] = None
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    emoji_cache: Dict[int, discord.Emoji] = {}
    if scan_data:
        emoji_cache = scan_data.get("server_emojis_cache", {})

    async def get_top_received_emoji(user_id, _): # Tham số thứ 2 không dùng
        if user_emoji_received_counts:
            user_specific_counts = user_emoji_received_counts.get(user_id, Counter())
            if user_specific_counts:
                try:
                    most_received_key, top_count = user_specific_counts.most_common(1)[0]
                    if isinstance(most_received_key, int):
                        emoji_obj = emoji_cache.get(most_received_key) or bot.get_emoji(most_received_key)
                        if emoji_obj: return f"• Top Nhận: {str(emoji_obj)} ({top_count:,})"
                        else: return f"• Top Nhận ID: `{most_received_key}` ({top_count:,})"
                    elif isinstance(most_received_key, str):
                         try: unicodedata.name(most_received_key); return f"• Top Nhận: {most_received_key} ({top_count:,})"
                         except (TypeError, ValueError): return f"• Top Nhận: `{most_received_key}` ({top_count:,})"
                except (ValueError, IndexError): pass
        return None

    # <<< SỬA LỖI: Thêm tertiary_info_getter >>>
    return await _create_user_leaderboard_embed(
        title=f"{e('reaction')} BXH User Nhận Reactions Nhiều Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=TOP_REACTION_RECEIVED_USERS_LIMIT,
        item_name_singular="reaction",
        item_name_plural="reactions",
        e=e,
        color=discord.Color.gold(),
        filter_admins=False, # Không lọc admin
        secondary_info_getter=get_top_received_emoji,
        tertiary_info_getter=lambda _, __: "Chỉ tính reaction đã lọc.", # Footer
        minimum_value=1
    )
    # <<< KẾT THÚC SỬA LỖI >>>

async def create_top_distinct_channel_users_embed(
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    user_distinct_counts: Counter = scan_data.get("user_distinct_channel_counts", Counter())
    user_channel_msg_counts: Dict[int, Dict[int, int]] = scan_data.get('user_channel_message_counts', {})

    async def get_top_channels(user_id, _):
        user_specific_channel_counts = user_channel_msg_counts.get(user_id, {})
        if user_specific_channel_counts:
            sorted_channels = sorted(user_specific_channel_counts.items(), key=lambda item: item[1], reverse=True)[:2]
            if sorted_channels:
                channel_details_line = []
                for loc_id, msg_count in sorted_channels:
                    channel_obj = guild.get_channel_or_thread(loc_id)
                    channel_mention = channel_obj.mention if channel_obj else f"`ID:{loc_id}`"
                    channel_type_emoji = utils.get_channel_type_emoji(channel_obj, bot) if channel_obj else "❓"
                    channel_details_line.append(f"{channel_type_emoji}{channel_mention}({msg_count:,})")
                return f"• Top: {', '.join(channel_details_line)}"
        return None

    return await _create_user_leaderboard_embed(
        title=f"🗺️ BXH {TOP_DISTINCT_CHANNEL_USERS_LIMIT} \"Người Đa Năng\" Nhất",
        counts=user_distinct_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=TOP_DISTINCT_CHANNEL_USERS_LIMIT,
        item_name_singular="kênh/luồng",
        item_name_plural="kênh/luồng",
        e=e,
        color=discord.Color.dark_teal(),
        filter_admins=True,
        secondary_info_getter=get_top_channels,
        minimum_value=1
    )

async def create_top_activity_span_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    limit = TOP_ACTIVITY_SPAN_USERS_LIMIT # <<< SỬA: Định nghĩa limit
    title = f"{e('clock')} BXH User Hoạt Động Lâu Nhất Server" # <<< SỬA: Chuyển title xuống sau limit
    # Tạo dict với span đã format làm giá trị để hiển thị
    user_spans_formatted = {
        uid: utils.format_timedelta(datetime.timedelta(seconds=data.get('activity_span_seconds', 0.0)))
        for uid, data in user_activity.items()
        if not data.get('is_bot', False) and data.get('activity_span_seconds', 0.0) > 0
    }
    # Tạo counter với giá trị giây để sắp xếp
    counts_for_sorting = collections.Counter({
        uid: data.get('activity_span_seconds', 0.0)
        for uid, data in user_activity.items()
        if not data.get('is_bot', False) and data.get('activity_span_seconds', 0.0) > 0
    })

    async def get_last_seen_span(user_id, _):
        user_act_data = user_activity.get(user_id)
        if user_act_data:
            last_seen = user_act_data.get('last_seen')
            return f"• Seen: {utils.format_discord_time(last_seen, 'R')}" if last_seen else None
        return None

    # Helper mới cần hỗ trợ value_getter hoặc điều chỉnh _format_user_tree_line
    # => Viết lại logic embed này không dùng helper chung để hiển thị đúng
    if not counts_for_sorting: return None

    filtered_sorted_users_seconds = sorted(
        counts_for_sorting.items(),
        key=lambda item: item[1],
        reverse=True
    )
    # Lọc admin (nếu cần)
    admin_ids_to_filter: Optional[Set[int]] = None
    filter_admins = False # Đặt cờ filter_admins
    # if filter_admins: ...

    filtered_sorted_users = [
        (uid, user_spans_formatted.get(uid, "N/A")) # Lấy span đã format
        for uid, _ in filtered_sorted_users_seconds
        if uid in user_spans_formatted # Chỉ lấy user có span đã format
           and (not filter_admins or not isinstance(uid, int) or not admin_ids_to_filter or uid not in admin_ids_to_filter) # <<< SỬA: Sửa logic lọc admin
    ]

    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit] # <<< SỬA: Dùng biến limit đã định nghĩa
    user_ids_to_fetch = [uid for uid, span_str in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji}{title}", color=discord.Color.dark_grey())
    desc_prefix = "*Dựa trên khoảng TG giữa tin nhắn đầu và cuối trong lần quét. Đã lọc bot.*"
    if filter_admins: desc_prefix += " Đã lọc admin*" # Thêm ghi chú nếu lọc admin được bật
    description_lines = [desc_prefix, ""]

    for rank, (user_id, span_str) in enumerate(users_to_display, 1):
        last_seen_str = await get_last_seen_span(user_id, None)
        lines = await _format_user_tree_line(
            rank, user_id, span_str, "span", "span", # Truyền span đã format
            guild, user_cache, secondary_info=last_seen_str
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
    # <<< SỬA LỖI: Thêm tertiary_info_getter >>>
    return await _create_user_leaderboard_embed(
        title=f"{e('thread')} Top User Tạo Thread",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=TOP_THREAD_CREATORS_LIMIT,
        item_name_singular="thread",
        item_name_plural="threads",
        e=e,
        color=discord.Color.dark_magenta(),
        filter_admins=True,
        tertiary_info_getter=lambda _, __: "Yêu cầu quyền View Audit Log.", # Footer
        minimum_value=1
    )
    # <<< KẾT THÚC SỬA LỖI >>>

# Giữ nguyên các hàm create_top_booster_embed và create_top_oldest_members_embed
# (Copy lại từ code gốc nếu cần)
async def create_top_booster_embed(
    boosters: List[discord.Member],
    bot: discord.Client,
    scan_end_time: datetime.datetime
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    limit = 15
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
    title = f"{e('calendar')} BXH Thành Viên Lâu Năm Nhất Server"
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

        joined_at = data.get('joined_at')
        main_value_str = utils.format_discord_time(joined_at, 'D') if joined_at else "N/A"

        time_in_server_str = ""
        if isinstance(joined_at, datetime.datetime):
            try:
                join_aware = joined_at.astimezone(datetime.timezone.utc) if joined_at.tzinfo else joined_at.replace(tzinfo=datetime.timezone.utc)
                if now_utc >= join_aware: time_in_server_str = f"• TG: {utils.format_timedelta(now_utc - join_aware)}"
            except Exception: pass

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
    return embed


# --- CÁC HÀM TẠO EMBED "ÍT NHẤT" ---

async def create_least_active_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    async def get_last_seen(user_id, data_source): # Giống hàm trên
        user_act_data = data_source.get(user_id)
        if user_act_data:
            last_seen = user_act_data.get('last_seen')
            return f"• Lần cuối HĐ: {utils.format_discord_time(last_seen, 'R')}" if last_seen else None
        return None

    return await _create_user_leaderboard_embed(
        title=f"{e('stats')} BXH User Gửi Tin Nhắn Ít Nhất",
        counts=user_activity,
        value_key='message_count',
        guild=guild,
        bot=bot,
        limit=LEAST_ACTIVE_USERS_LIMIT,
        item_name_singular="tin nhắn",
        item_name_plural="tin nhắn",
        e=e,
        color=discord.Color.light_grey(), # Màu khác
        filter_admins=True,
        sort_ascending=True, # Sắp xếp tăng dần
        secondary_info_getter=get_last_seen,
        minimum_value=1 # Chỉ xét người có > 0 tin nhắn
    )

async def create_least_link_posters_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('link')} BXH User Gửi Link Ít Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_LINK_USERS_LIMIT,
        item_name_singular="link",
        item_name_plural="links",
        e=e,
        color=discord.Color.from_rgb(173, 216, 230), # Light blue
        filter_admins=True,
        sort_ascending=True,
        minimum_value=1
    )

async def create_least_image_posters_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('image')} BXH User Gửi Ảnh Ít Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_IMAGE_USERS_LIMIT,
        item_name_singular="ảnh",
        item_name_plural="ảnh",
        e=e,
        color=discord.Color.from_rgb(144, 238, 144), # Light green
        filter_admins=True,
        sort_ascending=True,
        minimum_value=1
    )

async def create_least_custom_emoji_users_embed(
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    user_detailed_counts: Dict[int, Counter] = scan_data.get("user_custom_emoji_content_counts", {})
    user_total_counts = collections.Counter({
        uid: sum(ecounts.values())
        for uid, ecounts in user_detailed_counts.items()
        if sum(ecounts.values()) > 0 # Chỉ xét người có dùng > 0
    })
    emoji_cache: Dict[int, discord.Emoji] = scan_data.get("server_emojis_cache", {})

    async def get_top_emoji(user_id, data_source): # Giống hàm trên
        user_specific_counts = data_source.get(user_id, Counter())
        if user_specific_counts:
            try:
                most_used_id, top_count = user_specific_counts.most_common(1)[0]
                emoji_obj = emoji_cache.get(most_used_id) or bot.get_emoji(most_used_id)
                if emoji_obj: return f"• Top: {str(emoji_obj)} ({top_count:,})"
                else: return f"• Top ID: `{most_used_id}` ({top_count:,})"
            except (ValueError, IndexError): pass
        return None

    return await _create_user_leaderboard_embed(
        title=f"{e('mention')} BXH User Dùng Emoji Server Ít Nhất",
        counts=user_total_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_EMOJI_USERS_LIMIT,
        item_name_singular="emoji",
        item_name_plural="emojis",
        e=e,
        color=discord.Color.from_rgb(255, 223, 186), # Light gold/orange
        filter_admins=True,
        sort_ascending=True,
        secondary_info_getter=lambda uid, _: get_top_emoji(uid, user_detailed_counts),
        minimum_value=1
    )

async def create_least_sticker_users_embed(
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    user_detailed_counts: Dict[int, Counter] = scan_data.get("user_sticker_id_counts", {})
    user_total_counts: Counter = scan_data.get("user_sticker_counts", Counter())
    sticker_ids_to_fetch_names = set()
    # Lấy sticker ID từ tất cả user trong counter để fetch tên 1 lần
    for user_id, _ in user_total_counts.items():
         user_specific_counts = user_detailed_counts.get(user_id, Counter())
         if user_specific_counts:
             try:
                 if user_specific_counts:
                     most_used_id_str, _ = user_specific_counts.most_common(1)[0]
                     if most_used_id_str.isdigit(): sticker_ids_to_fetch_names.add(int(most_used_id_str))
             except (ValueError, IndexError): pass
    sticker_name_cache: Dict[int, str] = {}
    if sticker_ids_to_fetch_names:
        sticker_name_cache = await utils._fetch_sticker_dict(list(sticker_ids_to_fetch_names), bot)

    async def get_top_sticker(user_id, data_source): # Giống hàm trên
        user_specific_counts_display = data_source.get(user_id, Counter())
        if user_specific_counts_display:
            try:
                most_used_id_str_display, top_count = user_specific_counts_display.most_common(1)[0]
                if most_used_id_str_display.isdigit():
                    sticker_id = int(most_used_id_str_display)
                    sticker_name = sticker_name_cache.get(sticker_id, "...")
                    return f"• Top: '{utils.escape_markdown(sticker_name)}' ({top_count:,})"
                else:
                    return f"• Top ID: `{most_used_id_str_display}` ({top_count:,})"
            except (ValueError, IndexError): pass
        return None

    return await _create_user_leaderboard_embed(
        title=f"{e('sticker')} BXH User Gửi Sticker Ít Nhất",
        counts=user_total_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_STICKER_USERS_LIMIT,
        item_name_singular="sticker",
        item_name_plural="stickers",
        e=e,
        color=discord.Color.from_rgb(221, 160, 221), # Light purple
        filter_admins=True,
        sort_ascending=True,
        secondary_info_getter=lambda uid, _: get_top_sticker(uid, user_detailed_counts),
        minimum_value=1
    )

async def create_least_mentioned_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('mention')} BXH User Được Nhắc Tên Ít Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_MENTIONED_USERS_LIMIT,
        item_name_singular="lần",
        item_name_plural="lần",
        e=e,
        color=discord.Color.from_rgb(218, 112, 214), # Orchid
        filter_admins=False, # Không lọc admin
        sort_ascending=True,
        minimum_value=1
    )

async def create_least_mentioning_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('mention')} Top User Ít Nhắc Tên Người Khác Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_MENTIONING_USERS_LIMIT,
        item_name_singular="lần nhắc",
        item_name_plural="lần nhắc",
        e=e,
        color=discord.Color.from_rgb(186, 85, 211), # Medium Orchid
        filter_admins=True,
        sort_ascending=True,
        minimum_value=1
    )

async def create_least_repliers_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('reply')} BXH User Ít Trả Lời Tin Nhắn Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_REPLIERS_LIMIT,
        item_name_singular="lần trả lời",
        item_name_plural="lần trả lời",
        e=e,
        color=discord.Color.from_rgb(100, 149, 237), # Cornflower Blue
        filter_admins=True,
        sort_ascending=True,
        minimum_value=1
    )

async def create_least_reaction_received_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('reaction')} BXH User Nhận Reactions Ít Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_REACTION_RECEIVED_USERS_LIMIT,
        item_name_singular="reaction",
        item_name_plural="reactions",
        e=e,
        color=discord.Color.from_rgb(255, 215, 0), # Gold (light variant)
        filter_admins=False, # Không lọc admin
        sort_ascending=True,
        tertiary_info_getter=lambda _, __: "Chỉ tính reaction đã lọc.", # Footer
        minimum_value=1
    )

async def create_least_reaction_givers_embed(
    user_reaction_given_counts: Counter,
    guild: discord.Guild,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed xếp hạng người dùng thả reaction ít nhất."""
    e = lambda name: utils.get_emoji(name, bot)
    return await _create_user_leaderboard_embed(
        title=f"{e('reaction')} BXH User Thả Reaction Ít Nhất",
        counts=user_reaction_given_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_REACTION_GIVERS_LIMIT,
        item_name_singular="reaction",
        item_name_plural="reactions",
        e=e,
        color=discord.Color.from_rgb(64, 224, 208), # Turquoise
        filter_admins=True,
        sort_ascending=True,
        tertiary_info_getter=lambda _, __: "Chỉ tính reaction đã lọc.", # Footer
        minimum_value=1
    )

async def create_least_distinct_channel_users_embed(
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    user_distinct_counts: Counter = scan_data.get("user_distinct_channel_counts", Counter())
    # Không cần hiển thị top kênh hoạt động ít nhất
    return await _create_user_leaderboard_embed(
        title=f"🗺️ BXH {LEAST_DISTINCT_CHANNEL_USERS_LIMIT} \"Người Ẩn Dật\" Nhất",
        counts=user_distinct_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_DISTINCT_CHANNEL_USERS_LIMIT,
        item_name_singular="kênh/luồng",
        item_name_plural="kênh/luồng",
        e=e,
        color=discord.Color.from_rgb(0, 139, 139), # Dark Cyan
        filter_admins=True,
        sort_ascending=True,
        minimum_value=1
    )

async def create_least_activity_span_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    limit = LEAST_ACTIVITY_SPAN_USERS_LIMIT # <<< Đặt limit
    title = f"{e('clock')} BXH User Hoạt Động Ngắn Nhất Server" # <<< Đặt title
    # Tạm thời viết lại logic embed này không dùng helper chung
    # vì helper chưa hỗ trợ tốt việc hiển thị giá trị đã format (timedelta)
    user_spans_seconds = {
        uid: data.get('activity_span_seconds', 0.0)
        for uid, data in user_activity.items()
        if not data.get('is_bot', False) and data.get('activity_span_seconds', 0.0) > 0 # Chỉ xét người có span > 0
    }
    if not user_spans_seconds: return None

    # Sắp xếp user theo span tăng dần
    sorted_users_by_span = sorted(user_spans_seconds.items(), key=lambda item: item[1])

    # Lọc admin nếu cần (filter_admins=False cho span)
    admin_ids_to_filter: Optional[Set[int]] = None
    filter_admins = False # Đặt cờ filter_admins
    # if filter_admins: ...

    filtered_sorted_users = [
        (uid, user_activity[uid]) # Lấy cả data để lấy last_seen và span gốc
        for uid, span_sec in sorted_users_by_span
        if (not filter_admins or not isinstance(uid, int) or not admin_ids_to_filter or uid not in admin_ids_to_filter)
    ]

    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit] # Dùng biến limit
    user_ids_to_fetch = [uid for uid, data in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = '📉' # Emoji cho list "ít nhất"
    embed = discord.Embed(title=f"{title_emoji}{title}", color=discord.Color.from_rgb(119, 136, 153))
    desc_prefix = "*Dựa trên khoảng TG giữa tin nhắn đầu và cuối. Chỉ tính user có span > 0s. Đã lọc bot.*"
    if filter_admins: desc_prefix += " Đã lọc admin*"
    description_lines = [desc_prefix, ""]

    for rank, (user_id, data) in enumerate(users_to_display, 1):
        span_sec = data.get('activity_span_seconds', 0.0)
        span_str = utils.format_timedelta(datetime.timedelta(seconds=span_sec))
        last_seen = data.get('last_seen')
        last_seen_str = f"• Seen: {utils.format_discord_time(last_seen, 'R')}" if last_seen else None

        lines = await _format_user_tree_line(
            rank, user_id, span_str, "span", "span",
            guild, user_cache, secondary_info=last_seen_str
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    if total_users_in_lb > limit: embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")
    return embed


async def create_least_thread_creators_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # <<< SỬA LỖI: Thêm tertiary_info_getter >>>
    return await _create_user_leaderboard_embed(
        title=f"{e('thread')} Top User Ít Tạo Thread Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_THREAD_CREATORS_LIMIT,
        item_name_singular="thread",
        item_name_plural="threads",
        e=e,
        color=discord.Color.from_rgb(147, 112, 219), # Medium Purple
        filter_admins=True,
        sort_ascending=True,
        tertiary_info_getter=lambda _, __: "Yêu cầu quyền View Audit Log.", # Footer
        minimum_value=1 # Chỉ tính người có tạo > 0 thread
    )
    # <<< KẾT THÚC SỬA LỖI >>>


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
        try: # Bọc try-except cho is_deleted()
            if channel.is_deleted():
                 log.debug(f"Skipping fetch in channel {channel_id} for user {user_id}: Channel is deleted.")
                 continue
        except AttributeError: # Xử lý trường hợp channel không có is_deleted (hiếm)
            log.warning(f"Channel object type {type(channel)} (ID: {channel_id}) does not have is_deleted attribute.")
            # Có thể bỏ qua hoặc tiếp tục tùy logic
            continue

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