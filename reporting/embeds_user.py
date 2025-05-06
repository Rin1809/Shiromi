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
from collections import Counter, defaultdict, OrderedDict 
import unicodedata

import utils
import config

log = logging.getLogger(__name__)

# --- Constants ---
TOP_ACTIVE_USERS_LIMIT = 15
TOP_OLDEST_MEMBERS_LIMIT = 10
TOP_LINK_USERS_LIMIT = 15
TOP_IMAGE_USERS_LIMIT = 15
TOP_EMOJI_USERS_LIMIT = 15
TOP_STICKER_USERS_LIMIT = 15
TOP_MENTIONED_USERS_LIMIT = 15
TOP_MENTIONING_USERS_LIMIT = 15
TOP_REPLIERS_LIMIT = 15
TOP_REACTION_RECEIVED_USERS_LIMIT = 15
TOP_REACTION_GIVERS_LIMIT = 15
TOP_ACTIVITY_SPAN_USERS_LIMIT = 15
TOP_THREAD_CREATORS_LIMIT = 15
TOP_DISTINCT_CHANNEL_USERS_LIMIT = 10

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


# --- HÀM HELPER TẠO DÒNG USER CHO CÂY (Giữ nguyên) ---
async def _format_user_tree_line(
    rank: int,
    user_id: int,
    main_value: Any,
    main_unit_singular: str,
    main_unit_plural: str,
    guild: discord.Guild,
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]],
    secondary_info: Optional[str] = None,
    tertiary_info: Optional[str] = None
) -> List[str]:
    """Tạo các dòng cho một user trong cây leaderboard."""
    lines = []
    rank_prefix = f"`#{rank:02d}`"
    user_obj = user_cache.get(user_id)
    user_mention = user_obj.mention if user_obj else f"`{user_id}`"
    user_display_name = f" ({utils.escape_markdown(user_obj.display_name)})" if user_obj else " (Unknown/Left)"
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

# --- HÀM HELPER CHUNG CHO TẠO BXH USER (ĐÃ DI CHUYỂN SANG utils.py) ---
# -> XÓA BỎ ĐỊNH NGHĨA HÀM _create_user_leaderboard_embed Ở ĐÂY <-

# --- CÁC HÀM TẠO EMBED "NHIỀU NHẤT" (GỌI HELPER TỪ UTILS) ---

async def create_top_active_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    async def get_last_seen(user_id, data_source):
        user_act_data = data_source.get(user_id)
        if user_act_data:
            last_seen = user_act_data.get('last_seen')
            return f"• Lần cuối HĐ: {utils.format_discord_time(last_seen, 'R')}" if last_seen else None
        return None
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('stats')} BXH User Gửi Tin Nhắn Nhiều Nhất",
        counts=user_activity,
        value_key='message_count',
        guild=guild,
        bot=bot,
        limit=TOP_ACTIVE_USERS_LIMIT,
        item_name_singular="tin nhắn",
        item_name_plural="tin nhắn",
        e=e,
        color=discord.Color.orange(),
        filter_admins=True,
        secondary_info_getter=get_last_seen,
        minimum_value=1
    )

async def create_top_link_posters_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('link')} BXH User Gửi Nhiều Link Nhất",
        counts=counts,
        value_key=None,
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
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
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

    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
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
        secondary_info_getter=lambda uid, _: get_top_emoji(uid, user_detailed_counts),
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

    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
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
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
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
        filter_admins=False,
        minimum_value=1
    )

async def create_top_mentioning_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
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
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
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

    async def get_top_received_emoji(user_id, _):
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

    def get_footer_note(*args):
        return "Chỉ tính reaction đã lọc."

    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
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
        filter_admins=False,
        secondary_info_getter=get_top_received_emoji,
        tertiary_info_getter=get_footer_note,
        minimum_value=1
    )

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

    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
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
    limit = TOP_ACTIVITY_SPAN_USERS_LIMIT
    title = f"{e('clock')} BXH User Hoạt Động Lâu Nhất Server"
    counts_for_sorting = collections.Counter({
        uid: data.get('activity_span_seconds', 0.0)
        for uid, data in user_activity.items()
        if not data.get('is_bot', False) and data.get('activity_span_seconds', 0.0) > 0
    })
    if not counts_for_sorting: return None

    async def get_last_seen_span(user_id, _):
        user_act_data = user_activity.get(user_id)
        if user_act_data:
            last_seen = user_act_data.get('last_seen')
            return f"• Seen: {utils.format_discord_time(last_seen, 'R')}" if last_seen else None
        return None

    formatted_counts = {
        uid: utils.format_timedelta(datetime.timedelta(seconds=span))
        for uid, span in counts_for_sorting.items()
    }
    sorted_users_by_seconds = sorted(counts_for_sorting.items(), key=lambda item: item[1], reverse=True)
    counts_for_helper = collections.OrderedDict([(uid, formatted_counts.get(uid, "N/A")) for uid, sec in sorted_users_by_seconds])

    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=title,
        counts=counts_for_helper,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=limit,
        item_name_singular="span",
        item_name_plural="span",
        e=e,
        color=discord.Color.dark_grey(),
        filter_admins=False,
        secondary_info_getter=get_last_seen_span,
        minimum_value=None
    )


async def create_top_thread_creators_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    def get_footer_note(*args):
        return "Yêu cầu quyền View Audit Log."
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
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
        tertiary_info_getter=get_footer_note,
        minimum_value=1
    )

# Giữ nguyên các hàm create_top_booster_embed và create_top_oldest_members_embed
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

    data_for_chart = []
    for member in users_to_display[:5]:
         boost_duration_secs = 0
         if member.premium_since:
             try:
                 scan_end_time_aware = scan_end_time if scan_end_time.tzinfo else scan_end_time.replace(tzinfo=datetime.timezone.utc)
                 premium_since_aware = member.premium_since if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc)
                 if scan_end_time_aware >= premium_since_aware: boost_duration_secs = (scan_end_time_aware - premium_since_aware).total_seconds()
             except Exception: pass
         if boost_duration_secs > 0: data_for_chart.append((member.id, boost_duration_secs))
    data_for_chart.sort(key=lambda x: x[1], reverse=True)

    bar_chart_str = ""
    if data_for_chart:
        async def format_user_key(user_id):
            user = user_cache.get(user_id)
            return utils.escape_markdown(user.display_name) if user else f"ID:{user_id}"
        def format_duration_value(seconds):
            return utils.format_timedelta(datetime.timedelta(seconds=seconds))

        bar_chart_str = await utils.create_vertical_text_bar_chart(
            sorted_data=data_for_chart,
            key_formatter=format_user_key,
            value_formatter=format_duration_value,
            top_n=5, max_chart_height=8, bar_width=1, bar_spacing=2,
            chart_title="Top 5 Booster", show_legend=True
        )

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Sắp xếp theo thời gian boost server lâu nhất.*"
    description_lines = [desc_prefix]
    if bar_chart_str: description_lines.append(bar_chart_str)
    description_lines.append("")

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

        # Gọi hàm helper định dạng cây từ utils
        lines = await utils._format_user_tree_line(
            rank, user_id, boost_duration_str, item_name_singular, item_name_plural,
            member.guild, user_cache, secondary_info=boost_start_str if boost_start_str else None
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    final_description = "\n".join(description_lines)
    if len(final_description) > 4096:
        cutoff_point = final_description.rfind('\n', 0, 4080);
        if cutoff_point != -1: final_description = final_description[:cutoff_point] + "\n[...]"
        else: final_description = final_description[:4090] + "\n[...]"
    embed.description = final_description

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

    data_for_chart = []
    for data in users_to_display[:5]:
        joined_at = data.get('joined_at'); days_in_server = 0
        if isinstance(joined_at, datetime.datetime):
            try:
                join_aware = joined_at.astimezone(datetime.timezone.utc) if joined_at.tzinfo else joined_at.replace(tzinfo=datetime.timezone.utc)
                if now_utc >= join_aware: days_in_server = (now_utc - join_aware).days
            except Exception: pass
        if days_in_server > 0 and data.get('id'): data_for_chart.append((data['id'], days_in_server))
    data_for_chart.sort(key=lambda x: x[1], reverse=True)

    bar_chart_str = ""
    if data_for_chart:
        async def format_user_key(user_id):
            user = user_cache.get(user_id)
            return utils.escape_markdown(user.display_name) if user else f"ID:{user_id}"
        def format_days_value(days):
            return f"{days} ngày"

        bar_chart_str = await utils.create_vertical_text_bar_chart(
            sorted_data=data_for_chart,
            key_formatter=format_user_key,
            value_formatter=format_days_value,
            top_n=5, max_chart_height=8, bar_width=1, bar_spacing=2,
            chart_title="Top 5 Lâu Năm (Ngày)", show_legend=True
        )

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = f"*Dựa trên ngày tham gia server. Hiển thị top {limit}.*"
    description_lines = [desc_prefix]
    if bar_chart_str: description_lines.append(bar_chart_str)
    description_lines.append("")

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

        # Gọi hàm helper định dạng cây từ utils
        lines = await utils._format_user_tree_line(
            rank, user_id, main_value_str, item_name_singular, item_name_plural,
            guild, user_cache,
            secondary_info=time_in_server_str if time_in_server_str else None,
            tertiary_info=tertiary_info
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    final_description = "\n".join(description_lines)
    if len(final_description) > 4096:
        cutoff_point = final_description.rfind('\n', 0, 4080);
        if cutoff_point != -1: final_description = final_description[:cutoff_point] + "\n[...]"
        else: final_description = final_description[:4090] + "\n[...]"
    embed.description = final_description

    return embed

# --- CÁC HÀM TẠO EMBED "ÍT NHẤT" ---

async def create_least_active_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    async def get_last_seen(user_id, data_source):
        user_act_data = data_source.get(user_id)
        if user_act_data:
            last_seen = user_act_data.get('last_seen')
            return f"• Lần cuối HĐ: {utils.format_discord_time(last_seen, 'R')}" if last_seen else None
        return None
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('stats')} BXH User Gửi Tin Nhắn Ít Nhất",
        counts=user_activity,
        value_key='message_count',
        guild=guild,
        bot=bot,
        limit=LEAST_ACTIVE_USERS_LIMIT,
        item_name_singular="tin nhắn",
        item_name_plural="tin nhắn",
        e=e,
        color=discord.Color.light_grey(),
        filter_admins=True,
        sort_ascending=True,
        secondary_info_getter=get_last_seen,
        minimum_value=1,
        show_bar_chart=False # Tắt biểu đồ cho BXH ít
    )

async def create_least_link_posters_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('link')} BXH User Gửi Link Ít Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_LINK_USERS_LIMIT,
        item_name_singular="link",
        item_name_plural="links",
        e=e,
        color=discord.Color.from_rgb(173, 216, 230),
        filter_admins=True,
        sort_ascending=True,
        minimum_value=1,
        show_bar_chart=False
    )

async def create_least_image_posters_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('image')} BXH User Gửi Ảnh Ít Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_IMAGE_USERS_LIMIT,
        item_name_singular="ảnh",
        item_name_plural="ảnh",
        e=e,
        color=discord.Color.from_rgb(144, 238, 144),
        filter_admins=True,
        sort_ascending=True,
        minimum_value=1,
        show_bar_chart=False
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

    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('mention')} BXH User Dùng Emoji Server Ít Nhất",
        counts=user_total_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_EMOJI_USERS_LIMIT,
        item_name_singular="emoji",
        item_name_plural="emojis",
        e=e,
        color=discord.Color.from_rgb(255, 223, 186),
        filter_admins=True,
        sort_ascending=True,
        secondary_info_getter=lambda uid, _: get_top_emoji(uid, user_detailed_counts),
        minimum_value=1,
        show_bar_chart=False
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

    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('sticker')} BXH User Gửi Sticker Ít Nhất",
        counts=user_total_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_STICKER_USERS_LIMIT,
        item_name_singular="sticker",
        item_name_plural="stickers",
        e=e,
        color=discord.Color.from_rgb(221, 160, 221),
        filter_admins=True,
        sort_ascending=True,
        secondary_info_getter=lambda uid, _: get_top_sticker(uid, user_detailed_counts),
        minimum_value=1,
        show_bar_chart=False
    )

async def create_least_mentioned_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('mention')} BXH User Được Nhắc Tên Ít Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_MENTIONED_USERS_LIMIT,
        item_name_singular="lần",
        item_name_plural="lần",
        e=e,
        color=discord.Color.from_rgb(218, 112, 214),
        filter_admins=False,
        sort_ascending=True,
        minimum_value=1,
        show_bar_chart=False
    )

async def create_least_mentioning_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('mention')} Top User Ít Nhắc Tên Người Khác Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_MENTIONING_USERS_LIMIT,
        item_name_singular="lần nhắc",
        item_name_plural="lần nhắc",
        e=e,
        color=discord.Color.from_rgb(186, 85, 211),
        filter_admins=True,
        sort_ascending=True,
        minimum_value=1,
        show_bar_chart=False
    )

async def create_least_repliers_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('reply')} BXH User Ít Trả Lời Tin Nhắn Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_REPLIERS_LIMIT,
        item_name_singular="lần trả lời",
        item_name_plural="lần trả lời",
        e=e,
        color=discord.Color.from_rgb(100, 149, 237),
        filter_admins=True,
        sort_ascending=True,
        minimum_value=1,
        show_bar_chart=False
    )

async def create_least_reaction_received_users_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    def get_footer_note(*args):
        return "Chỉ tính reaction đã lọc."
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('reaction')} BXH User Nhận Reactions Ít Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_REACTION_RECEIVED_USERS_LIMIT,
        item_name_singular="reaction",
        item_name_plural="reactions",
        e=e,
        color=discord.Color.from_rgb(255, 215, 0),
        filter_admins=False,
        sort_ascending=True,
        tertiary_info_getter=get_footer_note,
        minimum_value=1,
        show_bar_chart=False
    )

async def create_least_reaction_givers_embed(
    user_reaction_given_counts: Counter,
    guild: discord.Guild,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed xếp hạng người dùng thả reaction ít nhất."""
    e = lambda name: utils.get_emoji(name, bot)
    def get_footer_note(*args):
        return "Chỉ tính reaction đã lọc."
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('reaction')} BXH User Thả Reaction Ít Nhất",
        counts=user_reaction_given_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_REACTION_GIVERS_LIMIT,
        item_name_singular="reaction",
        item_name_plural="reactions",
        e=e,
        color=discord.Color.from_rgb(64, 224, 208),
        filter_admins=True,
        sort_ascending=True,
        tertiary_info_getter=get_footer_note,
        minimum_value=1,
        show_bar_chart=False
    )

async def create_least_distinct_channel_users_embed(
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    user_distinct_counts: Counter = scan_data.get("user_distinct_channel_counts", Counter())
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"🗺️ BXH {LEAST_DISTINCT_CHANNEL_USERS_LIMIT} \"Người Ẩn Dật\" Nhất",
        counts=user_distinct_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_DISTINCT_CHANNEL_USERS_LIMIT,
        item_name_singular="kênh/luồng",
        item_name_plural="kênh/luồng",
        e=e,
        color=discord.Color.from_rgb(0, 139, 139),
        filter_admins=True,
        sort_ascending=True,
        minimum_value=1,
        show_bar_chart=False
    )

async def create_least_activity_span_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    limit = LEAST_ACTIVITY_SPAN_USERS_LIMIT
    title = f"{e('clock')} BXH User Hoạt Động Ngắn Nhất Server"
    counts_for_sorting = collections.Counter({
        uid: data.get('activity_span_seconds', 0.0)
        for uid, data in user_activity.items()
        if not data.get('is_bot', False) and data.get('activity_span_seconds', 0.0) > 0
    })
    if not counts_for_sorting: return None

    async def get_last_seen_span(user_id, _):
        user_act_data = user_activity.get(user_id)
        if user_act_data:
            last_seen = user_act_data.get('last_seen')
            return f"• Seen: {utils.format_discord_time(last_seen, 'R')}" if last_seen else None
        return None

    formatted_counts = {
        uid: utils.format_timedelta(datetime.timedelta(seconds=span))
        for uid, span in counts_for_sorting.items()
    }
    sorted_users_by_seconds = sorted(counts_for_sorting.items(), key=lambda item: item[1])
    counts_for_helper = collections.OrderedDict([(uid, formatted_counts.get(uid, "N/A")) for uid, sec in sorted_users_by_seconds])

    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=title,
        counts=counts_for_helper,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=limit,
        item_name_singular="span",
        item_name_plural="span",
        e=e,
        color=discord.Color.from_rgb(119, 136, 153),
        filter_admins=False,
        sort_ascending=True,
        secondary_info_getter=get_last_seen_span,
        minimum_value=None,
        show_bar_chart=False
    )


async def create_least_thread_creators_embed(
    counts: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    e = lambda name: utils.get_emoji(name, bot)
    def get_footer_note(*args):
        return "Yêu cầu quyền View Audit Log."
    # Gọi hàm helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('thread')} Top User Ít Tạo Thread Nhất",
        counts=counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=LEAST_THREAD_CREATORS_LIMIT,
        item_name_singular="thread",
        item_name_plural="threads",
        e=e,
        color=discord.Color.from_rgb(147, 112, 219),
        filter_admins=True,
        sort_ascending=True,
        tertiary_info_getter=get_footer_note,
        minimum_value=1,
        show_bar_chart=False
    )

# --- END OF FILE reporting/embeds_user.py ---