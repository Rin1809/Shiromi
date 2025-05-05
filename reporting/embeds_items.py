# --- START OF FILE reporting/embeds_items.py ---
import discord
import datetime
import math
import logging
import collections
import asyncio
from typing import List, Dict, Any, Optional, Union, Set
import unicodedata

import utils # Chỉ import utils
import config
# Bỏ import này vì hàm _format_user_tree_line đã chuyển sang utils
# from .embeds_user import _format_user_tree_line

log = logging.getLogger(__name__)

# --- Constants ---
TOP_INVITERS_LIMIT = 15
TOP_STICKER_USAGE_LIMIT = 15
UNUSED_EMOJI_LIMIT = 25
LEAST_STICKER_USAGE_LIMIT = 15

# --- Embed Functions ---

async def create_top_inviters_embed(
    invite_usage_counts: collections.Counter,
    guild: discord.Guild,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed xếp hạng người mời."""
    e = lambda name: utils.get_emoji(name, bot)
    title = f"{e('invite')} Top Người Mời (Lượt sử dụng)"
    limit = TOP_INVITERS_LIMIT
    filter_admins = False
    color=discord.Color.dark_teal()
    item_name_singular="lượt dùng"
    item_name_plural="lượt dùng"
    footer_note="Dựa trên lượt sử dụng các lời mời đang hoạt động đã quét."

    if not invite_usage_counts:
        log.debug("Bỏ qua tạo Top Người Mời embed: Không có dữ liệu.")
        return None

    filtered_sorted_users = [
        (uid, count) for uid, count in invite_usage_counts.most_common()
        if count > 0 and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users:
        log.debug("Bỏ qua tạo Top Người Mời embed: Không có user hợp lệ.")
        return None

    # Gọi helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=title,
        counts=invite_usage_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=limit,
        item_name_singular=item_name_singular,
        item_name_plural=item_name_plural,
        e=e,
        color=color,
        filter_admins=filter_admins,
        tertiary_info_getter=lambda *_: footer_note,
        minimum_value=1,
        show_bar_chart=True
    )


async def create_top_sticker_usage_embed(
    sticker_counts: collections.Counter,
    bot: discord.Client,
    guild: discord.Guild,
    scan_data: Dict[str, Any],
    limit: int = TOP_STICKER_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top stickers được sử dụng nhiều nhất."""
    if not sticker_counts:
        log.debug("Bỏ qua tạo Top Sticker Usage embed: Counter rỗng.")
        return None
    e = lambda name: utils.get_emoji(name, bot)
    server_sticker_ids: Set[int] = scan_data.get("server_sticker_ids_cache", set())

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    title_item_emoji = e('sticker') if e('sticker') != '❓' else '✨'
    embed = discord.Embed(
        title=f"{title_emoji} {title_item_emoji} BXH Top {limit} Stickers Được Dùng Nhiều Nhất",
        color=discord.Color.dark_orange()
    )
    desc_base = "*Dựa trên số lần sticker được gửi.*"

    sorted_stickers = sticker_counts.most_common()
    if not sorted_stickers: return None

    bar_chart_str = ""
    data_for_chart = sorted_stickers[:5]
    if data_for_chart:
        sticker_ids_to_fetch_chart = [int(sid) for sid, count in data_for_chart if sid.isdigit()]
        chart_sticker_name_cache: Dict[int, str] = {}
        if sticker_ids_to_fetch_chart:
             chart_sticker_name_cache = await utils._fetch_sticker_dict(sticker_ids_to_fetch_chart, bot)

        async def format_sticker_key(sticker_id_str):
            if sticker_id_str.isdigit():
                sticker_id = int(sticker_id_str)
                name = chart_sticker_name_cache.get(sticker_id, "...")
                prefix = f"{e('star')} " if sticker_id in server_sticker_ids else ""
                return f"{prefix}'{utils.escape_markdown(name)}'"
            return f"ID:{sticker_id_str}"

        bar_chart_str = await utils.create_vertical_text_bar_chart(
            sorted_data=data_for_chart,
            key_formatter=format_sticker_key,
            top_n=5, max_chart_height=8, bar_width=1, bar_spacing=1,
            chart_title="Top 5 Stickers", show_legend=True
        )

    display_list_stickers = sorted_stickers[:limit]
    sticker_ids_to_fetch_list = [int(sid) for sid, count in display_list_stickers if sid.isdigit()]
    list_sticker_name_cache: Dict[int, str] = {}
    if sticker_ids_to_fetch_list:
         list_sticker_name_cache = await utils._fetch_sticker_dict(sticker_ids_to_fetch_list, bot)

    sticker_lines = []
    podium_emojis = ["🥇", "🥈", "🥉"]
    for rank, (sticker_id_str, count) in enumerate(display_list_stickers, 1):
        display_sticker = f"ID: `{sticker_id_str}`"
        is_server_sticker = False
        if sticker_id_str.isdigit():
            sticker_id = int(sticker_id_str)
            if sticker_id in server_sticker_ids: is_server_sticker = True
            sticker_name = list_sticker_name_cache.get(sticker_id, "...")
            display_sticker = f"'{utils.escape_markdown(sticker_name)}' (`{sticker_id_str}`)"
        elif not sticker_id_str.isdigit():
            display_sticker = "`ID không hợp lệ?`"

        if is_server_sticker: display_sticker += f" {e('star')}"

        rank_prefix = podium_emojis[rank-1] if rank <= 3 else f"`#{rank:02d}`"
        sticker_lines.append(f"{rank_prefix} {display_sticker} — **{count:,}** lần")

    if not sticker_lines: return None

    if len(sticker_counts) > limit:
        sticker_lines.append(f"\n... và {len(sticker_counts) - limit} sticker khác.")

    embed.description = desc_base
    if bar_chart_str:
        embed.description += "\n\n" + bar_chart_str
    embed.description += "\n\n" + "\n".join(sticker_lines)

    if len(embed.description) > 4096:
        embed.description = embed.description[:4090] + "\n[...]"

    embed.set_footer(text=f"{e('star')} = Sticker của Server này.")
    return embed


async def create_least_sticker_usage_embed(
    sticker_counts: collections.Counter,
    bot: discord.Client,
    guild: discord.Guild,
    scan_data: Dict[str, Any],
    limit: int = LEAST_STICKER_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top stickers ÍT được sử dụng nhất (có > 0 lượt dùng)."""
    if not sticker_counts:
        log.debug("Bỏ qua tạo Least Sticker Usage embed: Counter rỗng.")
        return None
    e = lambda name: utils.get_emoji(name, bot)
    server_sticker_ids: Set[int] = scan_data.get("server_sticker_ids_cache", set())

    title_emoji = '📉'
    title_item_emoji = e('sticker') if e('sticker') != '❓' else '✨'
    embed = discord.Embed(
        title=f"{title_emoji} {title_item_emoji} BXH Top {limit} Stickers Ít Được Sử Dụng Nhất",
        color=discord.Color.from_rgb(176, 196, 222)
    )
    desc_base = "*Dựa trên số lần sticker được gửi. Chỉ tính sticker có > 0 lượt.*"

    filtered_stickers = {sid: count for sid, count in sticker_counts.items() if count > 0}
    if not filtered_stickers:
        embed.description = desc_base + "\n\n*Không có sticker nào được sử dụng ít nhất 1 lần.*"
        return embed

    sorted_stickers = sorted(filtered_stickers.items(), key=lambda item: item[1])

    display_list_stickers = sorted_stickers[:limit]
    sticker_ids_to_fetch_list = [int(sid) for sid, count in display_list_stickers if sid.isdigit()]
    list_sticker_name_cache: Dict[int, str] = {}
    if sticker_ids_to_fetch_list:
        list_sticker_name_cache = await utils._fetch_sticker_dict(sticker_ids_to_fetch_list, bot)

    sticker_lines = []
    for rank, (sticker_id_str, count) in enumerate(display_list_stickers, 1):
        display_sticker = f"ID: `{sticker_id_str}`"
        is_server_sticker = False
        if sticker_id_str.isdigit():
            sticker_id = int(sticker_id_str)
            if sticker_id in server_sticker_ids: is_server_sticker = True
            sticker_name = list_sticker_name_cache.get(sticker_id, "...")
            display_sticker = f"'{utils.escape_markdown(sticker_name)}' (`{sticker_id_str}`)"
        elif not sticker_id_str.isdigit():
            display_sticker = "`ID không hợp lệ?`"

        if is_server_sticker: display_sticker += f" {e('star')}"

        rank_prefix = f"`#{rank:02d}`"
        sticker_lines.append(f"{rank_prefix} {display_sticker} — **{count:,}** lần")

    if not sticker_lines: return None

    if len(filtered_stickers) > limit:
        sticker_lines.append(f"\n... và {len(filtered_stickers) - limit} sticker khác (> 0 lượt).")

    embed.description = desc_base + "\n\n" + "\n".join(sticker_lines)
    if len(embed.description) > 4096:
        embed.description = embed.description[:4090] + "\n[...]"

    embed.set_footer(text=f"{e('star')} = Sticker của Server này.")
    return embed

async def create_unused_emoji_embed(
    guild: discord.Guild,
    overall_custom_emoji_content_counts: collections.Counter,
    bot: discord.Client,
    limit: int = UNUSED_EMOJI_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed liệt kê các emoji CỦA SERVER không được sử dụng trong nội dung."""
    e = lambda name: utils.get_emoji(name, bot)
    server_emojis = guild.emojis
    if not server_emojis:
        log.debug("Server không có emoji nào để kiểm tra unused.")
        return None

    used_ids = set(overall_custom_emoji_content_counts.keys())
    unused_emojis = [emoji for emoji in server_emojis if emoji.id not in used_ids]

    if not unused_emojis:
        embed = discord.Embed(
            title=f"{e('success')} Emoji Server Đều Được Sử Dụng!",
            description="*Tất cả emoji của server này đều đã được sử dụng ít nhất 1 lần trong nội dung tin nhắn.*",
            color=discord.Color.green()
        )
        return embed

    title_emoji = e('info') if e('info') != '❓' else 'ℹ️'
    embed = discord.Embed(
        title=f"{title_emoji} Emoji Server Không Được Sử Dụng",
        description=f"*Danh sách các emoji của server này không xuất hiện trong nội dung tin nhắn đã quét (tối đa {limit}).*",
        color=discord.Color.blue()
    )

    unused_lines = [str(emoji) for emoji in unused_emojis[:limit]]
    embed.description += "\n\n" + " ".join(unused_lines)

    if len(unused_emojis) > limit:
        embed.set_footer(text=f"... và {len(unused_emojis) - limit} emoji khác không được sử dụng.")

    return embed

# --- END OF FILE reporting/embeds_items.py ---
