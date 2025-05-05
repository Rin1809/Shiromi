# --- START OF FILE reporting/embeds_items.py ---
import discord
import datetime
import math
import logging
import collections
import asyncio
from typing import List, Dict, Any, Optional, Union, Set

# Sử dụng import tuyệt đối cho utils và config
import utils
import config
# Import helper định dạng cây từ embeds_user
from .embeds_user import _format_user_tree_line # <--- Import helper này

log = logging.getLogger(__name__)

# --- Constants ---
TOP_INVITERS_LIMIT = 15 # Giảm giới hạn để phù hợp cây
TOP_STICKER_USAGE_LIMIT = 15

# --- Embed Functions ---

async def create_top_inviters_embed(
    invite_usage_counts: collections.Counter,
    guild: discord.Guild,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed xếp hạng người mời dựa trên tổng số lượt sử dụng các invite của họ (DẠNG CÂY)."""
    e = lambda name: utils.get_emoji(name, bot)
    # Bỏ '#' ở đầu title (Không có sẵn, nhưng để nhất quán)
    title = f"{e('invite')} Top Người Mời (Lượt sử dụng)"
    limit = TOP_INVITERS_LIMIT
    filter_admins = False # Thường không lọc admin cho BXH mời
    color=discord.Color.dark_teal()
    item_name_singular="lượt dùng"
    item_name_plural="lượt dùng"
    footer_note="Dựa trên lượt sử dụng các lời mời đang hoạt động đã quét."

    if not invite_usage_counts:
        log.debug("Bỏ qua tạo Top Người Mời embed: Không có dữ liệu.")
        return None

    # Lọc bot (admin không lọc theo filter_admins=False)
    filtered_sorted_users = [
        (uid, count) for uid, count in invite_usage_counts.most_common()
        if count > 0 and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users:
        log.debug("Bỏ qua tạo Top Người Mời embed: Không có user hợp lệ.")
        return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Đã lọc bot.*" # Không lọc admin
    description_lines = [desc_prefix, ""]

    for rank, (user_id, count) in enumerate(users_to_display, 1):
        # Không có thông tin phụ cho BXH này
        lines = await _format_user_tree_line(
            rank, user_id, count, item_name_singular, item_name_plural,
            guild, user_cache, secondary_info=None
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


async def create_top_sticker_usage_embed(
    sticker_counts: collections.Counter,
    bot: discord.Client,
    guild: discord.Guild,
    scan_data: Dict[str, Any],
    limit: int = TOP_STICKER_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top stickers (server và mặc định) được sử dụng nhiều nhất."""
    # (Giữ nguyên hàm này vì không phải BXH user)
    if not sticker_counts:
        log.debug("Bỏ qua tạo Top Sticker Usage embed: Counter rỗng.")
        return None
    e = lambda name: utils.get_emoji(name, bot)
    server_sticker_ids: Set[int] = scan_data.get("server_sticker_ids_cache", set())

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    title_item_emoji = e('sticker') if e('sticker') != '❓' else '✨'
    embed = discord.Embed(
        title=f"{title_emoji} {title_item_emoji} BXH Top {limit} Stickers Của Server Được Dùng Nhiều Nhất",
        color=discord.Color.dark_orange()
    )
    desc = "*Dựa trên số lần sticker được gửi.*"

    sorted_stickers = sticker_counts.most_common(limit)

    sticker_ids_to_fetch = [int(sid) for sid, count in sorted_stickers if sid.isdigit()]
    fetched_stickers_cache: Dict[int, Optional[discord.Sticker]] = {}
    if sticker_ids_to_fetch and bot:
        log.debug(f"Fetching {len(sticker_ids_to_fetch)} stickers for top usage embed...")
        async def fetch_sticker_safe(sticker_id):
            try: return await bot.fetch_sticker(sticker_id)
            except Exception: return None
        results = await asyncio.gather(*(fetch_sticker_safe(sid) for sid in sticker_ids_to_fetch))
        for sticker in results:
            if sticker: fetched_stickers_cache[sticker.id] = sticker
        log.debug(f"Fetch sticker hoàn thành cho top usage. Cache size: {len(fetched_stickers_cache)}")

    sticker_lines = []
    podium_emojis = ["🥇", "🥈", "🥉"] # Thêm podium cho sticker
    for rank, (sticker_id_str, count) in enumerate(sorted_stickers, 1):
        display_sticker = f"ID: `{sticker_id_str}`"
        sticker_obj: Optional[discord.Sticker] = None
        is_server_sticker = False
        sticker_name = "Unknown/Deleted"

        if sticker_id_str.isdigit():
            sticker_id = int(sticker_id_str)
            if sticker_id in server_sticker_ids: is_server_sticker = True
            sticker_obj = fetched_stickers_cache.get(sticker_id)
            if sticker_obj:
                sticker_name = utils.escape_markdown(sticker_obj.name)
                # Hiển thị tên và ID
                display_sticker = f"'{sticker_name}' (`{sticker_id_str}`)"
        elif not sticker_id_str.isdigit():
            display_sticker = "`ID không hợp lệ?`"
            sticker_name = "Invalid ID"

        if is_server_sticker: display_sticker += f" {e('star')}"

        rank_prefix = podium_emojis[rank-1] if rank <= 3 else f"`#{rank:02d}`"
        sticker_lines.append(f"{rank_prefix} {display_sticker} — **{count:,}** lần")

    if not sticker_lines:
        log.debug("Không có dòng sticker hợp lệ nào để hiển thị sau khi fetch/xử lý.")
        return None

    if len(sticker_counts) > limit:
        sticker_lines.append(f"\n... và {len(sticker_counts) - limit} sticker khác.")

    embed.description = desc + "\n\n" + "\n".join(sticker_lines)
    if len(embed.description) > 4096:
        embed.description = embed.description[:4090] + "\n[...]"

    embed.set_footer(text=f"{e('star')} = Sticker của Server này.")
    return embed

# --- END OF FILE reporting/embeds_items.py ---