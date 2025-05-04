# --- START OF FILE reporting/embeds_items.py ---
import discord
import datetime
import math
import logging
import collections
import asyncio
from typing import List, Dict, Any, Optional, Union, Set # <<< Thêm Set

# Relative import
try:
    from .. import utils
    from .embeds_user import create_generic_leaderboard_embed
except ImportError:
    import utils
    # from embeds_user import create_generic_leaderboard_embed # Giữ comment nếu vẫn có thể lỗi import vòng
    pass

log = logging.getLogger(__name__)

# --- Constants ---
TOP_INVITERS_LIMIT = 30
TOP_STICKER_USAGE_LIMIT = 15 # <<< Tăng lên 15 để hiển thị nhiều hơn

# --- Embed Functions ---

async def create_top_inviters_embed(
    invite_usage_counts: collections.Counter,
    guild: discord.Guild,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed xếp hạng người mời dựa trên tổng số lượt sử dụng các invite của họ."""
    e = lambda name: utils.get_emoji(name, bot)
    if not invite_usage_counts:
        return None

    # <<< FIX: Kiểm tra sự tồn tại của hàm helper trước khi gọi >>>
    if 'create_generic_leaderboard_embed' not in globals() or not callable(create_generic_leaderboard_embed):
        log.warning("Không thể tạo embed Top Người Mời do thiếu 'create_generic_leaderboard_embed'.")
        return None
    # <<< END FIX >>>

    try:
        # Sử dụng hàm tạo leaderboard chung
        return await create_generic_leaderboard_embed(
            counter_data=invite_usage_counts,
            guild=guild,
            bot=bot,
            title=f"{e('invite')} Top Người Mời (Lượt sử dụng)",
            item_name_singular="lượt dùng",
            item_name_plural="lượt dùng",
            limit=TOP_INVITERS_LIMIT,
            color=discord.Color.dark_teal(),
            footer_note="Dựa trên lượt sử dụng các lời mời đang hoạt động đã quét."
            # Mặc định filter_admins=True trong helper là ổn cho inviter
        )
    except Exception as err:
        log.error(f"Lỗi tạo embed Top Người Mời: {err}", exc_info=True)
        return None


async def create_top_sticker_usage_embed(
    sticker_counts: collections.Counter,
    bot: discord.Client,
    guild: discord.Guild,
    scan_data: Dict[str, Any], # <<< ADDED: Thêm scan_data để lấy sticker cache ID
    limit: int = TOP_STICKER_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top stickers (server và mặc định) được sử dụng nhiều nhất."""
    if not sticker_counts:
        log.debug("Bỏ qua tạo Top Sticker Usage embed: Counter rỗng.")
        return None
    e = lambda name: utils.get_emoji(name, bot)
    # <<< ADDED: Lấy cache ID sticker server >>>
    server_sticker_ids: Set[int] = scan_data.get("server_sticker_ids_cache", set())
    # <<< END ADDED >>>

    embed = discord.Embed(
        title=f"{e('award')} {e('sticker')} Top {limit} Stickers Được Dùng",
        color=discord.Color.dark_orange()
    )
    desc = "*Dựa trên số lần sticker được gửi.*"

    sorted_stickers = sticker_counts.most_common(limit)

    # Fetch thông tin sticker để hiển thị tên (chỉ fetch nếu là ID số)
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
    for rank, (sticker_id_str, count) in enumerate(sorted_stickers, 1):
        display_sticker = f"ID: `{sticker_id_str}`" # Mặc định
        sticker_obj: Optional[discord.Sticker] = None
        is_server_sticker = False
        sticker_name = "Unknown/Deleted" # Tên mặc định

        if sticker_id_str.isdigit():
            sticker_id = int(sticker_id_str)
            # <<< FIX: Kiểm tra sticker server bằng cache ID >>>
            if sticker_id in server_sticker_ids:
                is_server_sticker = True
            # <<< END FIX >>>
            sticker_obj = fetched_stickers_cache.get(sticker_id)
            if sticker_obj:
                sticker_name = utils.escape_markdown(sticker_obj.name)
                # Cập nhật lại display_sticker nếu fetch thành công
                display_sticker = f"`{sticker_name}` (ID: `{sticker_id_str}`)"

        elif not sticker_id_str.isdigit(): # ID không hợp lệ?
            display_sticker = "`ID không hợp lệ?`"
            sticker_name = "Invalid ID"

        # <<< FIX: Thêm emoji sao nếu là sticker server >>>
        if is_server_sticker:
             display_sticker += f" {e('star')}"
        # <<< END FIX >>>

        sticker_lines.append(f"**`#{rank:02d}`**. {display_sticker} — **{count:,}** lần")

    if not sticker_lines: # Nếu sau khi xử lý không còn dòng nào hợp lệ
        log.debug("Không có dòng sticker hợp lệ nào để hiển thị sau khi fetch/xử lý.")
        return None

    if len(sticker_counts) > limit:
        sticker_lines.append(f"\n... và {len(sticker_counts) - limit} sticker khác.")

    embed.description = desc + "\n\n" + "\n".join(sticker_lines)
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    embed.set_footer(text=f"{e('star')} = Sticker của Server này.")
    return embed

# --- END OF FILE reporting/embeds_items.py ---