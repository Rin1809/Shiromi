# --- START OF FILE reporting/embeds_items.py ---
import discord
import datetime
import math
import logging
import collections
import asyncio
from typing import List, Dict, Any, Optional, Union

# Relative import
try:
    from .. import utils
    from .embeds_user import create_generic_leaderboard_embed
except ImportError:
    import utils
    # from embeds_user import create_generic_leaderboard_embed
    pass

log = logging.getLogger(__name__)

# --- Constants ---
# Bỏ INVITES_PER_EMBED, WEBHOOKS_PER_EMBED, INTEGRATIONS_PER_EMBED
TOP_INVITERS_LIMIT = 30
TOP_STICKER_USAGE_LIMIT = 10


# --- Embed Functions ---

# --- LOẠI BỎ: create_invite_embeds ---
# --- LOẠI BỎ: create_webhook_integration_embeds ---

# --- Giữ lại các hàm Leaderboard ---
async def create_top_inviters_embed(
    invite_usage_counts: collections.Counter,
    guild: discord.Guild,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed xếp hạng người mời dựa trên tổng số lượt sử dụng các invite của họ."""
    e = lambda name: utils.get_emoji(name, bot)
    if not invite_usage_counts:
        return None

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
        )
    except NameError:
         log.warning("Không thể tạo embed Top Người Mời do thiếu 'create_generic_leaderboard_embed'.")
         return None
    except Exception as err:
        log.error(f"Lỗi tạo embed Top Người Mời: {err}", exc_info=True)
        return None


async def create_top_sticker_usage_embed(
    sticker_counts: collections.Counter,
    bot: discord.Client,
    guild: discord.Guild, # Thêm tham số guild
    limit: int = TOP_STICKER_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top stickers (server và mặc định) được sử dụng nhiều nhất."""
    if not sticker_counts: return None
    e = lambda name: utils.get_emoji(name, bot)

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
        log.debug("Fetch sticker hoàn thành.")

    sticker_lines = []
    for rank, (sticker_id_str, count) in enumerate(sorted_stickers, 1):
        display_sticker = f"ID: `{sticker_id_str}`" # Mặc định
        sticker_obj = None
        is_server_sticker = False

        if sticker_id_str.isdigit():
            sticker_obj = fetched_stickers_cache.get(int(sticker_id_str))
            if isinstance(sticker_obj, discord.GuildSticker):
                if sticker_obj.guild_id == guild.id: # Dùng guild.id
                    is_server_sticker = True
                    display_sticker += f" {e('star')}"
                    

        elif not sticker_id_str.isdigit(): # ID không hợp lệ?
            display_sticker = "`ID không hợp lệ?`"

        sticker_lines.append(f"**`#{rank:02d}`**. {display_sticker} — **{count:,}** lần")

    if len(sticker_counts) > limit:
        sticker_lines.append(f"\n... và {len(sticker_counts) - limit} sticker khác.")

    embed.description = desc + "\n\n" + "\n".join(sticker_lines)
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    embed.set_footer(text=f"{e('star')} = Sticker của Server này.")
    return embed

# --- END OF FILE reporting/embeds_items.py ---