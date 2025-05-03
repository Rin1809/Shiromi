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
    # Cần import trực tiếp nếu chạy riêng lẻ
    # from embeds_user import create_generic_leaderboard_embed
    pass # Bỏ qua lỗi import khi chạy riêng

log = logging.getLogger(__name__)

# --- Constants ---
INVITES_PER_EMBED = 15          # Số invite mỗi embed
WEBHOOKS_PER_EMBED = 15         # Số webhook mỗi embed
INTEGRATIONS_PER_EMBED = 15     # Số integration mỗi embed
TOP_INVITERS_LIMIT = 30         # Giới hạn hiển thị top người mời
TOP_STICKER_USAGE_LIMIT = 10    # Giới hạn hiển thị top sticker được dùng


# --- Embed Functions ---

async def create_invite_embeds(
    invites: List[discord.Invite], # Danh sách invite đã fetch
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds danh sách các lời mời đang hoạt động của server."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    if not invites:
        # Trả về embed thông báo nếu không có invite
        no_invites_embed = discord.Embed(
            title=f"{e('invite')} Lời mời Server",
            description=f"{e('info')} Không tìm thấy lời mời nào đang hoạt động.",
            color=discord.Color.light_grey()
        )
        return [no_invites_embed]

    # Sắp xếp theo số lượt sử dụng giảm dần
    invites.sort(key=lambda inv: inv.uses or 0, reverse=True)
    num_invite_embeds = math.ceil(len(invites) / INVITES_PER_EMBED)

    for i in range(num_invite_embeds):
        start_index = i * INVITES_PER_EMBED
        end_index = start_index + INVITES_PER_EMBED
        invite_batch = invites[start_index:end_index]

        invite_embed = discord.Embed(
            title=f"{e('invite')} Lời mời Server Đang Hoạt Động (Phần {i + 1}/{num_invite_embeds})",
            description="*Sắp xếp theo số lượt sử dụng giảm dần.*",
            color=discord.Color.blurple()
        )

        invite_list_lines = []
        for inv in invite_batch:
            # Lấy thông tin người tạo invite
            inviter_mention = "Không rõ"
            inviter_display = ""
            if inv.inviter:
                inviter_mention = inv.inviter.mention
                inviter_display = f" (`{utils.escape_markdown(inv.inviter.display_name)}`)"

            # Kênh của invite
            channel_mention = inv.channel.mention if inv.channel else f"ID: `{inv.channel_id}`"

            # Số lượt sử dụng
            uses_str = f"{inv.uses or 0:,}" # Mặc định chỉ hiện số lượt dùng
            if inv.max_uses and inv.max_uses > 0:
                uses_str += f"/{inv.max_uses:,}" # Thêm max_uses nếu có giới hạn

            # Thời gian hết hạn và tạo
            expires_str = utils.format_discord_time(inv.expires_at, 'R') if inv.expires_at else "Không hết hạn"
            created_str = utils.format_discord_time(inv.created_at, 'R') if inv.created_at else "Không rõ"

            # Dòng chính: code và người tạo
            line1 = f"**`{inv.code}`** (Tạo bởi: {inviter_mention}{inviter_display})"
            # Dòng phụ 1: Kênh và lượt sử dụng
            line2 = f" └ {e('text_channel')} Kênh: {channel_mention} | {e('members')} SD: **{uses_str}**"
            # Dòng phụ 2: Thời gian
            line3 = f"   └ {e('calendar')} Tạo: {created_str} | {e('clock')} Hết hạn: {expires_str}"
            invite_list_lines.extend([line1, line2, line3])

        # Thêm vào description và giới hạn độ dài
        current_desc = invite_embed.description + "\n\n"
        new_content = "\n".join(invite_list_lines) if invite_list_lines else "Không có dữ liệu."
        if len(current_desc) + len(new_content) > 4000:
            remaining = 4000 - len(current_desc) - 20
            invite_embed.description = current_desc + new_content[:remaining] + "\n... (quá dài)"
        else:
            invite_embed.description = current_desc + new_content

        embeds.append(invite_embed)
    return embeds


async def create_top_inviters_embed(
    invite_usage_counts: collections.Counter, # Counter {user_id: total_uses}
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


async def create_webhook_integration_embeds(
    webhooks: List[discord.Webhook],
    integrations: List[discord.Integration],
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds danh sách webhooks và tích hợp của server."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)

    # --- Embeds cho Webhooks ---
    if webhooks:
        num_webhook_embeds = math.ceil(len(webhooks) / WEBHOOKS_PER_EMBED)
        for i in range(num_webhook_embeds):
            start_index = i * WEBHOOKS_PER_EMBED
            end_index = start_index + WEBHOOKS_PER_EMBED
            webhook_batch = webhooks[start_index:end_index]

            webhook_embed = discord.Embed(
                title=f"{e('webhook')} Webhooks Đang Hoạt Động (Phần {i + 1}/{num_webhook_embeds})",
                color=discord.Color.dark_grey()
            )
            webhook_list_lines = []
            for wh in webhook_batch:
                creator = "Không rõ"
                creator_display = ""
                if wh.user:
                    creator = wh.user.mention
                    creator_display = f" (`{utils.escape_markdown(wh.user.display_name)}`)"

                channel_mention = wh.channel.mention if wh.channel else f"ID: `{wh.channel_id}`"
                created_at_str = utils.format_discord_time(wh.created_at, 'R') if wh.created_at else "Không rõ"

                # Dòng chính: Tên webhook và ID
                line1 = f"**{utils.escape_markdown(wh.name)}** (`{wh.id}`)"
                # Dòng phụ 1: Kênh và người tạo
                line2 = f" └ {e('text_channel')} Kênh: {channel_mention} | {e('crown')} Tạo bởi: {creator}{creator_display}"
                # Dòng phụ 2: Thời gian tạo và URL (ẩn mặc định)
                line3 = f"   └ {e('calendar')} Tạo: {created_at_str} | {e('invite')} URL: ||`{wh.url}`||"
                webhook_list_lines.extend([line1, line2, line3])

            # Thêm vào description và giới hạn độ dài
            new_content = "\n".join(webhook_list_lines) if webhook_list_lines else "Không có dữ liệu."
            if len(new_content) > 4000:
                webhook_embed.description = new_content[:4000] + "\n... (quá dài)"
            else:
                webhook_embed.description = new_content
            embeds.append(webhook_embed)

    # --- Embeds cho Integrations ---
    if integrations:
        num_integration_embeds = math.ceil(len(integrations) / INTEGRATIONS_PER_EMBED)
        for i in range(num_integration_embeds):
            start_index = i * INTEGRATIONS_PER_EMBED
            end_index = start_index + INTEGRATIONS_PER_EMBED
            integration_batch = integrations[start_index:end_index]

            integration_embed = discord.Embed(
                title=f"{e('integration')} Tích Hợp Server (Phần {i + 1}/{num_integration_embeds})",
                color=discord.Color.dark_purple()
            )
            integration_list_lines = []
            for integ in integration_batch:
                # Lấy thông tin tích hợp
                type_str = integ.type if isinstance(integ.type, str) else integ.type.name # twitch, youtube, discord, etc.
                account_info = f"{utils.escape_markdown(integ.account.name)} (`{integ.account.id}`)" if integ.account else "N/A"
                enabled_str = f"{e('success')} Bật" if integ.enabled else f"{e('error')} Tắt"
                sync_str = f" | Đồng bộ: {'Có' if integ.syncing else 'Không'}" if hasattr(integ, 'syncing') else ""
                role_str = f" | Role: {integ.role.mention}" if hasattr(integ, 'role') and integ.role else ""
                expire_str = f" | Hết hạn: {integ.expire_behaviour.name}" if hasattr(integ, 'expire_behaviour') and integ.expire_behaviour else ""
                grace_str = f" | TG chờ: {integ.expire_grace_period}s" if hasattr(integ, 'expire_grace_period') and integ.expire_grace_period is not None else ""

                # Dòng chính: Tên và loại
                line1 = f"**{utils.escape_markdown(integ.name)}** ({type_str.capitalize()})"
                # Dòng phụ 1: Tài khoản, trạng thái, đồng bộ, role
                line2 = f" └ {e('id_card')} TK: {account_info} | {enabled_str}{sync_str}{role_str}"
                # Dòng phụ 2: Thông tin hết hạn (nếu có)
                line3 = f"   └{expire_str}{grace_str}".strip() # Chỉ thêm nếu có thông tin

                integration_list_lines.append(line1)
                integration_list_lines.append(line2)
                if line3: # Chỉ thêm dòng 3 nếu có nội dung
                    integration_list_lines.append(line3)

            # Thêm vào description và giới hạn độ dài
            new_content = "\n".join(integration_list_lines) if integration_list_lines else "Không có dữ liệu."
            if len(new_content) > 4000:
                 integration_embed.description = new_content[:4000] + "\n... (quá dài)"
            else:
                 integration_embed.description = new_content
            embeds.append(integration_embed)

    # Nếu không có cả webhook và integration
    if not webhooks and not integrations:
        no_data_embed = discord.Embed(
            title=f"{e('webhook')}/{e('integration')} Webhooks & Tích Hợp",
            description="Không tìm thấy webhook hoặc tích hợp nào.",
            color=discord.Color.light_grey()
        )
        embeds.append(no_data_embed)

    return embeds


async def create_top_sticker_usage_embed(
    sticker_counts: collections.Counter, # Counter {sticker_id_str: count}
    bot: discord.Client,
    limit: int = TOP_STICKER_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top stickers được sử dụng nhiều nhất."""
    if not sticker_counts: return None
    e = lambda name: utils.get_emoji(name, bot)

    embed = discord.Embed(
        title=f"{e('award')} {e('sticker')} Top {limit} Stickers Được Dùng",
        color=discord.Color.dark_orange()
    )
    desc = "*Dựa trên số lần sticker được gửi.*"

    sorted_stickers = sticker_counts.most_common(limit)

    # Fetch thông tin sticker để hiển thị tên (nếu có thể)
    sticker_ids_to_fetch = [int(sid) for sid, count in sorted_stickers if sid.isdigit()]
    fetched_stickers_cache: Dict[int, Optional[discord.Sticker]] = {}
    if sticker_ids_to_fetch and bot:
        log.debug(f"Fetching {len(sticker_ids_to_fetch)} stickers for top usage embed...")
        async def fetch_sticker_safe(sticker_id):
            try: return await bot.fetch_sticker(sticker_id)
            except Exception: return None # Trả về None nếu fetch lỗi

        # Chạy fetch song song
        results = await asyncio.gather(*(fetch_sticker_safe(sid) for sid in sticker_ids_to_fetch))
        for sticker in results:
            if sticker: fetched_stickers_cache[sticker.id] = sticker
        log.debug("Fetch sticker hoàn thành.")

    # Tạo danh sách hiển thị
    sticker_lines = []
    for rank, (sticker_id_str, count) in enumerate(sorted_stickers, 1):
        display_sticker = f"ID: `{sticker_id_str}`" # Mặc định hiển thị ID
        sticker_obj = None
        if sticker_id_str.isdigit():
            sticker_obj = fetched_stickers_cache.get(int(sticker_id_str))

        if sticker_obj:
            # Nếu fetch được, hiển thị tên sticker
            display_sticker = f"`{utils.escape_markdown(sticker_obj.name)}`"
        elif not sticker_id_str.isdigit():
            # Trường hợp ID không hợp lệ
            display_sticker = "`ID không hợp lệ`"

        sticker_lines.append(f"**`#{rank:02d}`**. {display_sticker} — **{count:,}** lần")

    if len(sticker_counts) > limit:
        sticker_lines.append(f"\n... và {len(sticker_counts) - limit} sticker khác.")

    embed.description = desc + "\n\n" + "\n".join(sticker_lines)
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    return embed

# --- END OF FILE reporting/embeds_items.py ---