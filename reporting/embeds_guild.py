# --- START OF FILE reporting/embeds_guild.py ---
import discord
import datetime
import math
import logging
import collections
from typing import List, Dict, Any, Optional, Union
from discord.ext import commands
from collections import Counter
import asyncio


try:
    from .. import utils
    from .. import config 
except ImportError:
    import utils
    import config

log = logging.getLogger(__name__)

# --- Constants ---
VOICE_CHANNELS_PER_EMBED = 20
FIRST_MESSAGES_LIMIT = 10
FIRST_MESSAGES_CONTENT_PREVIEW = 100


# --- Embed Creation Functions ---

async def create_summary_embed(
    server: discord.Guild,
    bot: discord.Client,
    processed_channels_count: int,
    processed_threads_count: int,
    skipped_channels_count: int,
    skipped_threads_count: int,
    overall_total_message_count: int,
    user_activity_count: int,
    overall_duration: datetime.timedelta,
    initial_member_status_counts: collections.Counter,
    channel_counts: collections.Counter,
    all_roles_count: int,
    start_time: datetime.datetime,
    scan_data: Dict[str, Any],
    ctx: Optional[commands.Context] = None,
    overall_total_reaction_count: Optional[int] = None 
) -> discord.Embed:
    """Tạo embed tóm tắt chính thông tin server và kết quả quét (đã nâng cấp)."""
    e = lambda name: utils.get_emoji(name, bot)

    # --- Chuẩn bị các giá trị hiển thị ---
    explicit_filter = str(server.explicit_content_filter).replace('_', ' ').title()
    mfa_level = "Yêu cầu (Cho Mod)" if server.mfa_level >= discord.MFALevel.require_2fa else "Không yêu cầu"
    notifications = "Chỉ @mention" if server.default_notifications == discord.NotificationLevel.only_mentions else "Tất cả tin nhắn"

    member_count = len([m for m in server.members if not m.bot])
    bot_count = len([m for m in server.members if m.bot])

    # --- NÂNG CẤP: Lấy top custom emoji/sticker server ---
    top_custom_emojis_str = "N/A"
    top_custom_stickers_str = "N/A"

    # Lấy top custom emoji reactions (đã lọc)
    filtered_reaction_counts = scan_data.get("filtered_reaction_emoji_counts", Counter())
    custom_emoji_reactions = {eid: count for eid, count in filtered_reaction_counts.items() if isinstance(eid, int)} # Chỉ lấy ID emoji
    if custom_emoji_reactions:
        sorted_custom_reactions = sorted(custom_emoji_reactions.items(), key=lambda item: item[1], reverse=True)
        top_emojis = []
        for emoji_id, count in sorted_custom_reactions[:5]: # Lấy top 5
            emoji_obj = bot.get_emoji(emoji_id)
            if emoji_obj:
                top_emojis.append(f"{str(emoji_obj)} ({count:,})")
        if top_emojis:
            top_custom_emojis_str = " ".join(top_emojis)

    # Lấy top custom stickers server (đã đếm trong scan_channels)
    custom_sticker_counts = scan_data.get("overall_custom_sticker_counts", Counter())
    if custom_sticker_counts:
        sorted_custom_stickers = custom_sticker_counts.most_common(5) # Lấy top 5
        top_stickers = []
        # Fetch tên sticker (có thể chậm nếu nhiều) - Cân nhắc chỉ hiển thị ID nếu cần tối ưu
        sticker_cache = {}
        async def fetch_sticker_name(sid):
             if sid not in sticker_cache:
                 try: sticker_cache[sid] = await bot.fetch_sticker(sid)
                 except: sticker_cache[sid] = None
             return sticker_cache[sid]

        fetch_tasks = [fetch_sticker_name(sid) for sid, count in sorted_custom_stickers]
        await asyncio.gather(*fetch_tasks, return_exceptions=True)

        for sticker_id, count in sorted_custom_stickers:
             sticker_obj = sticker_cache.get(sticker_id)
             name = f"`{sticker_obj.name}`" if sticker_obj else f"`ID:{sticker_id}`"
             top_stickers.append(f"{name} ({count:,})")
        if top_stickers:
             top_custom_stickers_str = ", ".join(top_stickers)

    # Chuỗi tóm tắt kết quả quét (sử dụng reaction đã lọc)
    filtered_reaction_count = sum(filtered_reaction_counts.values())
    reaction_line = f"\n{e('reaction')} Tổng **{filtered_reaction_count:,}** biểu cảm (lọc)." if config.ENABLE_REACTION_SCAN else ""
    scan_summary = (
        f"Quét **{processed_channels_count:,}** kênh text/voice ({skipped_channels_count} lỗi/bỏ qua).\n"
        f"Quét **{processed_threads_count:,}** luồng ({skipped_threads_count} lỗi/bỏ qua).\n"
        f"Tổng **{overall_total_message_count:,}** tin nhắn."
        f"{reaction_line}\n"
        f"**{user_activity_count:,}** users có hoạt động.\n\n"
        f"{e('clock')} **Tổng thời gian quét:** {utils.format_timedelta(overall_duration, high_precision=True)}"
    )

    # --- Tạo Embed ---
    summary_embed = discord.Embed(
        title=f"{e('star')} Tổng Quan Server: {server.name} {e('star')}",
        description=scan_summary,
        color=discord.Color.purple(),
        timestamp=start_time + overall_duration
    )
    if server.icon:
        summary_embed.set_thumbnail(url=server.icon.url)

    # --- Thêm Fields ---
    owner = server.owner
    if not owner and server.owner_id:
        try: owner = await utils.fetch_user_data(server, server.owner_id, bot_ref=bot)
        except Exception as owner_err: log.warning(f"Lỗi fetch owner {server.owner_id}: {owner_err}"); owner = None
    owner_mention = owner.mention if owner else (f'`{server.owner_id}` (Không rõ)' if server.owner_id else 'Không rõ')
    summary_embed.add_field(name=f"{e('crown')} Chủ sở hữu", value=owner_mention, inline=True)
    summary_embed.add_field(name=f"{e('calendar')} Ngày tạo", value=utils.format_discord_time(server.created_at, 'D'), inline=True)
    summary_embed.add_field(name=f"{e('boost')} Boost", value=f"Cấp {server.premium_tier} ({server.premium_subscription_count})", inline=True)

    summary_embed.add_field(name=f"{e('members')} Tổng Members", value=f"{server.member_count:,} (Cache)", inline=True)
    summary_embed.add_field(name="🧑‍🤝‍🧑 Users", value=f"{member_count:,}", inline=True)
    summary_embed.add_field(name=f"{e('bot_tag')} Bots", value=f"{bot_count:,}", inline=True)

    # Thống kê kênh (giữ nguyên)
    channel_stats_lines = [
        f"{utils.get_channel_type_emoji(discord.ChannelType.text, bot)} Text: {channel_counts.get(discord.ChannelType.text, 0)}",
        f"{utils.get_channel_type_emoji(discord.ChannelType.voice, bot)} Voice: {channel_counts.get(discord.ChannelType.voice, 0)}",
        f"{utils.get_channel_type_emoji(discord.ChannelType.category, bot)} Cat: {channel_counts.get(discord.ChannelType.category, 0)}",
        f"{utils.get_channel_type_emoji(discord.ChannelType.stage_voice, bot)} Stage: {channel_counts.get(discord.ChannelType.stage_voice, 0)}",
        f"{utils.get_channel_type_emoji(discord.ChannelType.forum, bot)} Forum: {channel_counts.get(discord.ChannelType.forum, 0)}",
        f"{utils.get_channel_type_emoji(discord.ChannelType.public_thread, bot)} Thread: {processed_threads_count}"
    ]
    summary_embed.add_field(
        name=f"{e('info')} Kênh ({sum(channel_counts.values())}) & Luồng",
        value=" | ".join(channel_stats_lines),
        inline=False
    )

    # --- NÂNG CẤP: Field Điểm Nhấn ---
    summary_embed.add_field(
        name=f"{e('star')} Điểm Nhấn Server",
        value=(
            f"{utils.get_emoji('mention', bot)} **Custom Emojis:** {len(server.emojis):,} (Top: {top_custom_emojis_str})\n"
            f"{e('sticker')} **Custom Stickers:** {len(server.stickers):,} (Top: {top_custom_stickers_str})\n"
            f"{e('role')} **Roles:** {all_roles_count:,}"
        ),
        inline=False
    )

    # Footer
    footer_text = f"ID Server: {server.id}"
    if ctx: footer_text += f" | Yêu cầu bởi: {ctx.author.display_name} ({ctx.author.id})"
    summary_embed.set_footer(text=footer_text)

    return summary_embed


async def create_channel_activity_embed( # Đổi tên hàm cho rõ nghĩa
    guild: discord.Guild,
    bot: discord.Client,
    channel_details: List[Dict[str, Any]],
    voice_channel_static_data: List[Dict[str, Any]] # Thêm data kênh voice tĩnh
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top kênh text/voice hoạt động."""
    e = lambda name: utils.get_emoji(name, bot)

    # --- Top Kênh Text ---
    top_text_channels = sorted(
        [d for d in channel_details if d.get("processed") and d["type"] == str(discord.ChannelType.text)],
        key=lambda d: d.get("message_count", 0),
        reverse=True
    )
    top_text_lines = []
    for rank, detail in enumerate(top_text_channels[:5], 1):
        channel = guild.get_channel(detail['id'])
        mention = channel.mention if channel else f"`#{utils.escape_markdown(detail['name'])}`"
        top_text_lines.append(f"`#{rank}`. {mention} ({detail.get('message_count', 0):,} tin)")

    # --- Top Kênh Voice ---
    # Cần dữ liệu voice states lúc quét để biết kênh nào đông - TẠM THỜI CHƯA CÓ
    # Thay vào đó, có thể hiển thị các kênh voice được tạo gần đây hoặc kênh có tên hấp dẫn?
    # Hoặc chỉ hiển thị top kênh text
    top_voice_lines = ["*Cần cập nhật logic để lấy top kênh voice đông*"] # Placeholder
    # Ví dụ lấy kênh voice tĩnh được tạo gần đây nhất:
    # sorted_voice_static = sorted(voice_channel_static_data, key=lambda vc: vc.get('created_at') or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc), reverse=True)
    # for rank, vc_data in enumerate(sorted_voice_static[:5], 1):
    #      vc = guild.get_channel(vc_data['id'])
    #      mention = vc.mention if vc else f"`{utils.escape_markdown(vc_data['name'])}`"
    #      created_str = utils.format_discord_time(vc_data.get('created_at'), 'R')
    #      top_voice_lines.append(f"`#{rank}`. {mention} (Tạo: {created_str})")

    # --- Giờ Vàng ---
    # Cần phân tích timestamp tin nhắn - TẠM THỜI CHƯA CÓ
    golden_hour_str = "*Cần cập nhật logic để xác định giờ vàng*" # Placeholder

    # --- Tạo Embed ---
    embed = discord.Embed(
        title=f"💬 Hoạt động Kênh & Giờ Vàng 🌙",
        color=discord.Color.green()
    )
    embed.add_field(
        name="🔥 Top Kênh Text \"Nóng\"",
        value="\n".join(top_text_lines) if top_text_lines else "Không có dữ liệu.",
        inline=False
    )
    embed.add_field(
        name="🎤 Top Kênh Voice \"Đông Vui\"",
        value="\n".join(top_voice_lines), # Hiện tại là placeholder
        inline=False
    )
    embed.add_field(
        name="☀️ \"Giờ Vàng\" của Server",
        value=golden_hour_str, # Hiện tại là placeholder
        inline=False
    )

    if not top_text_lines: # Nếu không có kênh text nào thì trả về None
        return None

    return embed


# --- END OF FILE reporting/embeds_guild.py ---