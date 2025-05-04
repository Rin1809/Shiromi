# --- START OF FILE reporting/embeds_guild.py ---
import discord
import datetime
import math
import logging
import collections
import time # <<< ADDED IMPORT
from typing import List, Dict, Any, Optional, Union
from discord.ext import commands
from collections import Counter, defaultdict
import asyncio

# Relative import (giữ nguyên)
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
GOLDEN_HOUR_INTERVAL = 3
GOLDEN_HOUR_TOP_CHANNELS = 5

# --- Embed Creation Functions ---

# create_summary_embed (Không thay đổi, giữ nguyên phiên bản trước)
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

    # Lấy số liệu member từ cache ban đầu trong scan_data nếu có, nếu không thì từ server object
    member_count_real = scan_data.get('server_info', {}).get('member_count_real')
    bot_count_scan = scan_data.get('server_info', {}).get('bot_count')
    if member_count_real is None or bot_count_scan is None:
        log.warning("Thiếu server_info trong scan_data, lấy member count từ server object (có thể không chính xác lúc bắt đầu quét).")
        member_count_real = len([m for m in server.members if not m.bot])
        bot_count_scan = len([m for m in server.members if m.bot])

    # --- NÂNG CẤP: Lấy top custom emoji/sticker server ---
    top_custom_emojis_str = "N/A"
    top_custom_stickers_str = "N/A"

    # Lấy top custom emoji reactions (đã lọc)
    filtered_reaction_counts = scan_data.get("filtered_reaction_emoji_counts", Counter())
    custom_emoji_reactions = {eid: count for eid, count in filtered_reaction_counts.items() if isinstance(eid, int)}
    if custom_emoji_reactions:
        sorted_custom_reactions = sorted(custom_emoji_reactions.items(), key=lambda item: item[1], reverse=True)
        top_emojis = []
        for emoji_id, count in sorted_custom_reactions[:5]:
            emoji_obj = bot.get_emoji(emoji_id)
            if not emoji_obj:
                emoji_obj = scan_data.get("server_emojis_cache", {}).get(emoji_id)
            if emoji_obj:
                top_emojis.append(f"{str(emoji_obj)} ({count:,})")
        if top_emojis:
            top_custom_emojis_str = " ".join(top_emojis)
        elif sorted_custom_reactions:
            top_custom_emojis_str = f"Top ID: {sorted_custom_reactions[0][0]} ({sorted_custom_reactions[0][1]:,}), ..."

    # Lấy top custom stickers server (đã đếm trong scan_channels)
    custom_sticker_counts = scan_data.get("overall_custom_sticker_counts", Counter())
    if custom_sticker_counts:
        sorted_custom_stickers = custom_sticker_counts.most_common(5)
        top_stickers = []

        sticker_cache = scan_data.get("server_stickers_cache_objects")
        if sticker_cache is None:
            sticker_cache = {}
            log.debug("Summary Embed: Fetching sticker objects...")
            async def fetch_sticker_name(sid):
                if sid not in sticker_cache:
                    try: sticker_cache[sid] = await bot.fetch_sticker(sid)
                    except Exception: sticker_cache[sid] = None
                return sticker_cache[sid]
            fetch_tasks = [fetch_sticker_name(sid) for sid, count in sorted_custom_stickers if isinstance(sid, int)]
            await asyncio.gather(*fetch_tasks, return_exceptions=True)
            log.debug("Summary Embed: Fetch sticker objects complete.")

        for sticker_id, count in sorted_custom_stickers:
             sticker_obj = sticker_cache.get(sticker_id)
             name = f"`{sticker_obj.name}`" if sticker_obj else f"`ID:{sticker_id}`"
             top_stickers.append(f"{name} ({count:,})")
        if top_stickers:
             top_custom_stickers_str = ", ".join(top_stickers)
        elif sorted_custom_stickers:
            top_custom_stickers_str = f"Top ID: {sorted_custom_stickers[0][0]} ({sorted_custom_stickers[0][1]:,}), ..."

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
        timestamp=start_time + overall_duration # Thời gian kết thúc quét
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
    summary_embed.add_field(name="🧑‍🤝‍🧑 Users", value=f"{member_count_real:,}", inline=True)
    summary_embed.add_field(name=f"{e('bot_tag')} Bots", value=f"{bot_count_scan:,}", inline=True)

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
            f"{utils.get_emoji('mention', bot)} **Custom Emojis:** {len(scan_data.get('server_emojis_cache', {})):,} (Top Reactions: {top_custom_emojis_str})\n"
            f"{e('sticker')} **Custom Stickers:** {len(scan_data.get('server_sticker_ids_cache', set())):,} (Top Gửi: {top_custom_stickers_str})\n"
            f"{e('role')} **Roles:** {all_roles_count:,}"
        ),
        inline=False
    )

    # Footer
    footer_text = f"ID Server: {server.id}"
    if ctx: footer_text += f" | Yêu cầu bởi: {ctx.author.display_name} ({ctx.author.id})"
    summary_embed.set_footer(text=footer_text)

    return summary_embed

# create_channel_activity_embed (Không thay đổi, giữ nguyên phiên bản trước)
async def create_channel_activity_embed(
    guild: discord.Guild,
    bot: discord.Client,
    channel_details: List[Dict[str, Any]],
    voice_channel_static_data: List[Dict[str, Any]]
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
    top_voice_channels = sorted(
        [d for d in channel_details if d.get("processed") and d["type"] == str(discord.ChannelType.voice) and d.get("message_count", 0) > 0],
        key=lambda d: d.get("message_count", 0),
        reverse=True
    )
    top_voice_lines = []
    if top_voice_channels:
        for rank, detail in enumerate(top_voice_channels[:5], 1):
            channel = guild.get_channel(detail['id'])
            mention = channel.mention if channel else f"`#{utils.escape_markdown(detail['name'])}`"
            top_voice_lines.append(f"`#{rank}`. {mention} ({detail.get('message_count', 0):,} tin)")
    else:
        top_voice_lines.append("*Không tìm thấy tin nhắn trong kênh voice (hoặc API không hỗ trợ).*")

    # --- Tạo Embed ---
    embed = discord.Embed(
        title=f"💬 Hoạt động Kênh",
        color=discord.Color.green()
    )
    embed.add_field(
        name="🔥 Top Kênh Text \"Nóng\"",
        value="\n".join(top_text_lines) if top_text_lines else "Không có dữ liệu.",
        inline=False
    )
    embed.add_field(
        name="🎤 Top Kênh Voice \"Nóng\" (Chat Text)",
        value="\n".join(top_voice_lines),
        inline=False
    )

    if not top_text_lines and not top_voice_channels:
        return None

    return embed

# create_golden_hour_embed (Fix NameError)
async def create_golden_hour_embed(
    server_hourly_activity: Counter,
    channel_hourly_activity: Dict[int, Counter],
    thread_hourly_activity: Dict[int, Counter],
    guild: discord.Guild,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị khung giờ hoạt động sôi nổi nhất (Giờ Vàng)."""
    e = lambda name: utils.get_emoji(name, bot)
    if not server_hourly_activity:
        return None # Không có dữ liệu giờ

    # <<< FIX: Tính toán timezone offset từ module time >>>
    try:
        # Lấy offset của timezone local so với UTC tính bằng giây
        # time.timezone trả về offset tính bằng giây phía TÂY UTC (nên cần đảo dấu)
        # Chia cho 3600 để đổi sang giờ
        local_offset_hours = int(time.timezone / -3600)
        timezone_str = f"UTC{local_offset_hours:+d}" # Format thành "+H" hoặc "-H"
    except Exception as tz_err:
        log.warning(f"Không thể xác định timezone local của bot: {tz_err}. Mặc định về UTC.")
        timezone_str = "UTC" # Fallback về UTC nếu lỗi
    # <<< END FIX >>>

    embed = discord.Embed(
        # <<< FIX: Sử dụng timezone_str đã tính >>>
        title=f"☀️🌙 \"Giờ Vàng\" của Server ({timezone_str})",
        # <<< END FIX >>>
        description="*Khung giờ server và các kênh/luồng có nhiều tin nhắn nhất.*",
        color=discord.Color.gold()
    )

    # --- Tính Giờ Vàng Server ---
    hourly_grouped = defaultdict(int)
    for hour, count in server_hourly_activity.items():
        start_hour = (hour // GOLDEN_HOUR_INTERVAL) * GOLDEN_HOUR_INTERVAL
        hourly_grouped[start_hour] += count

    sorted_server_hours = sorted(hourly_grouped.items(), key=lambda item: item[1], reverse=True)

    server_golden_lines = []
    for rank, (start_hour, count) in enumerate(sorted_server_hours, 1):
        # <<< FIX: Tính toán giờ địa phương đúng cách >>>
        # Tạo datetime UTC giả lập với giờ bắt đầu
        utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=start_hour, minute=0, second=0, microsecond=0)
        # Chuyển sang timezone local của bot (nếu xác định được)
        try:
            # Lấy timezone object từ offset đã tính
            local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours))
            local_start_dt = utc_start_dt.astimezone(local_tz)
        except Exception: # Fallback về UTC nếu lỗi timezone
            local_start_dt = utc_start_dt

        local_end_dt = local_start_dt + datetime.timedelta(hours=GOLDEN_HOUR_INTERVAL)
        # Format giờ địa phương
        time_str = f"{local_start_dt.strftime('%H:%M')} - {local_end_dt.strftime('%H:%M')}"
        # <<< END FIX >>>
        server_golden_lines.append(f"**`#{rank}`**. **{time_str}**: {count:,} tin")
        if rank >= 3: # Chỉ hiển thị top 3 khung giờ server
            break

    embed.add_field(
        name="🏆 Khung Giờ Vàng Toàn Server",
        value="\n".join(server_golden_lines) if server_golden_lines else "Không có dữ liệu.",
        inline=False
    )

    # --- Tính Giờ Vàng Kênh/Luồng ---
    location_hourly_activity = defaultdict(Counter)
    for loc_id, counts in channel_hourly_activity.items(): location_hourly_activity[loc_id].update(counts)
    for loc_id, counts in thread_hourly_activity.items(): location_hourly_activity[loc_id].update(counts)

    location_golden_hours = {} # {loc_id: (start_hour, count)}
    for loc_id, hourly_counts in location_hourly_activity.items():
        if not hourly_counts: continue
        loc_grouped = defaultdict(int)
        for hour, count in hourly_counts.items():
            start_hour = (hour // GOLDEN_HOUR_INTERVAL) * GOLDEN_HOUR_INTERVAL
            loc_grouped[start_hour] += count
        if loc_grouped:
            best_start_hour, max_count = max(loc_grouped.items(), key=lambda item: item[1])
            location_golden_hours[loc_id] = (best_start_hour, max_count)

    # Sắp xếp kênh/luồng theo số tin nhắn giờ vàng của chúng
    sorted_locations_by_gold = sorted(location_golden_hours.items(), key=lambda item: item[1][1], reverse=True)

    location_golden_lines = []
    locations_shown = 0
    for loc_id, (start_hour, count) in sorted_locations_by_gold:
        if locations_shown >= GOLDEN_HOUR_TOP_CHANNELS: break # Giới hạn số dòng hiển thị

        location_obj = guild.get_channel_or_thread(loc_id)
        if not location_obj: continue

        loc_mention = location_obj.mention
        loc_type_emoji = utils.get_channel_type_emoji(location_obj, bot)

        # <<< FIX: Tính giờ địa phương >>>
        utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=start_hour, minute=0, second=0, microsecond=0)
        try:
            local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours))
            local_start_dt = utc_start_dt.astimezone(local_tz)
        except Exception:
            local_start_dt = utc_start_dt
        local_end_dt = local_start_dt + datetime.timedelta(hours=GOLDEN_HOUR_INTERVAL)
        time_str = f"{local_start_dt.strftime('%H:%M')}-{local_end_dt.strftime('%H:%M')}"
        # <<< END FIX >>>

        location_golden_lines.append(f"{loc_type_emoji} {loc_mention}: **{time_str}** ({count:,} tin)")
        locations_shown += 1

    embed.add_field(
        name=f"🏅 Giờ Vàng Của Top {GOLDEN_HOUR_TOP_CHANNELS} Kênh/Luồng",
        value="\n".join(location_golden_lines) if location_golden_lines else "Không có dữ liệu.",
        inline=False
    )

    return embed
# <<< END ADDED >>>

# --- END OF FILE reporting/embeds_guild.py ---