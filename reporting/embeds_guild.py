# --- START OF FILE reporting/embeds_guild.py ---
import discord
import datetime
import math
import logging
import collections
import time
from typing import List, Dict, Any, Optional, Union
from discord.ext import commands
from collections import Counter, defaultdict
import asyncio
import re

log = logging.getLogger(__name__)

import utils
import config

# --- Constants ---
VOICE_CHANNELS_PER_EMBED = 20
FIRST_MESSAGES_LIMIT = 10
FIRST_MESSAGES_CONTENT_PREVIEW = 100
GOLDEN_HOUR_INTERVAL = 3
GOLDEN_HOUR_TOP_CHANNELS = 5
CHANNEL_ACTIVITY_LIMIT = 10
UMBRA_HOUR_INTERVAL = 3
UMBRA_HOUR_TOP_CHANNELS = 5

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
    """Tạo embed tóm tắt chính thông tin server và kết quả quét."""
    e = lambda name: utils.get_emoji(name, bot)

    explicit_filter = str(server.explicit_content_filter).replace('_', ' ').title()
    mfa_level = "Yêu cầu (Cho Mod)" if server.mfa_level >= discord.MFALevel.require_2fa else "Không yêu cầu"
    notifications = "Chỉ @mention" if server.default_notifications == discord.NotificationLevel.only_mentions else "Tất cả tin nhắn"

    current_members_list: List[discord.Member] = scan_data.get("current_members_list", [])
    member_count_real = len([m for m in current_members_list if not m.bot]) if current_members_list else 'N/A'
    bot_count_scan = len([m for m in current_members_list if m.bot]) if current_members_list else 'N/A'

    total_custom_emojis = len(scan_data.get("server_emojis_cache", server.emojis))
    total_custom_stickers = len(scan_data.get("server_sticker_ids_cache", server.stickers))

    filtered_reaction_count = overall_total_reaction_count if overall_total_reaction_count is not None else 0
    reaction_line = f"\n{e('reaction')} Tổng **{filtered_reaction_count:,}** biểu cảm (lọc)." if config.ENABLE_REACTION_SCAN else ""
    scan_summary = (
        f"Quét **{processed_channels_count:,}** kênh text/voice ({skipped_channels_count} lỗi/bỏ qua).\n"
        f"Quét **{processed_threads_count:,}** luồng ({skipped_threads_count} lỗi/bỏ qua).\n"
        f"Tổng **{overall_total_message_count:,}** tin nhắn."
        f"{reaction_line}\n"
        f"**{user_activity_count:,}** users có hoạt động.\n\n"
        f"{e('clock')} **Tổng thời gian quét:** {utils.format_timedelta(overall_duration, high_precision=True)}"
    )

    summary_embed = discord.Embed(
        title=f"{e('star')} Tổng Quan Server: {server.name} {e('star')}",
        description=scan_summary,
        color=discord.Color.purple(),
        timestamp=start_time + overall_duration
    )
    if server.icon:
        summary_embed.set_thumbnail(url=server.icon.url)

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

    # --- Phần hiển thị kênh được cải thiện ---
    channel_stats_lines = []
    # Các loại kênh chính muốn hiển thị
    types_to_show = [
        discord.ChannelType.text,
        discord.ChannelType.voice,
        discord.ChannelType.category,
        discord.ChannelType.stage_voice,
        discord.ChannelType.forum,
    ]
    total_channels_from_counter = 0
    for chan_type in types_to_show:
        count = channel_counts.get(chan_type, 0)
        total_channels_from_counter += count # Cộng dồn số lượng kênh
        emoji = utils.get_channel_type_emoji(chan_type, bot)
        # Lấy tên loại kênh và định dạng
        type_name = chan_type.name.replace('_', ' ').title()
        channel_stats_lines.append(f"{emoji} {type_name}: **{count}**")

    # Thêm số lượng Thread (luồng) đã quét
    thread_emoji = utils.get_channel_type_emoji(discord.ChannelType.public_thread, bot) # Dùng emoji của public thread
    channel_stats_lines.append(f"{thread_emoji} Threads (Đã quét): **{processed_threads_count}**")

    # Thêm field mới vào embed
    summary_embed.add_field(
        name=f"{e('info')} Phân Loại Kênh ({total_channels_from_counter}) & Luồng", # Hiển thị tổng số kênh từ counter
        value="\n".join(channel_stats_lines), # Nối các dòng bằng newline
        inline=False # Để field này chiếm toàn bộ chiều rộng
    )
    # --- Kết thúc phần cải thiện ---


    summary_embed.add_field(
        name=f"{e('star')} Điểm Nhấn Server",
        value=(
            f"{utils.get_emoji('mention', bot)} **Custom Emojis:** {total_custom_emojis:,}\n"
            f"{e('sticker')} **Custom Stickers:** {total_custom_stickers:,}\n"
            f"{e('role')} **Roles:** {all_roles_count:,}"
        ),
        inline=False
    )

    footer_text = f"ID Server: {server.id}"
    if ctx: footer_text += f" | Yêu cầu bởi: {ctx.author.display_name} ({ctx.author.id})"
    summary_embed.set_footer(text=footer_text)

    return summary_embed


async def create_channel_activity_embed(
    guild: discord.Guild,
    bot: discord.Client,
    channel_details: List[Dict[str, Any]],
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top kênh text/voice hoạt động NHIỀU NHẤT."""
    e = lambda name: utils.get_emoji(name, bot)
    limit = CHANNEL_ACTIVITY_LIMIT

    top_text_channels_data = sorted(
        [(d.get('id'), d.get("message_count", 0))
         for d in channel_details
         if d.get("processed") and d.get("type") == str(discord.ChannelType.text) and d.get("message_count", 0) > 0],
        key=lambda item: item[1],
        reverse=True
    )
    top_voice_channels_data = sorted(
        [(d.get('id'), d.get("message_count", 0))
         for d in channel_details
         if d.get("processed") and d.get("type") == str(discord.ChannelType.voice) and d.get("message_count", 0) > 0],
        key=lambda item: item[1],
        reverse=True
    )

    if not top_text_channels_data and not top_voice_channels_data:
        log.debug("Không có dữ liệu kênh hoạt động nhiều để tạo embed.")
        return None

    async def format_channel_key(channel_id):
        channel = guild.get_channel_or_thread(channel_id)
        if channel:
            type_emoji = utils.get_channel_type_emoji(channel, bot)
            return f"{type_emoji} {utils.escape_markdown(channel.name)}"
        return f"ID:{channel_id}"

    text_chart_str = ""
    if top_text_channels_data:
        text_chart_str = await utils.create_vertical_text_bar_chart(
            sorted_data=top_text_channels_data[:5],
            key_formatter=format_channel_key,
            top_n=5, max_chart_height=8, bar_width=1, bar_spacing=2,
            chart_title="Top 5 Kênh Text", show_legend=True
        )

    voice_chart_str = ""
    if top_voice_channels_data:
         voice_chart_str = await utils.create_vertical_text_bar_chart(
             sorted_data=top_voice_channels_data[:5],
             key_formatter=format_channel_key,
             top_n=5, max_chart_height=8, bar_width=1, bar_spacing=2,
             chart_title="Top 5 Kênh Voice (Chat)", show_legend=True
         )

    top_text_lines = []
    for rank, (channel_id, count) in enumerate(top_text_channels_data[:limit], 1):
        channel = guild.get_channel(channel_id)
        mention = channel.mention if channel else f"`#{utils.escape_markdown(next((d.get('name', 'Unknown') for d in channel_details if d.get('id') == channel_id), 'Unknown'))}`"
        top_text_lines.append(f"`#{rank}`. {mention} ({count:,} tin)")

    top_voice_lines = []
    if top_voice_channels_data:
        for rank, (channel_id, count) in enumerate(top_voice_channels_data[:limit], 1):
            channel = guild.get_channel(channel_id)
            mention = channel.mention if channel else f"`#{utils.escape_markdown(next((d.get('name', 'Unknown') for d in channel_details if d.get('id') == channel_id), 'Unknown'))}`"
            top_voice_lines.append(f"`#{rank}`. {mention} ({count:,} tin)")
    else:
        if any(d.get("type") == str(discord.ChannelType.voice) and d.get("processed") for d in channel_details):
            top_voice_lines.append("*Không tìm thấy tin nhắn chat trong kênh voice.*")

    embed = discord.Embed(
        title=f"🔥 Top {limit} Kênh Hoạt Động Nhiều Nhất",
        color=discord.Color.green()
    )

    text_field_value = (text_chart_str + "\n\n" if text_chart_str else "") + ("\n".join(top_text_lines) if top_text_lines else "*Không có dữ liệu*")
    if len(text_field_value) > 1024: text_field_value = text_field_value[:1020] + "\n[...]"
    embed.add_field(
        name=f"{utils.get_channel_type_emoji(discord.ChannelType.text, bot)} Kênh Text",
        value=text_field_value,
        inline=False
    )

    voice_field_value = (voice_chart_str + "\n\n" if voice_chart_str else "") + ("\n".join(top_voice_lines) if top_voice_lines else "*Không có dữ liệu*")
    if len(voice_field_value) > 1024: voice_field_value = voice_field_value[:1020] + "\n[...]"
    embed.add_field(
        name=f"{utils.get_channel_type_emoji(discord.ChannelType.voice, bot)} Kênh Voice (Chat Text)",
        value=voice_field_value,
        inline=False
    )

    return embed

async def create_least_channel_activity_embed(
    guild: discord.Guild,
    bot: discord.Client,
    channel_details: List[Dict[str, Any]],
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top kênh text/voice hoạt động ÍT NHẤT (có > 0 tin nhắn)."""
    e = lambda name: utils.get_emoji(name, bot)
    limit = CHANNEL_ACTIVITY_LIMIT

    least_text_channels = sorted(
        [d for d in channel_details if d.get("processed") and d.get("type") == str(discord.ChannelType.text) and d.get("message_count", 0) > 0],
        key=lambda d: d.get("message_count", 0),
        reverse=False
    )
    least_text_lines = []
    for rank, detail in enumerate(least_text_channels[:limit], 1):
        channel = guild.get_channel(detail.get('id'))
        mention = channel.mention if channel else f"`#{utils.escape_markdown(detail.get('name', 'Unknown'))}`"
        least_text_lines.append(f"`#{rank}`. {mention} ({detail.get('message_count', 0):,} tin)")

    least_voice_channels = sorted(
        [d for d in channel_details if d.get("processed") and d.get("type") == str(discord.ChannelType.voice) and d.get("message_count", 0) > 0],
        key=lambda d: d.get("message_count", 0),
        reverse=False
    )
    least_voice_lines = []
    if least_voice_channels:
        for rank, detail in enumerate(least_voice_channels[:limit], 1):
            channel = guild.get_channel(detail.get('id'))
            mention = channel.mention if channel else f"`#{utils.escape_markdown(detail.get('name', 'Unknown'))}`"
            least_voice_lines.append(f"`#{rank}`. {mention} ({detail.get('message_count', 0):,} tin)")

    if not least_text_channels and not least_voice_channels:
        log.debug("Không có dữ liệu kênh hoạt động ít (>0) để tạo embed.")
        return None

    embed = discord.Embed(
        title=f"📉 Top {limit} Kênh Hoạt Động Ít Nhất",
        description="*Chỉ tính các kênh có ít nhất 1 tin nhắn được quét.*",
        color=discord.Color.light_grey()
    )

    text_field_value = "\n".join(least_text_lines) if least_text_lines else "*Không có kênh text nào phù hợp*"
    embed.add_field(
        name=f"{utils.get_channel_type_emoji(discord.ChannelType.text, bot)} Kênh Text",
        value=text_field_value[:1024],
        inline=False
    )

    voice_field_value = "\n".join(least_voice_lines) if least_voice_lines else "*Không có kênh voice nào phù hợp*"
    embed.add_field(
        name=f"{utils.get_channel_type_emoji(discord.ChannelType.voice, bot)} Kênh Voice (Chat Text)",
        value=voice_field_value[:1024],
        inline=False
    )

    return embed


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
        log.debug("Không có dữ liệu giờ để tạo embed Giờ Vàng.")
        return None

    local_offset_hours = utils.get_local_timezone_offset()
    timezone_str = f"UTC{local_offset_hours:+d}" if local_offset_hours is not None else "UTC"

    embed = discord.Embed(
        title=f"☀️🌙 \"Giờ Vàng\" của Server ({timezone_str})",
        color=discord.Color.gold()
    )

    hourly_grouped = defaultdict(int)
    for hour, count in server_hourly_activity.items():
        if isinstance(hour, int) and 0 <= hour <= 23:
            start_hour = (hour // GOLDEN_HOUR_INTERVAL) * GOLDEN_HOUR_INTERVAL
            hourly_grouped[start_hour] += count
        else:
            log.warning(f"Bỏ qua dữ liệu giờ không hợp lệ cho server: hour={hour} (type: {type(hour)})")

    if not hourly_grouped:
        log.warning("Không có dữ liệu giờ hợp lệ để tính giờ vàng server.")
        return None

    sorted_server_hours = sorted(hourly_grouped.items(), key=lambda item: item[1], reverse=True)

    bar_chart_server_str = ""
    data_for_chart_server = sorted_server_hours[:5]
    if data_for_chart_server:
         def format_hour_key(start_hour):
             try:
                 utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=start_hour, minute=0, second=0, microsecond=0)
                 local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours))
                 local_start_dt = utc_start_dt.astimezone(local_tz)
                 local_end_dt = local_start_dt + datetime.timedelta(hours=GOLDEN_HOUR_INTERVAL)
                 return f"{local_start_dt.strftime('%H:%M')}-{local_end_dt.strftime('%H:%M')}"
             except: return f"H:{start_hour}"

         bar_chart_server_str = await utils.create_vertical_text_bar_chart(
             sorted_data=data_for_chart_server,
             key_formatter=format_hour_key,
             top_n=5, max_chart_height=6, bar_width=1, bar_spacing=1,
             chart_title="Top Khung Giờ Server", show_legend=True
         )
         embed.description = bar_chart_server_str
    else:
        embed.description = "*Khung giờ server và các kênh/chủ đề có nhiều tin nhắn nhất.*"

    server_golden_lines = []
    for rank, (start_hour, count) in enumerate(sorted_server_hours, 1):
        try:
            utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=start_hour, minute=0, second=0, microsecond=0)
            local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours))
            local_start_dt = utc_start_dt.astimezone(local_tz)
        except ValueError: continue
        except Exception as tz_convert_err: local_start_dt = utc_start_dt
        local_end_dt = local_start_dt + datetime.timedelta(hours=GOLDEN_HOUR_INTERVAL)
        time_str = f"{local_start_dt.strftime('%H:%M')} - {local_end_dt.strftime('%H:%M')}"
        server_golden_lines.append(f"**`#{rank}`**. **{time_str}**: {count:,} tin")
        if rank >= 3: break

    embed.add_field(
        name="🏆 Khung Giờ Vàng Toàn Server",
        value="\n".join(server_golden_lines) if server_golden_lines else "Không có dữ liệu.",
        inline=False
    )

    location_hourly_activity = defaultdict(Counter)
    for loc_id, counts in channel_hourly_activity.items():
         if guild.get_channel_or_thread(loc_id):
             for hour, count in counts.items():
                 if isinstance(hour, int) and 0 <= hour <= 23: location_hourly_activity[loc_id][hour] += count
                 else: log.warning(f"Bỏ qua dữ liệu giờ không hợp lệ cho channel {loc_id}: hour={hour}")
    for loc_id, counts in thread_hourly_activity.items():
        if guild.get_channel_or_thread(loc_id):
            for hour, count in counts.items():
                if isinstance(hour, int) and 0 <= hour <= 23: location_hourly_activity[loc_id][hour] += count
                else: log.warning(f"Bỏ qua dữ liệu giờ không hợp lệ cho thread {loc_id}: hour={hour}")

    location_golden_hours = {}
    for loc_id, hourly_counts in location_hourly_activity.items():
        if not hourly_counts: continue
        loc_grouped = defaultdict(int)
        for hour, count in hourly_counts.items():
            start_hour = (hour // GOLDEN_HOUR_INTERVAL) * GOLDEN_HOUR_INTERVAL
            loc_grouped[start_hour] += count
        if loc_grouped:
            try:
                best_start_hour, max_count = max(loc_grouped.items(), key=lambda item: item[1])
                datetime.datetime.now(datetime.timezone.utc).replace(hour=best_start_hour, minute=0)
                location_golden_hours[loc_id] = (best_start_hour, max_count)
            except ValueError: log.warning(f"Giờ vàng không hợp lệ ({best_start_hour}) được tính cho location {loc_id}, bỏ qua.")
            except Exception as e_loc_gold: log.warning(f"Lỗi khi tính giờ vàng cho location {loc_id}: {e_loc_gold}")

    sorted_locations_by_gold = sorted(location_golden_hours.items(), key=lambda item: item[1][1], reverse=True)

    location_golden_lines = []
    locations_shown = 0
    for loc_id, (start_hour, count) in sorted_locations_by_gold:
        if locations_shown >= GOLDEN_HOUR_TOP_CHANNELS: break
        location_obj = guild.get_channel_or_thread(loc_id)
        if not location_obj: continue
        loc_mention = location_obj.mention; loc_type_emoji = utils.get_channel_type_emoji(location_obj, bot)
        try:
             utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=start_hour, minute=0, second=0, microsecond=0)
             local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours))
             local_start_dt = utc_start_dt.astimezone(local_tz)
        except ValueError: continue
        except Exception as tz_convert_err_loc: local_start_dt = utc_start_dt
        local_end_dt = local_start_dt + datetime.timedelta(hours=GOLDEN_HOUR_INTERVAL)
        time_str = f"{local_start_dt.strftime('%H:%M')}-{local_end_dt.strftime('%H:%M')}"
        location_golden_lines.append(f"{loc_type_emoji} {loc_mention}: **{time_str}** ({count:,} tin)")
        locations_shown += 1

    embed.add_field(
        name=f"🏅 Giờ Vàng Của Top {GOLDEN_HOUR_TOP_CHANNELS} Kênh/Luồng",
        value="\n".join(location_golden_lines) if location_golden_lines else "Không có dữ liệu.",
        inline=False
    )
    if embed.description and len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"
    return embed


async def create_umbra_hour_embed(
    server_hourly_activity: Counter,
    channel_hourly_activity: Dict[int, Counter],
    thread_hourly_activity: Dict[int, Counter],
    guild: discord.Guild,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị khung giờ hoạt động yên ắng nhất (Giờ Âm)."""
    e = lambda name: utils.get_emoji(name, bot)
    if not server_hourly_activity:
        log.debug("Không có dữ liệu giờ để tạo embed Giờ Âm.")
        return None

    local_offset_hours = utils.get_local_timezone_offset()
    timezone_str = f"UTC{local_offset_hours:+d}" if local_offset_hours is not None else "UTC"

    embed = discord.Embed(
        title=f"🌃👻 \"Giờ Âm\" của Server ({timezone_str})",
        description="*Khung giờ server và các kênh/chủ đề có ít tin nhắn nhất.*",
        color=discord.Color.dark_blue()
    )

    hourly_grouped = defaultdict(int)
    for h in range(0, 24, UMBRA_HOUR_INTERVAL): hourly_grouped[h] = 0
    for hour, count in server_hourly_activity.items():
        if isinstance(hour, int) and 0 <= hour <= 23:
            start_hour = (hour // UMBRA_HOUR_INTERVAL) * UMBRA_HOUR_INTERVAL
            hourly_grouped[start_hour] += count
        else: log.warning(f"Bỏ qua dữ liệu giờ không hợp lệ cho server (giờ âm): hour={hour} (type: {type(hour)})")

    if not hourly_grouped:
        log.warning("Không có dữ liệu giờ hợp lệ để tính giờ âm server.")
        return None

    sorted_server_hours = sorted(hourly_grouped.items(), key=lambda item: item[1])

    server_umbra_lines = []
    for rank, (start_hour, count) in enumerate(sorted_server_hours, 1):
        try:
            utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=start_hour, minute=0, second=0, microsecond=0)
            local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours))
            local_start_dt = utc_start_dt.astimezone(local_tz)
        except ValueError: continue
        except Exception as tz_convert_err: local_start_dt = utc_start_dt
        local_end_dt = local_start_dt + datetime.timedelta(hours=UMBRA_HOUR_INTERVAL)
        time_str = f"{local_start_dt.strftime('%H:%M')} - {local_end_dt.strftime('%H:%M')}"
        server_umbra_lines.append(f"**`#{rank}`**. **{time_str}**: {count:,} tin")
        if rank >= 3: break

    embed.add_field(
        name="📉 Khung Giờ Yên Ắng Nhất Server",
        value="\n".join(server_umbra_lines) if server_umbra_lines else "Không có dữ liệu.",
        inline=False
    )

    location_hourly_activity = defaultdict(Counter)
    for loc_id, counts in channel_hourly_activity.items():
         if guild.get_channel_or_thread(loc_id):
             for hour, count in counts.items():
                 if isinstance(hour, int) and 0 <= hour <= 23: location_hourly_activity[loc_id][hour] += count
                 else: log.warning(f"Bỏ qua dữ liệu giờ không hợp lệ cho channel {loc_id} (giờ âm): hour={hour}")
    for loc_id, counts in thread_hourly_activity.items():
        if guild.get_channel_or_thread(loc_id):
            for hour, count in counts.items():
                if isinstance(hour, int) and 0 <= hour <= 23: location_hourly_activity[loc_id][hour] += count
                else: log.warning(f"Bỏ qua dữ liệu giờ không hợp lệ cho thread {loc_id} (giờ âm): hour={hour}")

    location_umbra_hours = {}
    for loc_id, hourly_counts in location_hourly_activity.items():
        if not hourly_counts: continue
        loc_grouped = defaultdict(int)
        for h in range(0, 24, UMBRA_HOUR_INTERVAL): loc_grouped[h] = 0
        for hour, count in hourly_counts.items():
            start_hour = (hour // UMBRA_HOUR_INTERVAL) * UMBRA_HOUR_INTERVAL
            loc_grouped[start_hour] += count
        if loc_grouped:
            try:
                least_start_hour, min_count = min(loc_grouped.items(), key=lambda item: item[1])
                datetime.datetime.now(datetime.timezone.utc).replace(hour=least_start_hour, minute=0)
                location_umbra_hours[loc_id] = (least_start_hour, min_count)
            except ValueError: log.warning(f"Giờ âm không hợp lệ ({least_start_hour}) được tính cho location {loc_id}, bỏ qua.")
            except Exception as e_loc_umbra: log.warning(f"Lỗi khi tính giờ âm cho location {loc_id}: {e_loc_umbra}")

    sorted_locations_by_umbra = sorted(location_umbra_hours.items(), key=lambda item: item[1][1])

    location_umbra_lines = []
    locations_shown = 0
    for loc_id, (start_hour, count) in sorted_locations_by_umbra:
        if locations_shown >= UMBRA_HOUR_TOP_CHANNELS: break
        location_obj = guild.get_channel_or_thread(loc_id)
        if not location_obj: continue
        loc_mention = location_obj.mention; loc_type_emoji = utils.get_channel_type_emoji(location_obj, bot)
        try:
             utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=start_hour, minute=0, second=0, microsecond=0)
             local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours))
             local_start_dt = utc_start_dt.astimezone(local_tz)
        except ValueError: continue
        except Exception as tz_convert_err_loc: local_start_dt = utc_start_dt
        local_end_dt = local_start_dt + datetime.timedelta(hours=UMBRA_HOUR_INTERVAL)
        time_str = f"{local_start_dt.strftime('%H:%M')}-{local_end_dt.strftime('%H:%M')}"
        location_umbra_lines.append(f"{loc_type_emoji} {loc_mention}: **{time_str}** ({count:,} tin)")
        locations_shown += 1

    embed.add_field(
        name=f"❄️ Giờ Yên Ắng Của Top {UMBRA_HOUR_TOP_CHANNELS} Kênh/Luồng",
        value="\n".join(location_umbra_lines) if location_umbra_lines else "Không có dữ liệu.",
        inline=False
    )

    return embed

# --- END OF FILE reporting/embeds_guild.py ---