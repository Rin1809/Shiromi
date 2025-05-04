# --- START OF FILE reporting/embeds_guild.py ---
import discord
import datetime
import math
import logging # <--- Di chuyển import logging lên đầu
import collections
import time
from typing import List, Dict, Any, Optional, Union
from discord.ext import commands
from collections import Counter, defaultdict
import asyncio
import re

# --- SỬA: Định nghĩa log NGAY ĐẦU FILE ---
log = logging.getLogger(__name__)
# ---------------------------------------

# Relative import (Ưu tiên cách này)
try:
    from .. import utils
    from .. import config
except ImportError:
    # --- SỬA: Loại bỏ khối fallback import phức tạp và chỉ log cảnh báo nếu cần ---
    # Log cảnh báo rằng import tương đối thất bại, có thể dùng import tuyệt đối nếu biết đường dẫn
    # Tuy nhiên, khi chạy như một cog, import tương đối PHẢI hoạt động.
    # Nếu ImportError vẫn xảy ra, đó là dấu hiệu của vấn đề cấu trúc thư mục hoặc cách chạy file.
    log.warning("ImportError khi thực hiện relative import trong embeds_guild.py. Kiểm tra cấu trúc project và cách chạy.")
    # Cố gắng import tuyệt đối như một fallback cuối cùng (ít khả thi hơn trong cấu trúc cog)
    try:
        import utils
        import config
    except ImportError:
        log.critical("Không thể import utils và config ngay cả với import tuyệt đối trong embeds_guild.py!")
        # Không thể hoạt động nếu thiếu utils/config, có thể raise lỗi ở đây
        raise


# --- Constants ---
VOICE_CHANNELS_PER_EMBED = 20
FIRST_MESSAGES_LIMIT = 10
FIRST_MESSAGES_CONTENT_PREVIEW = 100
GOLDEN_HOUR_INTERVAL = 3
GOLDEN_HOUR_TOP_CHANNELS = 5

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
    all_roles_count: int, # Sử dụng trực tiếp số role đã đếm
    start_time: datetime.datetime,
    scan_data: Dict[str, Any],
    ctx: Optional[commands.Context] = None,
    overall_total_reaction_count: Optional[int] = None # Đổi tên thành count thôi
) -> discord.Embed:
    """Tạo embed tóm tắt chính thông tin server và kết quả quét (đã nâng cấp)."""
    e = lambda name: utils.get_emoji(name, bot)

    # --- Chuẩn bị các giá trị hiển thị ---
    explicit_filter = str(server.explicit_content_filter).replace('_', ' ').title()
    mfa_level = "Yêu cầu (Cho Mod)" if server.mfa_level >= discord.MFALevel.require_2fa else "Không yêu cầu"
    notifications = "Chỉ @mention" if server.default_notifications == discord.NotificationLevel.only_mentions else "Tất cả tin nhắn"

    # Lấy số liệu member từ cache ban đầu trong scan_data nếu có, nếu không thì từ server object
    current_members_list: List[discord.Member] = scan_data.get("current_members_list", [])
    member_count_real = len([m for m in current_members_list if not m.bot]) if current_members_list else 'N/A'
    bot_count_scan = len([m for m in current_members_list if m.bot]) if current_members_list else 'N/A'

    # --- Lấy tổng số emoji và sticker ---
    total_custom_emojis = len(scan_data.get("server_emojis_cache", server.emojis))
    total_custom_stickers = len(scan_data.get("server_sticker_ids_cache", server.stickers))

    # Chuỗi tóm tắt kết quả quét (sử dụng reaction đã lọc)
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

    # Thống kê kênh
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

    # --- Field Điểm Nhấn Server (Đã sửa) ---
    summary_embed.add_field(
        name=f"{e('star')} Điểm Nhấn Server",
        value=(
            f"{utils.get_emoji('mention', bot)} **Custom Emojis:** {total_custom_emojis:,}\n"
            f"{e('sticker')} **Custom Stickers:** {total_custom_stickers:,}\n"
            f"{e('role')} **Roles:** {all_roles_count:,}"
        ),
        inline=False
    )

    # Footer
    footer_text = f"ID Server: {server.id}"
    if ctx: footer_text += f" | Yêu cầu bởi: {ctx.author.display_name} ({ctx.author.id})"
    summary_embed.set_footer(text=footer_text)

    return summary_embed


async def create_channel_activity_embed(
    guild: discord.Guild,
    bot: discord.Client,
    channel_details: List[Dict[str, Any]],
    voice_channel_static_data: List[Dict[str, Any]] # Giữ lại param này nếu có logic dùng sau
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top kênh text/voice hoạt động."""
    e = lambda name: utils.get_emoji(name, bot)

    # --- Top Kênh Text ---
    top_text_channels = sorted(
        [d for d in channel_details if d.get("processed") and d.get("type") == str(discord.ChannelType.text)],
        key=lambda d: d.get("message_count", 0),
        reverse=True
    )
    top_text_lines = []
    for rank, detail in enumerate(top_text_channels[:5], 1):
        channel = guild.get_channel(detail.get('id')) # An toàn hơn khi dùng .get()
        mention = channel.mention if channel else f"`#{utils.escape_markdown(detail.get('name', 'Unknown'))}`"
        top_text_lines.append(f"`#{rank}`. {mention} ({detail.get('message_count', 0):,} tin)")

    # --- Top Kênh Voice (có tin nhắn chat) ---
    top_voice_channels = sorted(
        [d for d in channel_details if d.get("processed") and d.get("type") == str(discord.ChannelType.voice) and d.get("message_count", 0) > 0],
        key=lambda d: d.get("message_count", 0),
        reverse=True
    )
    top_voice_lines = []
    if top_voice_channels:
        for rank, detail in enumerate(top_voice_channels[:5], 1):
            channel = guild.get_channel(detail.get('id'))
            mention = channel.mention if channel else f"`#{utils.escape_markdown(detail.get('name', 'Unknown'))}`"
            top_voice_lines.append(f"`#{rank}`. {mention} ({detail.get('message_count', 0):,} tin)")
    else:
        if any(d.get("type") == str(discord.ChannelType.voice) for d in channel_details):
            top_voice_lines.append("*Không tìm thấy tin nhắn chat trong kênh voice.*")
        else:
            top_voice_lines.append("*Không có kênh voice nào được quét.*")


    # --- Tạo Embed ---
    if not top_text_channels and not top_voice_channels:
        log.debug("Không có dữ liệu hoạt động kênh text/voice để tạo embed.")
        return None

    embed = discord.Embed(
        title=f"💬 Hoạt động Kênh",
        color=discord.Color.green()
    )

    embed.add_field(
        name="🔥 Top Kênh Text \"Nóng\"",
        value="\n".join(top_text_lines) if top_text_lines else "*Không có dữ liệu kênh text.*",
        inline=False
    )
    embed.add_field(
        name="🎤 Top Kênh Voice \"Nóng\" (Chat Text)",
        value="\n".join(top_voice_lines), # Sẽ hiển thị "Không tìm thấy..." nếu cần
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
        return None # Không có dữ liệu giờ

    local_offset_hours = utils.get_local_timezone_offset()
    timezone_str = f"UTC{local_offset_hours:+d}" if local_offset_hours is not None else "UTC"

    embed = discord.Embed(
        title=f"☀️🌙 \"Giờ Vàng\" của Server ({timezone_str})",
        description="*Khung giờ server và các kênh/luồng có nhiều tin nhắn nhất.*",
        color=discord.Color.gold()
    )

    # --- Tính Giờ Vàng Server ---
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

    server_golden_lines = []
    for rank, (start_hour, count) in enumerate(sorted_server_hours, 1):
        try:
            utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=start_hour, minute=0, second=0, microsecond=0)
            local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours))
            local_start_dt = utc_start_dt.astimezone(local_tz)
        except ValueError:
             log.warning(f"Không thể tạo datetime cho start_hour={start_hour} khi tính giờ vàng server.")
             continue
        except Exception as tz_convert_err:
             log.warning(f"Lỗi chuyển đổi timezone khi tính giờ vàng server: {tz_convert_err}")
             local_start_dt = utc_start_dt

        local_end_dt = local_start_dt + datetime.timedelta(hours=GOLDEN_HOUR_INTERVAL)
        time_str = f"{local_start_dt.strftime('%H:%M')} - {local_end_dt.strftime('%H:%M')}"
        server_golden_lines.append(f"**`#{rank}`**. **{time_str}**: {count:,} tin")
        if rank >= 3:
            break

    embed.add_field(
        name="🏆 Khung Giờ Vàng Toàn Server",
        value="\n".join(server_golden_lines) if server_golden_lines else "Không có dữ liệu.",
        inline=False
    )

    # --- Tính Giờ Vàng Kênh/Luồng ---
    location_hourly_activity = defaultdict(Counter)
    for loc_id, counts in channel_hourly_activity.items():
         if guild.get_channel_or_thread(loc_id):
             for hour, count in counts.items():
                 if isinstance(hour, int) and 0 <= hour <= 23:
                     location_hourly_activity[loc_id][hour] += count
                 else:
                     log.warning(f"Bỏ qua dữ liệu giờ không hợp lệ cho channel {loc_id}: hour={hour}")
    for loc_id, counts in thread_hourly_activity.items():
        if guild.get_channel_or_thread(loc_id):
            for hour, count in counts.items():
                if isinstance(hour, int) and 0 <= hour <= 23:
                    location_hourly_activity[loc_id][hour] += count
                else:
                     log.warning(f"Bỏ qua dữ liệu giờ không hợp lệ cho thread {loc_id}: hour={hour}")

    location_golden_hours = {} # {loc_id: (start_hour, count)}
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
            except ValueError:
                log.warning(f"Giờ vàng không hợp lệ ({best_start_hour}) được tính cho location {loc_id}, bỏ qua.")
            except Exception as e_loc_gold:
                 log.warning(f"Lỗi khi tính giờ vàng cho location {loc_id}: {e_loc_gold}")

    sorted_locations_by_gold = sorted(location_golden_hours.items(), key=lambda item: item[1][1], reverse=True)

    location_golden_lines = []
    locations_shown = 0
    for loc_id, (start_hour, count) in sorted_locations_by_gold:
        if locations_shown >= GOLDEN_HOUR_TOP_CHANNELS: break

        location_obj = guild.get_channel_or_thread(loc_id)
        if not location_obj: continue

        loc_mention = location_obj.mention
        loc_type_emoji = utils.get_channel_type_emoji(location_obj, bot)

        try:
             utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=start_hour, minute=0, second=0, microsecond=0)
             local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours))
             local_start_dt = utc_start_dt.astimezone(local_tz)
        except ValueError:
             log.warning(f"Không thể tạo datetime cho start_hour={start_hour} khi tính giờ vàng location {loc_id}.")
             continue
        except Exception as tz_convert_err_loc:
             log.warning(f"Lỗi chuyển đổi timezone khi tính giờ vàng location {loc_id}: {tz_convert_err_loc}")
             local_start_dt = utc_start_dt

        local_end_dt = local_start_dt + datetime.timedelta(hours=GOLDEN_HOUR_INTERVAL)
        time_str = f"{local_start_dt.strftime('%H:%M')}-{local_end_dt.strftime('%H:%M')}"

        location_golden_lines.append(f"{loc_type_emoji} {loc_mention}: **{time_str}** ({count:,} tin)")
        locations_shown += 1

    embed.add_field(
        name=f"🏅 Giờ Vàng Của Top {GOLDEN_HOUR_TOP_CHANNELS} Kênh/Luồng",
        value="\n".join(location_golden_lines) if location_golden_lines else "Không có dữ liệu.",
        inline=False
    )

    return embed

# --- END OF FILE reporting/embeds_guild.py ---