# --- START OF FILE reporting/embeds_dm.py ---
import discord
from discord.ext import commands
import datetime
import logging
from typing import Dict, Any, Optional, Union, Tuple, List, Set # Thêm Set
from collections import Counter, defaultdict # Thêm defaultdict
import asyncio # Thêm asyncio

import config
import utils

log = logging.getLogger(__name__)

# --- Constants ---
TOP_PERSONAL_ITEMS_LIMIT = 3 # Giới hạn top emoji/sticker cá nhân
PERSONAL_GOLDEN_HOUR_INTERVAL = 3 # Khoảng giờ vàng cá nhân

async def create_personal_activity_embed(
    member: discord.Member,
    scan_data: Dict[str, Any],
    bot: commands.Bot,
    ranking_data: Dict[str, Dict[int, int]] # Thêm ranking_data
) -> Optional[discord.Embed]:
    """Tạo Embed chính hiển thị hoạt động cá nhân của user (Nâng cấp)."""
    e = lambda name: utils.get_emoji(name, bot)
    user_id = member.id
    user_activity_data = scan_data.get("user_activity", {}).get(user_id)
    guild = member.guild # Lấy guild object

    if not user_activity_data:
        log.warning(f"Không tìm thấy dữ liệu hoạt động cho {member.display_name} ({user_id}) để tạo DM embed.")
        return None

    embed = discord.Embed(
        title=f"{e('user_activity')} Hoạt động của Bạn trên {member.guild.name}",
        color=member.color if member.color.value != 0 else discord.Color.blue()
    )
    if member.display_avatar:
        embed.set_thumbnail(url=member.display_avatar.url)

    # --- Field 1: Tin nhắn & Nội dung ---
    msg_count = user_activity_data.get('message_count', 0)
    msg_rank = ranking_data.get('messages', {}).get(user_id)
    msg_rank_str = f"(Hạng: **#{msg_rank}**)" if msg_rank else ""
    link_count = user_activity_data.get('link_count', 0)
    img_count = user_activity_data.get('image_count', 0)
    # Sửa: Lấy đúng counter tổng custom emoji content
    custom_emoji_total_count = scan_data.get("user_total_custom_emoji_content_counts", {}).get(user_id, 0)
    sticker_count = user_activity_data.get('sticker_count', 0)
    other_file_count = user_activity_data.get('other_file_count', 0) # Lấy số liệu file khác

    content_lines = [
        f"{e('stats')} Tổng tin nhắn: **{msg_count:,}** {msg_rank_str}".strip(),
        f"{e('link')} Links đã gửi: {link_count:,}",
        f"{e('image')} Ảnh đã gửi: {img_count:,}",
        f"{utils.get_emoji('mention', bot)} Emoji Server (Nội dung): {custom_emoji_total_count:,}",
        f"{e('sticker')} Stickers đã gửi: {sticker_count:,}",
        f"📎 Files khác: {other_file_count:,}" # Thêm dòng file khác
    ]
    # Bỏ '#' ở đầu field name
    embed.add_field(name="📜 Tin Nhắn & Nội Dung", value="\n".join(content_lines), inline=False)

    # --- Field 2: Tương tác ---
    reply_count = user_activity_data.get('reply_count', 0)
    mention_given = user_activity_data.get('mention_given_count', 0)
    mention_received = user_activity_data.get('mention_received_count', 0)
    reaction_received = user_activity_data.get('reaction_received_count', 0)
    reaction_given = user_activity_data.get('reaction_given_count', 0) # Lấy số reaction đã thả

    react_lines = []
    if config.ENABLE_REACTION_SCAN:
        react_lines.append(f"{e('reaction')} Reactions nhận (lọc): {reaction_received:,}")
        react_lines.append(f"{e('reaction')} Reactions đã thả (lọc): {reaction_given:,}") # Thêm dòng reaction đã thả

    interaction_lines = [
        f"{e('reply')} Trả lời đã gửi: {reply_count:,}",
        f"{e('mention')} Mentions đã gửi: {mention_given:,}",
        f"{e('mention')} Mentions nhận: {mention_received:,}",
        *react_lines # Chèn các dòng reaction nếu có
    ]
    # Bỏ '#' ở đầu field name
    embed.add_field(name="💬 Tương Tác", value="\n".join(interaction_lines).strip(), inline=False)

    # --- Field 3: Thời gian hoạt động ---
    first_seen = user_activity_data.get('first_seen')
    last_seen = user_activity_data.get('last_seen')
    activity_span_secs = user_activity_data.get('activity_span_seconds', 0)
    activity_span_str = utils.format_timedelta(datetime.timedelta(seconds=activity_span_secs)) if activity_span_secs > 0 else "N/A"

    time_lines = [
        f"{e('calendar')} HĐ đầu tiên: {utils.format_discord_time(first_seen, 'R') if first_seen else 'N/A'}",
        f"{e('calendar')} HĐ cuối cùng: {utils.format_discord_time(last_seen, 'R') if last_seen else 'N/A'}",
        f"{e('clock')} Khoảng TG hoạt động: **{activity_span_str}**"
    ]
    # Bỏ '#' ở đầu field name
    embed.add_field(name="⏳ Thời Gian Hoạt Động", value="\n".join(time_lines), inline=False)

    # --- Field 4: Phạm vi hoạt động (Top 3 Kênh) ---
    distinct_channels_count = len(user_activity_data.get('channels_messaged_in', set()))
    user_channel_msg_counts: Optional[Dict[int, int]] = scan_data.get('user_channel_message_counts', {}).get(user_id)

    scope_lines = [
        f"🗺️ Số kênh/luồng khác nhau đã nhắn: **{distinct_channels_count}**"
    ]

    if user_channel_msg_counts:
        sorted_channels = sorted(user_channel_msg_counts.items(), key=lambda item: item[1], reverse=True)[:3] # Lấy top 3
        if sorted_channels:
            scope_lines.append("📍 **Top Kênh/Luồng Hoạt Động:**")
            num_top_channels = len(sorted_channels)
            for i, (location_id, msg_count_in_loc) in enumerate(sorted_channels):
                channel_obj = guild.get_channel_or_thread(location_id)
                channel_mention = channel_obj.mention if channel_obj else f"`ID:{location_id}`"
                channel_name_str = f" (`#{utils.escape_markdown(channel_obj.name)}`)" if channel_obj else " (Không rõ/Đã xóa)"
                channel_type_emoji = utils.get_channel_type_emoji(channel_obj, bot) if channel_obj else "❓"
                branch = "└──" if (i == num_top_channels - 1) else "├──"
                scope_lines.append(f"   {branch} {channel_type_emoji} {channel_mention}{channel_name_str} ({msg_count_in_loc:,} tin)")
        else:
             scope_lines.append("📍 Top Kênh/Luồng Hoạt Động: *Chưa có dữ liệu*")
    else:
        scope_lines.append("📍 Top Kênh/Luồng Hoạt Động: *Chưa có dữ liệu*")

    # Bỏ '#' ở đầu field name
    embed.add_field(name="🎯 Phạm Vi Hoạt Động", value="\n".join(scope_lines), inline=False)

    # --- Field 5: Top Items Cá Nhân (Emoji & Sticker) ---
    top_items_lines = []
    # Top Custom Emojis
    user_custom_emoji_counts: Counter = scan_data.get("user_custom_emoji_content_counts", defaultdict(Counter)).get(user_id, Counter()) # Sửa lại để lấy Counter
    if user_custom_emoji_counts:
        sorted_emojis = user_custom_emoji_counts.most_common(TOP_PERSONAL_ITEMS_LIMIT)
        emoji_cache: Dict[int, discord.Emoji] = scan_data.get("server_emojis_cache", {})
        emoji_strs = []
        for emoji_id, count in sorted_emojis:
            emoji_obj = emoji_cache.get(emoji_id) or bot.get_emoji(emoji_id)
            if emoji_obj:
                emoji_strs.append(f"{str(emoji_obj)} ({count:,})")
            else:
                emoji_strs.append(f"`ID:{emoji_id}` ({count:,})")
        if emoji_strs:
            top_items_lines.append(f"{e('mention')} **Top Emoji Server:** " + " ".join(emoji_strs))

    # Top Stickers
    user_sticker_counts: Counter = scan_data.get("user_sticker_id_counts", defaultdict(Counter)).get(user_id, Counter()) # Sửa lại để lấy Counter
    if user_sticker_counts:
        sorted_stickers = user_sticker_counts.most_common(TOP_PERSONAL_ITEMS_LIMIT)
        # Fetch sticker names (cần hàm helper trong utils)
        sticker_ids_to_fetch = [int(sid) for sid, count in sorted_stickers if sid.isdigit()]
        sticker_name_cache: Dict[int, str] = {}
        if sticker_ids_to_fetch:
             # Giả sử utils có hàm _fetch_sticker_dict tương tự _fetch_user_dict
             if hasattr(utils, '_fetch_sticker_dict'):
                 sticker_name_cache = await utils._fetch_sticker_dict(sticker_ids_to_fetch, bot)
             else:
                 log.warning("Thiếu hàm utils._fetch_sticker_dict để lấy tên sticker.")

        sticker_strs = []
        for sticker_id_str, count in sorted_stickers:
            if sticker_id_str.isdigit():
                sticker_id = int(sticker_id_str)
                sticker_name = sticker_name_cache.get(sticker_id, "...") # Lấy tên từ cache
                sticker_strs.append(f"'{utils.escape_markdown(sticker_name)}' ({count:,})")
            else:
                 sticker_strs.append(f"`ID:{sticker_id_str}` ({count:,})")
        if sticker_strs:
             top_items_lines.append(f"{e('sticker')} **Top Stickers:** " + ", ".join(sticker_strs))

    if top_items_lines:
        # Bỏ '#' ở đầu field name
        embed.add_field(name=f"⭐ Top Items Cá Nhân", value="\n".join(top_items_lines), inline=False)

    # --- Field 6: Giờ Vàng Cá Nhân ---
    user_hourly_counter: Counter = scan_data.get("user_hourly_activity", defaultdict(Counter)).get(user_id) # Sửa lại để lấy Counter
    if user_hourly_counter:
        hourly_grouped = defaultdict(int)
        for hour, count in user_hourly_counter.items():
            start_hour = (hour // PERSONAL_GOLDEN_HOUR_INTERVAL) * PERSONAL_GOLDEN_HOUR_INTERVAL
            hourly_grouped[start_hour] += count

        if hourly_grouped:
             # Lấy timezone từ utils (giả sử đã thêm vào utils)
             timezone_str = "UTC" # Default
             local_offset_hours = 0
             if hasattr(utils, 'get_local_timezone_offset'): # Kiểm tra nếu hàm tồn tại
                 local_offset_hours = utils.get_local_timezone_offset()
                 timezone_str = f"UTC{local_offset_hours:+d}"
             elif hasattr(utils, 'local_timezone_offset_hours'): # Hoặc biến toàn cục
                 local_offset_hours = utils.local_timezone_offset_hours
                 timezone_str = f"UTC{local_offset_hours:+d}"

             best_start_hour, max_count = max(hourly_grouped.items(), key=lambda item: item[1])
             # Tính giờ địa phương
             try:
                 utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=best_start_hour, minute=0, second=0, microsecond=0)
                 local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours))
                 local_start_dt = utc_start_dt.astimezone(local_tz)
                 local_end_dt = local_start_dt + datetime.timedelta(hours=PERSONAL_GOLDEN_HOUR_INTERVAL)
                 time_str = f"{local_start_dt.strftime('%H:%M')} - {local_end_dt.strftime('%H:%M')}"
                 golden_hour_line = f"Khung giờ sôi nổi nhất ({timezone_str}): **{time_str}** ({max_count:,} tin)"
                 # Bỏ '#' ở đầu field name
                 embed.add_field(name="☀️🌙 Giờ Vàng Cá Nhân", value=golden_hour_line, inline=False)
             except Exception as gh_err:
                 log.warning(f"Lỗi tính giờ vàng cá nhân cho user {user_id}: {gh_err}")
                 # Bỏ '#' ở đầu field name
                 embed.add_field(name="☀️🌙 Giờ Vàng Cá Nhân", value="*Không thể xác định*", inline=False)
        else:
             # Bỏ '#' ở đầu field name
             embed.add_field(name="☀️🌙 Giờ Vàng Cá Nhân", value="*Chưa có dữ liệu*", inline=False)

    scan_end_time = scan_data.get("scan_end_time", datetime.datetime.now(datetime.timezone.utc))
    embed.set_footer(text=f"Dữ liệu quét từ {utils.format_discord_time(scan_end_time)}")
    return embed


async def create_achievements_embed(
    member: discord.Member,
    scan_data: Dict[str, Any], # Chỉ cần scan_data để lấy tracked roles
    bot: commands.Bot,
    ranking_data: Dict[str, Dict[int, int]] # Dùng ranking_data đã tính
) -> Optional[discord.Embed]:
    """Tạo Embed hiển thị thành tích và so sánh vị trí của user (Nâng cấp)."""
    e = lambda name: utils.get_emoji(name, bot)
    user_id = member.id
    has_achievements = False # Cờ để kiểm tra xem có thành tích nào không

    # Bỏ '#' ở đầu title
    embed = discord.Embed(
        title=f"{e('award')} Thành Tích & Vị Trí Của Bạn",
        description="*So sánh hoạt động của bạn với toàn server. Chỉ hiển thị nếu bạn lọt vào top.*",
        color=member.color if member.color.value != 0 else discord.Color.gold()
    )

    def add_rank_line(lines_list: list, display_name: str, rank_key: str):
        nonlocal has_achievements
        rank = ranking_data.get(rank_key, {}).get(user_id)
        if rank:
            lines_list.append(f"- {display_name}: **Hạng #{rank}**")
            has_achievements = True

    # === Field 1: BXH Hoạt Động & Tương Tác ===
    activity_ranks: List[str] = []
    add_rank_line(activity_ranks, "Gửi Tin Nhắn", "messages")
    add_rank_line(activity_ranks, "Nhận Reaction (lọc)", "reaction_received")
    # Thêm hạng Thả Reaction
    if config.ENABLE_REACTION_SCAN:
        add_rank_line(activity_ranks, "Thả Reaction (lọc)", "reaction_given") # Thêm dòng này
    add_rank_line(activity_ranks, "Trả Lời Tin Nhắn", "replies")
    add_rank_line(activity_ranks, "Được Nhắc Tên", "mention_received")
    add_rank_line(activity_ranks, "Hay Nhắc Tên", "mention_given")
    add_rank_line(activity_ranks, '"Người Đa Năng" (Nhiều kênh)', "distinct_channels")
    if activity_ranks:
        # Bỏ '#' ở đầu field name
        embed.add_field(name=f"{e('stats')} BXH Hoạt Động & Tương Tác", value="\n".join(activity_ranks), inline=False)

    # === Field 2: BXH Sáng Tạo Nội Dung ===
    content_ranks: List[str] = []
    add_rank_line(content_ranks, "Dùng Custom Emoji (Content)", "custom_emoji_content")
    add_rank_line(content_ranks, "Gửi Sticker", "stickers_sent")
    add_rank_line(content_ranks, "Gửi Link", "links_sent")
    add_rank_line(content_ranks, "Gửi Ảnh", "images_sent")
    add_rank_line(content_ranks, "Tạo Thread", "threads_created")
    if content_ranks:
        # Bỏ '#' ở đầu field name
        embed.add_field(name=f"{e('image')} BXH Sáng Tạo Nội Dung", value="\n".join(content_ranks), inline=False)

    # === Field 3: Danh Hiệu Đặc Biệt ===
    tracked_role_grants: Counter = scan_data.get("tracked_role_grant_counts", Counter()) # { (uid, rid): count }
    special_role_lines: List[str] = []
    guild = member.guild
    for tracked_role_id in config.TRACKED_ROLE_GRANT_IDS:
        grant_count = tracked_role_grants.get((user_id, tracked_role_id), 0)
        if grant_count > 0:
            role = guild.get_role(tracked_role_id)
            role_mention = role.mention if role else f"`ID: {tracked_role_id}`"
            role_name_fallback = f"'{role.name}'" if role else "(Unknown Role)"
            rank_key = f"tracked_role_{tracked_role_id}"
            rank = ranking_data.get(rank_key, {}).get(user_id)
            rank_str = f"(Hạng #{rank})" if rank else ""
            special_role_lines.append(f'- Đã nhận {role_mention}: **{grant_count}** lần {rank_str}'.strip())
            has_achievements = True
    if special_role_lines:
        # Bỏ '#' ở đầu field name
        embed.add_field(name=f"{e('crown')} Danh Hiệu Đặc Biệt", value="\n".join(special_role_lines), inline=False)


    # === Field 4: BXH Thời Gian & Tham Gia ===
    time_ranks: List[str] = []
    add_rank_line(time_ranks, "Thành viên Lâu Năm", "oldest_members")
    add_rank_line(time_ranks, "Hoạt Động Lâu Nhất (Span)", "activity_span")
    if member.premium_since:
        add_rank_line(time_ranks, "Booster Bền Bỉ", "booster_duration")
    if time_ranks:
        # Bỏ '#' ở đầu field name
        embed.add_field(name=f"{e('calendar')} BXH Thời Gian & Tham Gia", value="\n".join(time_ranks), inline=False)


    # === Xử lý trường hợp không có thành tích ===
    if not has_achievements:
        embed.description = "*Bạn chưa có thành tích nào nổi bật lọt vào top trong lần quét này. Hãy tiếp tục hoạt động nhé!*"
        embed.clear_fields()
        return embed

    return embed

# --- END OF FILE reporting/embeds_dm.py ---