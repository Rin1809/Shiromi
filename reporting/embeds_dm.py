# --- START OF FILE reporting/embeds_dm.py ---
import discord
from discord.ext import commands
import datetime
import logging
from typing import Dict, Any, Optional, Union, Tuple, List, Set, Callable
from collections import Counter, defaultdict
import collections
import config
import utils 

log = logging.getLogger(__name__)




# --- Constants cho việc tạo Embed (Nếu có, đặt ở đây) ---
TOP_PERSONAL_ITEMS_LIMIT = 3
PERSONAL_GOLDEN_HOUR_INTERVAL = 3
PERSONAL_CHANNEL_CHART_LIMIT = 3

# --- Embed Creation Functions ---

async def create_personal_activity_embed(
    member: discord.Member,
    scan_data: Dict[str, Any],
    bot: commands.Bot,
    ranking_data: Dict[str, Dict[int, int]] # Ranking data được truyền vào
) -> Optional[discord.Embed]:
    """Tạo Embed chính hiển thị hoạt động cá nhân của user."""
    e = lambda name: utils.get_emoji(name, bot)
    user_id = member.id
    user_activity_data = scan_data.get("user_activity", {}).get(user_id)
    guild = member.guild

    if not user_activity_data:
        return None

    embed = discord.Embed(
        title=f"{e('user_activity')} Hoạt động của Bạn trên {member.guild.name}",
        color=member.color if member.color.value != 0 else discord.Color.blue()
    )
    if member.display_avatar:
        embed.set_thumbnail(url=member.display_avatar.url)

    # --- Tin nhắn & Nội dung ---
    msg_count = user_activity_data.get('message_count', 0)
    msg_rank = ranking_data.get('messages', {}).get(user_id)
    msg_rank_str = f"(Hạng: **#{msg_rank}**)" if msg_rank else ""
    link_count = user_activity_data.get('link_count', 0)
    img_count = user_activity_data.get('image_count', 0)
    # Lấy tổng emoji custom từ scan_data (được tính ở data_processing)
    custom_emoji_total_count = scan_data.get("user_total_custom_emoji_content_counts", {}).get(user_id, 0)
    sticker_count = user_activity_data.get('sticker_count', 0)
    other_file_count = user_activity_data.get('other_file_count', 0)

    content_lines = [
        f"{e('stats')} Tổng tin nhắn: **{msg_count:,}** {msg_rank_str}".strip(),
        f"{e('link')} Links đã gửi: {link_count:,}",
        f"{e('image')} Ảnh đã gửi: {img_count:,}",
        f"{utils.get_emoji('mention', bot)} Emoji Server (Nội dung): {custom_emoji_total_count:,}", # Sử dụng biến đã lấy
        f"{e('sticker')} Stickers đã gửi: {sticker_count:,}",
        f"📎 Files khác: {other_file_count:,}"
    ]
    embed.add_field(name="📜 Tin Nhắn & Nội Dung", value="\n".join(content_lines), inline=False)

    # --- Tương tác ---
    reply_count = user_activity_data.get('reply_count', 0)
    mention_given = user_activity_data.get('mention_given_count', 0)
    mention_received = user_activity_data.get('mention_received_count', 0)
    # Lấy reaction counts từ user_activity (đã được cập nhật trong scan_channels)
    reaction_received = user_activity_data.get('reaction_received_count', 0)
    reaction_given = user_activity_data.get('reaction_given_count', 0)

    react_lines = []
    # Kiểm tra xem config có bật reaction scan không TRƯỚC KHI hiển thị
    if scan_data.get("can_scan_reactions", False): # Lấy cờ từ scan_data
        react_lines.append(f"{e('reaction')} Reactions nhận (lọc): {reaction_received:,}")
        react_lines.append(f"{e('reaction')} Reactions đã thả (lọc): {reaction_given:,}")

    interaction_lines = [
        f"{e('reply')} Trả lời đã gửi: {reply_count:,}",
        f"{e('mention')} Mentions đã gửi: {mention_given:,}",
        f"{e('mention')} Mentions nhận: {mention_received:,}",
        *react_lines # Thêm dòng reactions nếu có
    ]
    # Loại bỏ các dòng trống nếu reaction scan tắt
    interaction_lines_filtered = [line for line in interaction_lines if line.strip()]
    embed.add_field(name="💬 Tương Tác", value="\n".join(interaction_lines_filtered), inline=False)


    # --- Thời gian hoạt động ---
    first_seen = user_activity_data.get('first_seen')
    last_seen = user_activity_data.get('last_seen')
    activity_span_secs = user_activity_data.get('activity_span_seconds', 0)
    activity_span_str = utils.format_timedelta(datetime.timedelta(seconds=activity_span_secs)) if activity_span_secs > 0 else "N/A"

    time_lines = [
        f"{e('calendar')} HĐ đầu tiên: {utils.format_discord_time(first_seen, 'R') if first_seen else 'N/A'}",
        f"{e('calendar')} HĐ cuối cùng: {utils.format_discord_time(last_seen, 'R') if last_seen else 'N/A'}",
        f"{e('clock')} Khoảng TG hoạt động: **{activity_span_str}**"
    ]
    embed.add_field(name="⏳ Thời Gian Hoạt Động", value="\n".join(time_lines), inline=False)

    # --- Phạm vi hoạt động (Kèm Biểu Đồ) ---
    distinct_channels_count = len(user_activity_data.get('channels_messaged_in', set()))
    user_channel_msg_counts: Optional[Dict[int, int]] = scan_data.get('user_channel_message_counts', {}).get(user_id)

    scope_lines = [f"🗺️ Số kênh/luồng khác nhau đã nhắn: **{distinct_channels_count}**"]
    bar_chart_str = ""

    if user_channel_msg_counts:
        sorted_channels = sorted(user_channel_msg_counts.items(), key=lambda item: item[1], reverse=True)
        if sorted_channels:
            data_for_chart = sorted_channels[:PERSONAL_CHANNEL_CHART_LIMIT]

            async def format_location_key(location_id):
                channel_obj = guild.get_channel_or_thread(location_id)
                if channel_obj:
                    channel_type_emoji = utils.get_channel_type_emoji(channel_obj, bot)
                    name = utils.escape_markdown(channel_obj.name)
                    max_len = 15
                    name_display = (name[:max_len] + '…') if len(name) > max_len else name
                    return f"{channel_type_emoji} {name_display}"
                return f"ID:{location_id}"

            bar_chart_str = await utils.create_vertical_text_bar_chart(
                sorted_data=data_for_chart,
                key_formatter=format_location_key,
                top_n=PERSONAL_CHANNEL_CHART_LIMIT,
                max_chart_height=5, bar_width=1, bar_spacing=1,
                chart_title=f"Top {PERSONAL_CHANNEL_CHART_LIMIT} Kênh/Luồng",
                show_legend=True
            )
            if bar_chart_str:
                scope_lines.insert(1, bar_chart_str)
                scope_lines.insert(2, "")
        else:
             scope_lines.append("📍 Top Kênh/Luồng Hoạt Động: *Chưa có dữ liệu*")
    else:
        scope_lines.append("📍 Top Kênh/Luồng Hoạt Động: *Chưa có dữ liệu*")

    scope_field_value = "\n".join(scope_lines)
    if len(scope_field_value) > 1024:
        scope_field_value = scope_field_value[:1020] + "\n[...]"
    embed.add_field(name="🎯 Phạm Vi Hoạt Động", value=scope_field_value, inline=False)

    # --- Top Items Cá Nhân ---
    top_items_lines = []
    # Top Emoji
    user_custom_emoji_counts: Counter = scan_data.get("user_custom_emoji_content_counts", defaultdict(Counter)).get(user_id, Counter())
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

    # Top Sticker
    user_sticker_counts: Counter = scan_data.get("user_sticker_id_counts", defaultdict(Counter)).get(user_id, Counter())
    if user_sticker_counts:
        sorted_stickers = user_sticker_counts.most_common(TOP_PERSONAL_ITEMS_LIMIT)
        sticker_ids_to_fetch = [int(sid) for sid, count in sorted_stickers if sid.isdigit()]
        sticker_name_cache: Dict[int, str] = {}
        if sticker_ids_to_fetch:
            sticker_name_cache = await utils._fetch_sticker_dict(sticker_ids_to_fetch, bot)
        sticker_strs = []
        for sticker_id_str, count in sorted_stickers:
            if sticker_id_str.isdigit():
                sticker_id = int(sticker_id_str)
                sticker_name = sticker_name_cache.get(sticker_id, "...")
                sticker_strs.append(f"'{utils.escape_markdown(sticker_name)}' ({count:,})")
            else:
                sticker_strs.append(f"`ID:{sticker_id_str}` ({count:,})")
        if sticker_strs:
            top_items_lines.append(f"{e('sticker')} **Top Stickers:** " + ", ".join(sticker_strs))

    if top_items_lines:
        embed.add_field(
            name=f"⭐ Top Items Cá Nhân ({TOP_PERSONAL_ITEMS_LIMIT})",
            value="\n".join(top_items_lines),
            inline=False
        )

    # --- Giờ Vàng Cá Nhân ---
    user_hourly_counter: Counter = scan_data.get("user_hourly_activity", defaultdict(Counter)).get(user_id)
    if user_hourly_counter:
        hourly_grouped = defaultdict(int)
        for hour, count in user_hourly_counter.items():
             if isinstance(hour, int) and 0 <= hour <= 23: # Thêm kiểm tra hour hợp lệ
                start_hour = (hour // PERSONAL_GOLDEN_HOUR_INTERVAL) * PERSONAL_GOLDEN_HOUR_INTERVAL
                hourly_grouped[start_hour] += count
             else:
                log.warning(f"Bỏ qua dữ liệu giờ không hợp lệ cho user {user_id}: hour={hour}")

        if hourly_grouped:
            timezone_str = "UTC"
            local_offset_hours = utils.get_local_timezone_offset()
            if local_offset_hours is not None: # Kiểm tra None
                timezone_str = f"UTC{local_offset_hours:+d}"

            try: # Thêm try-except cho phần tính toán giờ
                best_start_hour, max_count = max(hourly_grouped.items(), key=lambda item: item[1])
                utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=best_start_hour, minute=0, second=0, microsecond=0)
                local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours)) if local_offset_hours is not None else datetime.timezone.utc
                local_start_dt = utc_start_dt.astimezone(local_tz)
                local_end_dt = local_start_dt + datetime.timedelta(hours=PERSONAL_GOLDEN_HOUR_INTERVAL)
                time_str = f"{local_start_dt.strftime('%H:%M')} - {local_end_dt.strftime('%H:%M')}"
                golden_hour_line = f"Khung giờ sôi nổi nhất ({timezone_str}): **{time_str}** ({max_count:,} tin)"
                embed.add_field(name="☀️🌙 Giờ Vàng Cá Nhân", value=golden_hour_line, inline=False)
            except ValueError as ve: # Bắt lỗi ValueError nếu giờ không hợp lệ
                log.warning(f"Lỗi giá trị giờ khi tính giờ vàng cá nhân cho {user_id} (giờ={best_start_hour}): {ve}")
                embed.add_field(name="☀️🌙 Giờ Vàng Cá Nhân", value="*Không thể xác định (lỗi giờ)*", inline=False)
            except Exception as gh_err:
                log.warning(f"Lỗi tính giờ vàng cá nhân cho {user_id}: {gh_err}")
                embed.add_field(name="☀️🌙 Giờ Vàng Cá Nhân", value="*Không thể xác định*", inline=False)
        else:
             embed.add_field(name="☀️🌙 Giờ Vàng Cá Nhân", value="*Chưa có dữ liệu*", inline=False)
    else:
         embed.add_field(name="☀️🌙 Giờ Vàng Cá Nhân", value="*Chưa có dữ liệu*", inline=False)


    scan_end_time = scan_data.get("scan_end_time", datetime.datetime.now(datetime.timezone.utc))
    embed.set_footer(text=f"Dữ liệu quét từ {utils.format_discord_time(scan_end_time)}")
    return embed


async def create_achievements_embed(
    member: discord.Member,
    scan_data: Dict[str, Any],
    bot: commands.Bot,
    ranking_data: Dict[str, Dict[int, int]] # Ranking data được truyền vào
) -> Optional[discord.Embed]:
    """Tạo Embed hiển thị thành tích và so sánh vị trí của user."""
    e = lambda name: utils.get_emoji(name, bot)
    user_id = member.id
    has_achievements = False

    embed = discord.Embed(
        title=f"{e('award')} Thành Tích & Vị Trí Của Bạn",
        description="*So sánh hoạt động của bạn với toàn server. Chỉ hiển thị nếu bạn lọt vào top.*",
        color=member.color if member.color.value != 0 else discord.Color.gold()
    )

    def add_rank_line(lines_list: list, display_name: str, rank_key: str):
        """Thêm dòng xếp hạng vào danh sách nếu user có rank."""
        nonlocal has_achievements
        rank = ranking_data.get(rank_key, {}).get(user_id)
        if rank:
            lines_list.append(f"- {display_name}: **Hạng #{rank}**")
            has_achievements = True

    # --- BXH Hoạt Động & Tương Tác ---
    activity_ranks: List[str] = []
    add_rank_line(activity_ranks, "Gửi Tin Nhắn", "messages")
    add_rank_line(activity_ranks, "Nhận Reaction (lọc)", "reaction_received")
    # Chỉ thêm dòng rank reaction_given nếu scan bật
    if scan_data.get("can_scan_reactions", False):
        add_rank_line(activity_ranks, "Thả Reaction (lọc)", "reaction_given")
    add_rank_line(activity_ranks, "Trả Lời Tin Nhắn", "replies")
    add_rank_line(activity_ranks, "Được Nhắc Tên", "mention_received")
    add_rank_line(activity_ranks, "Hay Nhắc Tên", "mention_given")
    add_rank_line(activity_ranks, '"Người Đa Năng" (Nhiều kênh)', "distinct_channels")
    if activity_ranks:
        embed.add_field(
            name=f"{e('stats')} BXH Hoạt Động & Tương Tác",
            value="\n".join(activity_ranks),
            inline=False
        )

    # --- BXH Sáng Tạo Nội Dung ---
    content_ranks: List[str] = []
    add_rank_line(content_ranks, "Dùng Custom Emoji (Content)", "custom_emoji_content")
    add_rank_line(content_ranks, "Gửi Sticker", "stickers_sent")
    add_rank_line(content_ranks, "Gửi Link", "links_sent")
    add_rank_line(content_ranks, "Gửi Ảnh", "images_sent")
    add_rank_line(content_ranks, "Tạo Thread", "threads_created")
    if content_ranks:
        embed.add_field(
            name=f"{e('image')} BXH Sáng Tạo Nội Dung",
            value="\n".join(content_ranks),
            inline=False
        )

    # --- Danh Hiệu Đặc Biệt ---
    tracked_role_grants: Counter = scan_data.get("tracked_role_grant_counts", Counter())
    special_role_lines: List[str] = []
    guild = member.guild
    # Lấy TRACKED_ROLE_GRANT_IDS từ config (import config ở đầu file này)
    tracked_role_ids_from_config = getattr(config, 'TRACKED_ROLE_GRANT_IDS', set())
    for tracked_role_id in tracked_role_ids_from_config:
        grant_count = tracked_role_grants.get((user_id, tracked_role_id), 0)
        if grant_count > 0:
            role = guild.get_role(tracked_role_id)
            role_mention = role.mention if role else f"`ID: {tracked_role_id}`"
            rank_key = f"tracked_role_{tracked_role_id}"
            rank = ranking_data.get(rank_key, {}).get(user_id)
            rank_str = f"(Hạng #{rank})" if rank else ""
            line = f'- Đã nhận {role_mention}: **{grant_count}** lần {rank_str}'.strip()
            special_role_lines.append(line)
            has_achievements = True
    if special_role_lines:
        embed.add_field(
            name=f"{e('crown')} Danh Hiệu Đặc Biệt",
            value="\n".join(special_role_lines),
            inline=False
        )

    # --- BXH Thời Gian & Tham Gia ---
    time_ranks: List[str] = []
    add_rank_line(time_ranks, "Thành viên Lâu Năm", "oldest_members")
    add_rank_line(time_ranks, "Hoạt Động Lâu Nhất (Span)", "activity_span")
    if member.premium_since:
        add_rank_line(time_ranks, "Booster Bền Bỉ", "booster_duration")
    if time_ranks:
        embed.add_field(
            name=f"{e('calendar')} BXH Thời Gian & Tham Gia",
            value="\n".join(time_ranks),
            inline=False
        )

    # --- Không có thành tích ---
    if not has_achievements:
        embed.description = "*Bạn chưa có thành tích nào nổi bật lọt vào top trong lần quét này. Hãy tiếp tục hoạt động nhé!*"
        embed.clear_fields() # Xóa các field nếu không có gì để hiển thị

    return embed

# --- END OF FILE reporting/embeds_dm.py ---