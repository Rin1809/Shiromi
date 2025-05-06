# --- START OF FILE reporting/embeds_dm.py ---
import discord
from discord.ext import commands
import datetime
import logging
from typing import Dict, Any, Optional, Union, Tuple, List, Set, Callable
from collections import Counter, defaultdict
import asyncio
import collections
import time
import config
import utils

log = logging.getLogger(__name__)

# --- Constants ---
DELAY_BETWEEN_USERS = 3.5
DELAY_BETWEEN_MESSAGES = 0.8
DELAY_BETWEEN_EMBEDS = 1.8
DELAY_ON_HTTP_ERROR = 5.0
DELAY_ON_FORBIDDEN = 1.0
DELAY_ON_UNKNOWN_ERROR = 3.0
DELAY_AFTER_FINAL_ITEM = 1.5
TOP_PERSONAL_ITEMS_LIMIT = 3
PERSONAL_GOLDEN_HOUR_INTERVAL = 3
PERSONAL_CHANNEL_CHART_LIMIT = 3

# --- Helper Function: Prepare Ranking Data ---
async def _prepare_ranking_data(
    scan_data: Dict[str, Any],
    guild: discord.Guild
) -> Dict[str, Dict[int, int]]:
    """Chuẩn bị dữ liệu xếp hạng cho người dùng."""
    rankings: Dict[str, Dict[int, int]] = {}
    e = lambda name: utils.get_emoji(name, scan_data["bot"])

    # Xác định User Admin cần lọc
    admin_ids_to_filter: Set[int] = set()
    try:
        admin_ids_to_filter.update(m.id for m in guild.members if m.guild_permissions.administrator)
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID:
            admin_ids_to_filter.add(config.ADMIN_USER_ID)
        log.debug(f"Admin IDs to filter from leaderboards: {admin_ids_to_filter}")
    except Exception as admin_err:
        log.error(f"Lỗi khi xác định admin IDs để lọc: {admin_err}")

    # Hàm Helper tính Rank từ Counter
    def get_ranks_from_counter(
        counter: Optional[Union[collections.Counter, Dict[Any, int]]],
        filter_admin: bool = True,
        min_value: int = 1
    ) -> Dict[int, int]:
        if not counter:
            return {}
        if not isinstance(counter, collections.Counter):
            counter = Counter(counter)

        ranks: Dict[int, int] = {}
        current_rank = 0
        sorted_items = counter.most_common()

        for key, count in sorted_items:
            user_id: Optional[int] = None
            if isinstance(key, int):
                user_id = key
            elif isinstance(key, str) and key.isdigit():
                user_id = int(key)
            else:
                continue # Bỏ qua key không hợp lệ

            if count < min_value:
                continue # Bỏ qua nếu giá trị quá thấp

            if filter_admin and user_id in admin_ids_to_filter:
                continue

            current_rank += 1
            ranks[user_id] = current_rank
        return ranks

    # Hàm Helper tính Rank từ List
    def get_ranks_from_list(data_list: List[Dict[str, Any]], id_key: str) -> Dict[int, int]:
        ranks: Dict[int, int] = {}
        for i, item in enumerate(data_list):
            user_id_any = item.get(id_key)
            user_id: Optional[int] = None
            if isinstance(user_id_any, int):
                user_id = user_id_any
            elif isinstance(user_id_any, str) and user_id_any.isdigit():
                user_id = int(user_id_any)

            if user_id is not None:
                ranks[user_id] = i + 1 # Rank bắt đầu từ 1
        return ranks

    # Hàm Helper tính Rank cho Tracked Roles
    def get_ranks_from_tracked_roles(
        tracked_counts: Optional[collections.Counter],
        role_id: int
    ) -> Dict[int, int]:
        if not isinstance(tracked_counts, collections.Counter):
            return {}

        role_specific_counter = Counter({
            uid: count
            for (uid, rid), count in tracked_counts.items()
            if rid == role_id and count > 0
        })
        # Dùng hàm get_ranks_from_counter (không lọc admin cho danh hiệu)
        return get_ranks_from_counter(role_specific_counter, filter_admin=False)

    log.debug(f"{e('loading')} Bắt đầu tính toán dữ liệu xếp hạng cho DM...")
    start_rank_time = time.monotonic()

    # --- Tính toán các bảng xếp hạng ---
    # Hoạt động & Tương tác
    rankings["messages"] = get_ranks_from_counter(scan_data.get("user_activity_message_counts"), filter_admin=True)
    rankings["reaction_received"] = get_ranks_from_counter(scan_data.get("user_reaction_received_counts"), filter_admin=False)
    rankings["replies"] = get_ranks_from_counter(scan_data.get("user_reply_counts"), filter_admin=True)
    rankings["mention_received"] = get_ranks_from_counter(scan_data.get("user_mention_received_counts"), filter_admin=False)
    rankings["mention_given"] = get_ranks_from_counter(scan_data.get("user_mention_given_counts"), filter_admin=True)
    rankings["distinct_channels"] = get_ranks_from_counter(scan_data.get("user_distinct_channel_counts"), filter_admin=True)
    rankings["reaction_given"] = get_ranks_from_counter(scan_data.get("user_reaction_given_counts"), filter_admin=True)

    # Sáng Tạo Nội Dung
    rankings["custom_emoji_content"] = get_ranks_from_counter(scan_data.get("user_total_custom_emoji_content_counts"), filter_admin=True)
    rankings["stickers_sent"] = get_ranks_from_counter(scan_data.get("user_sticker_counts"), filter_admin=True)
    rankings["links_sent"] = get_ranks_from_counter(scan_data.get("user_link_counts"), filter_admin=True)
    rankings["images_sent"] = get_ranks_from_counter(scan_data.get("user_image_counts"), filter_admin=True)
    rankings["threads_created"] = get_ranks_from_counter(scan_data.get("user_thread_creation_counts"), filter_admin=True)

    # BXH Danh hiệu đặc biệt
    tracked_grants = scan_data.get("tracked_role_grant_counts", Counter())
    for rid in config.TRACKED_ROLE_GRANT_IDS:
        rankings[f"tracked_role_{rid}"] = get_ranks_from_tracked_roles(tracked_grants, rid)

    # BXH Thời gian & Tham gia
    rankings["oldest_members"] = get_ranks_from_list(scan_data.get("oldest_members_data", []), 'id')

    # BXH Activity Span
    user_spans: List[Tuple[int, float]] = []
    for user_id, data in scan_data.get('user_activity', {}).items():
        span_seconds = data.get('activity_span_seconds', 0.0)
        if span_seconds > 0 and not data.get('is_bot', False):
            user_spans.append((user_id, span_seconds))
    user_spans.sort(key=lambda item: item[1], reverse=True)
    rankings["activity_span"] = {user_id: rank + 1 for rank, (user_id, span) in enumerate(user_spans)}

    # BXH Booster Duration
    boosters = scan_data.get("boosters", [])
    rankings["booster_duration"] = {m.id: rank + 1 for rank, m in enumerate(boosters)}

    end_rank_time = time.monotonic()
    log.debug(f"{e('success')} Hoàn thành tính toán dữ liệu xếp hạng ({len(rankings)} BXH) trong {end_rank_time - start_rank_time:.2f}s.")
    return rankings


# --- Embed Creation Functions ---

async def create_personal_activity_embed(
    member: discord.Member,
    scan_data: Dict[str, Any],
    bot: commands.Bot,
    ranking_data: Dict[str, Dict[int, int]]
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
    custom_emoji_total_count = scan_data.get("user_total_custom_emoji_content_counts", {}).get(user_id, 0)
    sticker_count = user_activity_data.get('sticker_count', 0)
    other_file_count = user_activity_data.get('other_file_count', 0)

    content_lines = [
        f"{e('stats')} Tổng tin nhắn: **{msg_count:,}** {msg_rank_str}".strip(),
        f"{e('link')} Links đã gửi: {link_count:,}",
        f"{e('image')} Ảnh đã gửi: {img_count:,}",
        f"{utils.get_emoji('mention', bot)} Emoji Server (Nội dung): {custom_emoji_total_count:,}",
        f"{e('sticker')} Stickers đã gửi: {sticker_count:,}",
        f"📎 Files khác: {other_file_count:,}"
    ]
    embed.add_field(name="📜 Tin Nhắn & Nội Dung", value="\n".join(content_lines), inline=False)

    # --- Tương tác ---
    reply_count = user_activity_data.get('reply_count', 0)
    mention_given = user_activity_data.get('mention_given_count', 0)
    mention_received = user_activity_data.get('mention_received_count', 0)
    reaction_received = user_activity_data.get('reaction_received_count', 0)
    reaction_given = user_activity_data.get('reaction_given_count', 0)

    react_lines = []
    if config.ENABLE_REACTION_SCAN:
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
            # Tạo biểu đồ
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
                max_chart_height=5, # Chart nhỏ trong DM
                bar_width=1,
                bar_spacing=1,
                chart_title=f"Top {PERSONAL_CHANNEL_CHART_LIMIT} Kênh/Luồng",
                show_legend=True
            )
            # Thêm chart vào đầu
            if bar_chart_str:
                scope_lines.insert(1, bar_chart_str)
                scope_lines.insert(2, "") # Thêm dòng trống sau chart
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
            start_hour = (hour // PERSONAL_GOLDEN_HOUR_INTERVAL) * PERSONAL_GOLDEN_HOUR_INTERVAL
            hourly_grouped[start_hour] += count

        if hourly_grouped:
            timezone_str = "UTC"
            local_offset_hours = utils.get_local_timezone_offset()
            timezone_str = f"UTC{local_offset_hours:+d}"

            best_start_hour, max_count = max(hourly_grouped.items(), key=lambda item: item[1])
            try:
                # Tính toán giờ địa phương
                utc_start_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=best_start_hour, minute=0, second=0, microsecond=0)
                local_tz = datetime.timezone(datetime.timedelta(hours=local_offset_hours))
                local_start_dt = utc_start_dt.astimezone(local_tz)
                local_end_dt = local_start_dt + datetime.timedelta(hours=PERSONAL_GOLDEN_HOUR_INTERVAL)
                time_str = f"{local_start_dt.strftime('%H:%M')} - {local_end_dt.strftime('%H:%M')}"
                golden_hour_line = f"Khung giờ sôi nổi nhất ({timezone_str}): **{time_str}** ({max_count:,} tin)"
                embed.add_field(name="☀️🌙 Giờ Vàng Cá Nhân", value=golden_hour_line, inline=False)
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
    ranking_data: Dict[str, Dict[int, int]]
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
    if config.ENABLE_REACTION_SCAN:
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
    for tracked_role_id in config.TRACKED_ROLE_GRANT_IDS:
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
        # Xóa các field đã thêm nếu không có thành tích nào
        embed.clear_fields()

    return embed


# --- Main Function: Send Personalized DM Reports ---

async def send_personalized_dm_reports(
    scan_data: Dict[str, Any],
    is_testing_mode: bool
):
    """Gửi báo cáo DM cá nhân hóa."""
    guild: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    recipient_role_id: Optional[int] = config.DM_REPORT_RECIPIENT_ROLE_ID
    thank_you_role_ids: Set[int] = config.BOOSTER_THANKYOU_ROLE_IDS
    admin_user_id: Optional[int] = config.ADMIN_USER_ID
    quy_toc_anh_mapping: Dict[str, str] = config.QUY_TOC_ANH_MAPPING
    final_dm_emoji: str = config.FINAL_DM_EMOJI

    is_test_mode = is_testing_mode
    log.debug(f"[DM Sender] Explicit is_testing_mode received = {is_test_mode}")

    # --- Lấy đối tượng admin (nếu test mode) ---
    admin_member: Optional[discord.Member] = None
    admin_dm_channel: Optional[discord.DMChannel] = None
    if is_test_mode:
        if not admin_user_id:
            log.error("Chế độ Test DM bật nhưng ADMIN_USER_ID chưa được cấu hình!")
            scan_data["scan_errors"].append("Test DM thất bại: Thiếu ADMIN_USER_ID.")
            return
        try:
            admin_member = await utils.fetch_user_data(guild, admin_user_id, bot_ref=bot)
            if not admin_member:
                log.error(f"Không tìm thấy Admin ({admin_user_id}) trong server để gửi Test DM.")
                scan_data["scan_errors"].append(f"Test DM thất bại: Không tìm thấy Admin ({admin_user_id}).")
                return
            if isinstance(admin_member, discord.Member): # Đảm bảo admin còn trong server
                admin_dm_channel = admin_member.dm_channel or await admin_member.create_dm()
            else: # Nếu admin không còn trong server
                 log.warning(f"Admin {admin_user_id} không còn trong server, không thể lấy DM channel.")
                 scan_data["scan_errors"].append(f"Test DM thất bại: Admin ({admin_user_id}) không còn trong server.")
                 return
        except discord.Forbidden:
            log.error(f"Không thể tạo DM channel cho Admin ({admin_user_id}). Bot bị chặn?")
            scan_data["scan_errors"].append(f"Test DM thất bại: Không thể tạo DM cho Admin ({admin_user_id}).")
            return
        except Exception as fetch_err:
             log.error(f"Lỗi khi fetch Admin ({admin_user_id}): {fetch_err}", exc_info=True)
             scan_data["scan_errors"].append(f"Test DM thất bại: Lỗi fetch Admin ({admin_user_id}).")
             return

    # --- Xác định danh sách thành viên cần xử lý ---
    members_to_process: List[discord.Member] = []
    process_description = ""
    if recipient_role_id:
        recipient_role = guild.get_role(recipient_role_id)
        if recipient_role:
            members_to_process = [m for m in guild.members if recipient_role in m.roles and not m.bot]
            process_description = f"thành viên có role '{recipient_role.name}'"
        else:
            log.error(f"Không tìm thấy role nhận DM với ID: {recipient_role_id}.")
            scan_data["scan_errors"].append(f"Không tìm thấy Role nhận DM ({recipient_role_id}).")
            if not is_test_mode: return
    else:
        if not is_test_mode:
            log.info("Không có ID role nhận DM được cấu hình, bỏ qua gửi DM.")
            return
        log.warning("Không có role nhận DM được cấu hình, Test Mode sẽ xử lý TẤT CẢ user (không phải bot).")
        members_to_process = [m for m in guild.members if not m.bot]
        process_description = "tất cả thành viên (không phải bot)"

    if not members_to_process:
        log.info(f"Không tìm thấy {process_description} để xử lý báo cáo DM.")
        return

    if is_test_mode:
        log.info(f"Chế độ Test: Sẽ tạo và gửi {len(members_to_process)} báo cáo của {process_description} đến Admin ({admin_member.display_name if admin_member else 'N/A'}).")
    else:
        log.info(f"Chuẩn bị gửi DM báo cáo cho {len(members_to_process)} {process_description}.")

    # --- Lấy Role Objects cho việc cảm ơn ---
    thank_you_roles: Set[discord.Role] = {
        guild.get_role(rid) for rid in thank_you_role_ids if guild.get_role(rid)
    }
    if thank_you_roles:
        log.info(f"Lời cảm ơn đặc biệt sẽ được thêm cho các role: {[r.name for r in thank_you_roles]}")

    # --- Chuẩn bị dữ liệu xếp hạng ---
    ranking_data = await _prepare_ranking_data(scan_data, guild)

    # --- Bắt đầu gửi DM ---
    sent_dm_count = 0
    failed_dm_count = 0
    processed_members_count = 0

    for member in members_to_process:
        processed_members_count += 1
        log.info(f"{e('loading')} ({processed_members_count}/{len(members_to_process)}) Đang tạo báo cáo cho {member.display_name} ({member.id})...")

        messages_to_send: List[str] = []
        embeds_to_send: List[discord.Embed] = []
        dm_successfully_sent = False

        # --- Xác định đích gửi DM ---
        target_dm_channel: Optional[Union[discord.DMChannel, Any]] = None
        target_description_log = ""
        is_sending_to_admin = False

        if is_test_mode:
            target_dm_channel = admin_dm_channel
            target_description_log = f"Admin ({admin_member.id if admin_member else 'N/A'})"
            is_sending_to_admin = True
            test_prefix = f"```---\n📝 Báo cáo Test cho: {member.display_name} ({member.id})\n---```\n"
            messages_to_send.append(test_prefix)
        else:
            try:
                target_dm_channel = member.dm_channel or await member.create_dm()
                target_description_log = f"User {member.id}"
            except discord.Forbidden:
                 log.warning(f"❌ Không thể tạo/lấy DM channel cho {member.display_name} ({member.id}). Bỏ qua.")
                 failed_dm_count += 1
                 await asyncio.sleep(DELAY_ON_FORBIDDEN)
                 continue # Sang user tiếp theo
            except Exception as dm_create_err:
                 log.error(f"❌ Lỗi khi tạo DM channel cho {member.display_name} ({member.id}): {dm_create_err}", exc_info=True)
                 failed_dm_count += 1
                 await asyncio.sleep(DELAY_ON_UNKNOWN_ERROR)
                 continue # Sang user tiếp theo

        if not target_dm_channel:
            log.error(f"Không thể xác định kênh DM đích cho {member.display_name}. Bỏ qua.")
            failed_dm_count +=1
            continue

        # --- Tạo nội dung báo cáo cho 'member' hiện tại ---
        try:
            user_has_thank_you_role = any(role in member.roles for role in thank_you_roles)

            # Lấy URL ảnh riêng
            personalized_image_url: Optional[str] = None
            if user_has_thank_you_role:
                personalized_image_url = quy_toc_anh_mapping.get(str(member.id))
                log.debug(f"Ảnh cá nhân cho {member.display_name}: {personalized_image_url or 'Không có'}")

            default_image_url = "https://cdn.discordapp.com/attachments/1141675354470223887/1368708955911753751/image.png?ex=6819350c&is=6817e38c&hm=2152f8ecd42616638d092986066d6123338aea5e8c485fc3153d52d2f9ede2d5&"
            image_to_send = personalized_image_url # Ưu tiên ảnh cá nhân

            # Tạo tin nhắn chào mừng/cảm ơn
            if user_has_thank_you_role:
                thank_you_title = f"💖 Cảm ơn bạn đã là một phần tuyệt vời của {guild.name}! 💖"
                thank_you_body = (
                    f"🎀 | Chào cậu, {member.mention},\n\n"
                    f"Thay mặt Rin - Misuzu và mọi người **{guild.name}**, bọn tớ xin gửi lời cảm ơn cậu vì đã **đóng góp/boost** cho server! ✨\n\n"
                    f"Sự đóng góp của cậu giúp server ngày càng phát triển và duy trì một môi trường tuyệt vời cho tất cả mọi người á.\n\n"
                    f"Dưới đây là một chút tổng kết về hoạt động của cậu trong thời gian vừa qua (có thể có một chút sai số). Mong rằng cậu sẽ tiếp tục đồng hành cùng bọn tớ!\n\n"
                    f"Mỗi Member sau khi xác thực role [🔭 | Cư Dân ᓚᘏᗢ] và bật nhận tin nhắn từ người lạ sẽ đều nhận được bức thư này...\n\n"
                    f"Nhưng bức thư đây là dành riêng cho các [Quý tộc (Server Booster)🌠💫] | [| Người đóng góp (quý tộc-)] á\n\n"
                    f"*Một lần nữa, cảm ơn cậu nhé ! 本当にありがとうございます！！*\n\n"
                    f"Tớ là {config.BOT_NAME} | (Bot của Rin, thay mặt cho Rin gửi lời!)\n\n"
                    f"# ᓚᘏᗢ"
                )
                messages_to_send.append(thank_you_title + "\n\n" + thank_you_body)
                # Chỉ gửi ảnh mặc định nếu KHÔNG CÓ ảnh cá nhân
                if not image_to_send:
                    image_to_send = default_image_url
            else:
                 greeting_msg = (
                     f"📊 Chào cậu {member.mention},\n\n"
                     f"Thay mặt Rin - Misuzu và mọi người **{guild.name}**, bọn tớ xin gửi lời cảm ơn cậu vì đã có mặt và hoạt động trong server của bọn tớ vào thời gian qua!\n\n"
                     f"Dưới đây là một chút tổng kết về hoạt động của cậu trong thời gian vừa qua (có thể có một chút sai số). Mong rằng cậu sẽ tiếp tục đồng hành cùng bọn tớ!\n\n"
                     f"Mỗi Member sau khi xác thực role [🔭 | Cư Dân ᓚᘏᗢ] và bật nhận tin nhắn từ người lạ sẽ đều nhận được bức thư này...\n\n"
                     f"*Một lần nữa, cảm ơn cậu nhé ! 本当にありがとうございます！！*\n\n"
                     f"Tớ là {config.BOT_NAME} | (Bot của Rin, thay mặt cho Rin gửi lời!)\n\n"
                     f"# ᓚᘏᗢ"
                 )
                 messages_to_send.append(greeting_msg)
                 # Người thường luôn nhận ảnh mặc định (nếu có)
                 image_to_send = default_image_url

            # Thêm URL ảnh (cá nhân hoặc mặc định)
            if image_to_send:
                messages_to_send.append(image_to_send)

            # --- Tạo Embeds ---
            personal_activity_embed = await create_personal_activity_embed(member, scan_data, bot, ranking_data)
            if personal_activity_embed:
                embeds_to_send.append(personal_activity_embed)
            else:
                log.warning(f"Không thể tạo personal_activity_embed cho {member.display_name}")

            achievements_embed = await create_achievements_embed(member, scan_data, bot, ranking_data)
            if achievements_embed:
                embeds_to_send.append(achievements_embed)
            else:
                log.warning(f"Không thể tạo achievements_embed cho {member.display_name}")

            # Thêm tin nhắn kết thúc
            final_message = f"Đây là báo cáo tự động được tạo bởi {config.BOT_NAME}. Báo cáo này chỉ dành cho cậu. Chúc cậu một ngày vui vẻ! 🎉"
            messages_to_send.append(final_message)

            # --- Gửi DM ---
            if not embeds_to_send and not messages_to_send:
                log.warning(f"Không có nội dung DM để gửi cho {member.display_name}.")
                failed_dm_count += 1
                continue # Bỏ qua user này

            try:
                # Gửi tin nhắn text trước
                for msg_content in messages_to_send:
                    if msg_content and target_dm_channel:
                            await target_dm_channel.send(content=msg_content)
                            await asyncio.sleep(DELAY_BETWEEN_MESSAGES)
                    elif not target_dm_channel:
                            log.warning(f"Target DM channel không còn hợp lệ khi gửi message cho {target_description_log}")
                            raise Exception("Target DM channel became invalid")

                # Gửi embeds sau
                for embed in embeds_to_send:
                    if isinstance(embed, discord.Embed) and target_dm_channel:
                            await target_dm_channel.send(embed=embed)
                            await asyncio.sleep(DELAY_BETWEEN_EMBEDS)
                    elif not target_dm_channel:
                            log.warning(f"Target DM channel không còn hợp lệ khi gửi embed cho {target_description_log}")
                            raise Exception("Target DM channel became invalid")

                # Gửi emoji cuối cùng
                if final_dm_emoji and target_dm_channel:
                    try:
                        log.debug(f"Đang gửi emoji cuối DM '{final_dm_emoji}' đến {target_description_log}...")
                        await target_dm_channel.send(final_dm_emoji)
                        await asyncio.sleep(DELAY_AFTER_FINAL_ITEM)
                    except discord.Forbidden:
                        log.warning(f"  -> Không thể gửi emoji cuối DM đến {target_description_log}: Bot bị chặn?")
                    except discord.HTTPException as emoji_err:
                        log.warning(f"  -> Lỗi HTTP {emoji_err.status} khi gửi emoji cuối DM đến {target_description_log}: {emoji_err.text}")
                    except Exception as emoji_e:
                        log.warning(f"  -> Lỗi không xác định khi gửi emoji cuối DM đến {target_description_log}: {emoji_e}")

                sent_dm_count += 1
                dm_successfully_sent = True
                log.info(f"✅ Gửi báo cáo của {member.display_name} ({member.id}) thành công đến {target_description_log}")

            except discord.Forbidden:
                log.warning(f"❌ Không thể gửi DM đến {target_description_log} (cho báo cáo của {member.id}): User/Admin đã chặn DM hoặc bot.")
                failed_dm_count += 1
                dm_successfully_sent = False
                await asyncio.sleep(DELAY_ON_FORBIDDEN)
                if is_test_mode:
                    log.error("LỖI NGHIÊM TRỌNG: Không thể gửi Test DM đến Admin. Dừng gửi DM.")
                    scan_data["scan_errors"].append("Test DM thất bại: Không thể gửi DM đến Admin (Forbidden).")
                    return # Dừng hẳn hàm
                target_dm_channel = None # Đánh dấu channel không hợp lệ
            except discord.HTTPException as dm_http_err:
                log.error(f"❌ Lỗi HTTP {dm_http_err.status} khi gửi DM đến {target_description_log} (cho báo cáo của {member.id}): {dm_http_err.text}")
                failed_dm_count += 1
                dm_successfully_sent = False
                await asyncio.sleep(DELAY_ON_HTTP_ERROR)
                if is_test_mode and dm_http_err.status != 429: # Cho phép retry nếu chỉ là rate limit
                     log.error("LỖI NGHIÊM TRỌNG: Lỗi HTTP khi gửi Test DM đến Admin. Dừng gửi DM.")
                     scan_data["scan_errors"].append(f"Test DM thất bại: Lỗi HTTP {dm_http_err.status} khi gửi đến Admin.")
                     return
                target_dm_channel = None
            except Exception as dm_err:
                log.error(f"❌ Lỗi không xác định khi gửi DM đến {target_description_log} (cho báo cáo của {member.id}): {dm_err}", exc_info=True)
                failed_dm_count += 1
                dm_successfully_sent = False
                await asyncio.sleep(DELAY_ON_UNKNOWN_ERROR)
                if is_test_mode:
                    log.error("LỖI NGHIÊM TRỌNG: Lỗi không xác định khi gửi Test DM đến Admin. Dừng gửi DM.")
                    scan_data["scan_errors"].append("Test DM thất bại: Lỗi không xác định khi gửi đến Admin.")
                    return
                target_dm_channel = None

            # Chỉ delay giữa các user nếu DM trước đó thành công hoặc không phải lỗi nghiêm trọng
            if dm_successfully_sent or not is_test_mode:
                await asyncio.sleep(DELAY_BETWEEN_USERS)

        except Exception as user_proc_err:
            log.error(f"Lỗi nghiêm trọng khi xử lý dữ liệu DM cho {member.display_name} ({member.id}): {user_proc_err}", exc_info=True)
            failed_dm_count += 1
            await asyncio.sleep(DELAY_ON_UNKNOWN_ERROR)

    # --- Log kết thúc ---
    log.info(f"--- {e('success')} Hoàn tất gửi DM báo cáo ---")
    mode_str = "Test Mode (gửi đến Admin)" if is_test_mode else "Normal Mode"
    log.info(f"Chế độ: {mode_str}")
    log.info(f"Tổng cộng: {sent_dm_count} thành công, {failed_dm_count} thất bại.")
    if failed_dm_count > 0:
        scan_data["scan_errors"].append(f"Gửi DM ({mode_str}) thất bại cho {failed_dm_count} báo cáo.")
# --- END OF FILE reporting/embeds_dm.py ---