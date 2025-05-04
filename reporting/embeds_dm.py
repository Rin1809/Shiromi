# --- START OF FILE reporting/embeds_dm.py ---
import discord
from discord.ext import commands
import datetime
import logging
from typing import Dict, Any, Optional, Union, Tuple, List
from collections import Counter

import config
import utils

log = logging.getLogger(__name__)

async def create_personal_activity_embed(
    member: discord.Member,
    scan_data: Dict[str, Any],
    bot: commands.Bot,
    ranking_data: Dict[str, Dict[int, int]] # Thêm ranking_data
) -> Optional[discord.Embed]:
    """Tạo Embed chính hiển thị hoạt động cá nhân của user."""
    e = lambda name: utils.get_emoji(name, bot)
    user_id = member.id
    user_activity_data = scan_data.get("user_activity", {}).get(user_id)

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
    # Lấy hạng từ ranking_data
    msg_rank = ranking_data.get('messages', {}).get(user_id)
    msg_rank_str = f"(Hạng: **#{msg_rank}**)" if msg_rank else "" # Chỉ hiển thị nếu có hạng
    link_count = user_activity_data.get('link_count', 0)
    img_count = user_activity_data.get('image_count', 0)
    # <<< FIX: Lấy đúng counter tổng custom emoji content của user >>>
    custom_emoji_total_count = scan_data.get("user_total_custom_emoji_content_counts", {}).get(user_id, 0)
    # <<< END FIX >>>
    sticker_count = user_activity_data.get('sticker_count', 0)
    other_file_count = user_activity_data.get('other_file_count', 0)

    content_lines = [
        f"{e('stats')} Tổng tin nhắn: **{msg_count:,}** {msg_rank_str}".strip(),
        f"{e('link')} Links đã gửi: {link_count:,}",
        f"{e('image')} Ảnh đã gửi: {img_count:,}",
        # <<< FIX: Sửa tên hiển thị và dùng dữ liệu đã fix >>>
        f"{utils.get_emoji('mention', bot)} Emoji Server (Nội dung): {custom_emoji_total_count:,}",
        # <<< END FIX >>>
        f"{e('sticker')} Stickers đã gửi: {sticker_count:,}",
        f"📎 Files khác: {other_file_count:,}"
    ]
    embed.add_field(name="📜 Tin Nhắn & Nội Dung", value="\n".join(content_lines), inline=False)

    # --- Field 2: Tương tác ---
    reply_count = user_activity_data.get('reply_count', 0)
    mention_given = user_activity_data.get('mention_given_count', 0)
    mention_received = user_activity_data.get('mention_received_count', 0)
    reaction_received = user_activity_data.get('reaction_received_count', 0)
    reaction_str = f"\n{e('reaction')} Reactions nhận: {reaction_received:,}" if config.ENABLE_REACTION_SCAN else ""

    interaction_lines = [
        f"{e('reply')} Trả lời đã gửi: {reply_count:,}",
        f"{e('mention')} Mentions đã gửi: {mention_given:,}",
        f"{e('mention')} Mentions nhận: {mention_received:,}{reaction_str}"
    ]
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
    embed.add_field(name="⏳ Thời Gian Hoạt Động", value="\n".join(time_lines), inline=False)

    # <<< FIX: Field 4: Phạm vi hoạt động (Thêm kênh hoạt động nhiều nhất) >>>
    distinct_channels_count = len(user_activity_data.get('channels_messaged_in', set())) # Lấy từ set
    most_active_data = scan_data.get('user_most_active_channel', {}).get(user_id) # Lấy dữ liệu đã tính

    scope_lines = [
        f"🗺️ Số kênh/luồng khác nhau đã nhắn: **{distinct_channels_count}**"
    ]

    if most_active_data:
        location_id, msg_count_in_loc = most_active_data
        guild = member.guild # Lấy guild từ member object
        channel_obj = guild.get_channel_or_thread(location_id) # Tìm kênh/luồng
        channel_mention = channel_obj.mention if channel_obj else f"`ID:{location_id}`"
        channel_name_str = f" (`#{utils.escape_markdown(channel_obj.name)}`)" if channel_obj else " (Không rõ/Đã xóa)"
        scope_lines.append(f"📍 Kênh hoạt động nhiều nhất: {channel_mention}{channel_name_str} ({msg_count_in_loc:,} tin)")
    else:
        scope_lines.append("📍 Kênh hoạt động nhiều nhất: N/A")

    embed.add_field(name="🎯 Phạm Vi Hoạt Động", value="\n".join(scope_lines), inline=False)
    # <<< END FIX >>>

    scan_end_time = scan_data.get("scan_end_time", datetime.datetime.now(datetime.timezone.utc))
    embed.set_footer(text=f"Dữ liệu quét từ {utils.format_discord_time(scan_end_time)}")
    return embed


async def create_achievements_embed(
    member: discord.Member,
    scan_data: Dict[str, Any], # Chỉ cần scan_data để lấy tracked roles
    bot: commands.Bot,
    ranking_data: Dict[str, Dict[int, int]] # Dùng ranking_data đã tính
) -> Optional[discord.Embed]:
    """Tạo Embed hiển thị thành tích và so sánh vị trí của user."""
    e = lambda name: utils.get_emoji(name, bot)
    user_id = member.id
    has_achievements = False # Cờ để kiểm tra xem có thành tích nào không

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
            has_achievements = True # Đánh dấu có ít nhất 1 thành tích

    # === Field 1: BXH Hoạt Động & Tương Tác ===
    activity_ranks: List[str] = []
    add_rank_line(activity_ranks, "Gửi Tin Nhắn", "messages")
    add_rank_line(activity_ranks, "Nhận Reaction", "reaction_received")
    add_rank_line(activity_ranks, "Trả Lời Tin Nhắn", "replies")
    add_rank_line(activity_ranks, "Được Nhắc Tên", "mention_received")
    add_rank_line(activity_ranks, "Hay Nhắc Tên", "mention_given")
    add_rank_line(activity_ranks, '"Người Đa Năng" (Nhiều kênh)', "distinct_channels")
    if activity_ranks:
        embed.add_field(name=f"{e('stats')} BXH Hoạt Động & Tương Tác", value="\n".join(activity_ranks), inline=False)

    # === Field 2: BXH Sáng Tạo Nội Dung ===
    content_ranks: List[str] = []
    add_rank_line(content_ranks, "Dùng Custom Emoji (Content)", "custom_emoji_content")
    add_rank_line(content_ranks, "Gửi Sticker", "stickers_sent")
    add_rank_line(content_ranks, "Gửi Link", "links_sent")
    add_rank_line(content_ranks, "Gửi Ảnh", "images_sent")
    add_rank_line(content_ranks, "Tạo Thread", "threads_created")
    if content_ranks:
        embed.add_field(name=f"{e('image')} BXH Sáng Tạo Nội Dung", value="\n".join(content_ranks), inline=False)

    # === Field 3: Danh Hiệu Đặc Biệt ===
    # Lấy dữ liệu grant từ scan_data
    tracked_role_grants: Counter = scan_data.get("tracked_role_grant_counts", Counter()) # { (uid, rid): count }
    special_role_lines: List[str] = []
    guild = member.guild

    # Lặp qua các role cần theo dõi trong config
    for tracked_role_id in config.TRACKED_ROLE_GRANT_IDS:
        # Lấy số lần user này nhận được role đó
        grant_count = tracked_role_grants.get((user_id, tracked_role_id), 0)
        if grant_count > 0:
            role = guild.get_role(tracked_role_id)
            # <<< FIX: Hiển thị mention của role >>>
            role_mention = role.mention if role else f"`ID: {tracked_role_id}`"
            role_name_fallback = f"'{role.name}'" if role else "(Unknown Role)"
            # <<< END FIX >>>
            rank_key = f"tracked_role_{tracked_role_id}"
            rank = ranking_data.get(rank_key, {}).get(user_id)
            rank_str = f"(Hạng #{rank})" if rank else ""
            # <<< FIX: Sử dụng mention >>>
            special_role_lines.append(f'- Đã nhận {role_mention}: **{grant_count}** lần {rank_str}'.strip())
            # <<< END FIX >>>
            has_achievements = True

    if special_role_lines:
        embed.add_field(name=f"{e('crown')} Danh Hiệu Đặc Biệt", value="\n".join(special_role_lines), inline=False)

    # === Field 4: BXH Thời Gian & Tham Gia ===
    time_ranks: List[str] = []
    add_rank_line(time_ranks, "Thành viên Lâu Năm", "oldest_members")
    add_rank_line(time_ranks, "Hoạt Động Lâu Nhất (Span)", "activity_span")
    if member.premium_since: # Chỉ hiển thị nếu đang boost
        add_rank_line(time_ranks, "Booster Bền Bỉ", "booster_duration")
    if time_ranks:
        embed.add_field(name=f"{e('calendar')} BXH Thời Gian & Tham Gia", value="\n".join(time_ranks), inline=False)

    # === Xử lý trường hợp không có thành tích ===
    if not has_achievements:
        embed.description = "*Bạn chưa có thành tích nào nổi bật lọt vào top trong lần quét này. Hãy tiếp tục hoạt động nhé!*"
        embed.clear_fields() # Xóa các field trống
        return embed # Vẫn trả về embed thông báo

    return embed

# --- END OF FILE reporting/embeds_dm.py ---