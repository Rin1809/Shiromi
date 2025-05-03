# --- START OF FILE reporting/csv_writer.py ---
import discord
import datetime
import csv
import io
import json
import logging
import collections
import time
import re
from typing import List, Dict, Any, Optional, Union

# Sử dụng relative import để hoạt động tốt trong package
try:
    from .. import utils
except ImportError:
    # Fallback cho trường hợp chạy file riêng lẻ (ít phổ biến hơn)
    import utils

log = logging.getLogger(__name__)

# --- Hàm Helper Ghi CSV ---
async def _write_csv_to_list(
    filename: str,
    headers: List[str],
    data_rows: List[List[Any]],
    files_list_ref: List[discord.File]
):
    """
    Ghi dữ liệu vào một file CSV trong bộ nhớ (StringIO),
    sau đó chuyển thành BytesIO với mã hóa UTF-8 (có BOM)
    và thêm vào danh sách `files_list_ref` dưới dạng discord.File.
    """
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)

    try:
        writer.writerow(headers)
        for row in data_rows:
            # Đảm bảo mọi ô đều được chuẩn hóa trước khi ghi
            sanitized_row = [utils.sanitize_for_csv(cell) for cell in row]
            writer.writerow(sanitized_row)
    except Exception as csv_write_err:
        log.error(f"Lỗi nghiêm trọng khi ghi dữ liệu vào CSV '{filename}': {csv_write_err}", exc_info=True)
        # Cố gắng ghi một dòng báo lỗi vào file CSV nếu có thể
        try:
            writer.writerow([f"CSV_WRITE_ERROR: {csv_write_err}"] * len(headers))
        except Exception:
            pass # Bỏ qua nếu việc ghi lỗi cũng thất bại

    output.seek(0) # Quay lại đầu StringIO để đọc nội dung
    # Thêm BOM UTF-8 (EF BB BF) để Excel (và các trình đọc khác) nhận diện đúng mã hóa UTF-8,
    # đặc biệt quan trọng cho tiếng Việt.
    csv_content_bytes = b'\xef\xbb\xbf' + output.getvalue().encode('utf-8')
    bytes_output = io.BytesIO(csv_content_bytes)

    # Tạo đối tượng discord.File và thêm vào danh sách tham chiếu
    files_list_ref.append(discord.File(bytes_output, filename=filename))
    log.debug(f"Đã tạo file CSV '{filename}' ({len(csv_content_bytes)} bytes) trong bộ nhớ.")


async def create_leaderboard_csv(
    counter: Optional[collections.Counter],
    filename: str,
    item_name: str, # Tên của mục được đếm (vd: "Link", "Ảnh")
    files_list_ref: List[discord.File],
    key_header: str = "User ID" # Header cho cột key (thường là ID)
):
    """Hàm trợ giúp tạo file CSV cho các bảng xếp hạng (leaderboard)."""
    if not counter:
        log.debug(f"Bỏ qua tạo '{filename}': Không có dữ liệu Counter.")
        return
    try:
        log.info(f"💾 Đang tạo file CSV leaderboard: {filename}...")
        headers = ["Rank", key_header, f"{item_name} Count"]
        # Lấy danh sách đã sắp xếp từ Counter và tạo các hàng
        rows = [
            [rank, key, count]
            for rank, (key, count) in enumerate(counter.most_common(), 1)
        ]
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI khi tạo leaderboard CSV '{filename}': {ex}", exc_info=True)


async def create_emoji_reaction_usage_csv(
    reaction_counts: collections.Counter,
    files_list_ref: List[discord.File]
):
    """Tạo file CSV thống kê số lượt sử dụng emoji trong reactions."""
    if not reaction_counts:
        log.debug("Bỏ qua tạo 'top_emoji_reaction_usage.csv': Không có dữ liệu reaction.")
        return

    filename = "top_emoji_reaction_usage.csv"
    try:
        log.info(f"💾 Đang tạo file CSV sử dụng reaction: {filename}...")
        headers = ["Rank", "Emoji Key", "Count"] # Emoji Key có thể là unicode hoặc <name:id>
        rows = [
            [rank, key, count]
            for rank, (key, count) in enumerate(reaction_counts.most_common(), 1)
        ]
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI khi tạo CSV sử dụng reaction '{filename}': {ex}", exc_info=True)


# --- Hàm Chính Tạo Báo Cáo CSV ---
async def create_csv_report(
    # Tham số bắt buộc
    server: discord.Guild,
    bot: discord.Client,
    server_info: Dict[str, Any], # Thông tin tóm tắt server
    channel_details: List[Dict[str, Any]], # Chi tiết kênh/luồng đã quét
    voice_channel_static_data: List[Dict[str, Any]], # Info kênh voice tĩnh
    user_activity: Dict[int, Dict[str, Any]], # Dữ liệu hoạt động user
    roles: List[discord.Role], # Danh sách roles
    boosters: List[discord.Member], # Danh sách boosters
    invites: List[discord.Invite], # Danh sách invites
    webhooks: List[discord.Webhook], # Danh sách webhooks
    integrations: List[discord.Integration], # Danh sách integrations
    audit_logs: List[Dict[str, Any]], # Audit log đã lấy từ DB
    permission_audit: Dict[str, List[Dict[str, Any]]], # Kết quả phân tích quyền
    scan_timestamp: datetime.datetime, # Thời gian kết thúc quét
    files_list_ref: List[discord.File], # List để thêm file CSV vào

    # Dữ liệu tùy chọn (thường là kết quả xử lý/phân tích)
    oldest_members_data: Optional[List[Dict[str, Any]]] = None,
    role_change_stats: Optional[Dict[str, Dict[str, collections.Counter]]] = None,
    user_role_changes: Optional[Dict[int, Dict[str, Dict[str, int]]]] = None,
    user_thread_creation_counts: Optional[collections.Counter] = None,
    keyword_totals: Optional[collections.Counter] = None,
    keyword_by_channel: Optional[Dict[int, collections.Counter]] = None,
    keyword_by_thread: Optional[Dict[int, collections.Counter]] = None,
    keyword_by_user: Optional[Dict[int, collections.Counter]] = None,
    keywords_searched: Optional[List[str]] = None,

    # Các Counter cho leaderboards
    reaction_emoji_counts: Optional[collections.Counter] = None,
    sticker_usage_counts: Optional[collections.Counter] = None,
    invite_usage_counts: Optional[collections.Counter] = None,
    user_link_counts: Optional[collections.Counter] = None,
    user_image_counts: Optional[collections.Counter] = None,
    user_emoji_counts: Optional[collections.Counter] = None,
    user_sticker_counts: Optional[collections.Counter] = None,
    user_mention_given_counts: Optional[collections.Counter] = None,
    user_mention_received_counts: Optional[collections.Counter] = None,
    user_reply_counts: Optional[collections.Counter] = None,
    user_reaction_received_counts: Optional[collections.Counter] = None,
) -> None:
    """
    Hàm chính điều phối việc tạo tất cả các file báo cáo CSV.
    Gọi các hàm helper để tạo từng file CSV cụ thể.
    """
    log.info("💾 Bắt đầu tạo các file CSV chính và phụ...")
    start_time = time.monotonic()

    # --- Tạo các file CSV CHÍNH ---

    # 1. Server Summary CSV
    try:
        await _create_server_summary_csv(server, bot, server_info, roles, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo server_summary.csv: {ex}", exc_info=True)

    # 2. Scanned Channels & Threads Detail CSV
    try:
        await _create_scanned_channels_threads_csv(channel_details, bot, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo scanned_channels_threads.csv: {ex}", exc_info=True)

    # 3. Static Voice/Stage Channels Info CSV
    try:
        await _create_static_voice_stage_csv(voice_channel_static_data, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo static_voice_stage_channels.csv: {ex}", exc_info=True)

    # 4. User Activity Detail CSV
    try:
        await _create_user_activity_csv(user_activity, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo user_activity_detail.csv: {ex}", exc_info=True)

    # 5. Roles Detail CSV
    try:
        await _create_roles_detail_csv(roles, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo roles_detail.csv: {ex}", exc_info=True)

    # 6. Boosters Detail CSV
    try:
        await _create_boosters_detail_csv(boosters, scan_timestamp, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo boosters_detail.csv: {ex}", exc_info=True)

    # 7. Invites Detail CSV
    try:
        await _create_invites_detail_csv(invites, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo invites_detail.csv: {ex}", exc_info=True)

    # 8. Webhooks Detail CSV
    try:
        await _create_webhooks_detail_csv(webhooks, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo webhooks_detail.csv: {ex}", exc_info=True)

    # 9. Integrations Detail CSV
    try:
        await _create_integrations_detail_csv(integrations, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo integrations_detail.csv: {ex}", exc_info=True)

    # 10. Audit Log Detail CSV
    if audit_logs: # Chỉ tạo nếu có dữ liệu audit log
        try:
            await _create_audit_log_detail_csv(audit_logs, files_list_ref)
        except Exception as ex:
            log.error(f"‼️ LỖI tạo audit_log_detail.csv: {ex}", exc_info=True)

    # 11. Permission Audit CSVs
    try:
        await _create_permission_audit_csvs(permission_audit, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo các file permission audit CSV: {ex}", exc_info=True)

    # --- Tạo các file CSV PHỤ (Leaderboards, Stats, Keywords) ---

    if oldest_members_data:
        await create_top_oldest_members_csv(oldest_members_data, files_list_ref)

    if role_change_stats:
        await create_role_change_stats_csv(role_change_stats, server, files_list_ref, filename_suffix="_by_mod")
        await create_top_roles_granted_csv(role_change_stats, server, files_list_ref)

    if user_role_changes:
        await create_user_role_change_csv(user_role_changes, server, files_list_ref)

    # Tạo các Leaderboard CSVs bằng hàm helper
    await create_leaderboard_csv(user_link_counts, "top_link_users.csv", "Link", files_list_ref)
    await create_leaderboard_csv(user_image_counts, "top_image_users.csv", "Ảnh", files_list_ref)
    await create_leaderboard_csv(user_emoji_counts, "top_emoji_content_users.csv", "Emoji (Content)", files_list_ref)
    await create_leaderboard_csv(user_sticker_counts, "top_sticker_senders.csv", "Sticker Sent", files_list_ref)
    await create_leaderboard_csv(user_mention_given_counts, "top_mentioning_users.csv", "Mention Given", files_list_ref)
    await create_leaderboard_csv(user_mention_received_counts, "top_mentioned_users.csv", "Mention Received", files_list_ref)
    await create_leaderboard_csv(user_reply_counts, "top_repliers.csv", "Reply", files_list_ref)
    if user_reaction_received_counts:
        await create_leaderboard_csv(user_reaction_received_counts, "top_reaction_received_users.csv", "Reaction Received", files_list_ref)
    if invite_usage_counts:
        await create_leaderboard_csv(invite_usage_counts, "top_inviters.csv", "Invite Use", files_list_ref)
    if sticker_usage_counts:
        await create_leaderboard_csv(sticker_usage_counts, "top_sticker_usage.csv", "Sticker Usage", files_list_ref, key_header="Sticker ID")
    if reaction_emoji_counts:
        await create_emoji_reaction_usage_csv(reaction_emoji_counts, files_list_ref)
    if user_thread_creation_counts:
        await create_leaderboard_csv(user_thread_creation_counts, "top_thread_creators.csv", "Thread Created", files_list_ref)

    # Tạo Keyword CSVs
    if keywords_searched and keyword_totals:
        await create_keyword_csv_reports(
            keyword_totals, keyword_by_channel, keyword_by_thread,
            keyword_by_user, keywords_searched, files_list_ref
        )

    end_time = time.monotonic()
    log.info(f"✅ Hoàn thành tạo tất cả file CSV yêu cầu trong {end_time - start_time:.2f}s.")


# --- Các hàm tạo CSV cụ thể (internal helpers) ---

async def _create_server_summary_csv(server, bot, server_info, roles, files_list_ref):
    """Tạo file CSV tóm tắt thông tin server."""
    headers = ["Metric", "Value"]
    owner_name = "N/A"
    if server.owner:
        owner_name = server.owner.name
    elif server.owner_id:
        owner = await utils.fetch_user_data(server, server.owner_id, bot_ref=bot)
        if owner: owner_name = owner.name

    rows = [
        ["Server Name", server.name], ["Server ID", server.id], ["Owner ID", server.owner_id],
        ["Owner Name", owner_name], ["Created At", server.created_at.isoformat()],
        ["Total Members (Cache)", server.member_count],
        ["Real Users (Scan Start)", server_info.get('member_count_real', 'N/A')],
        ["Bots (Scan Start)", server_info.get('bot_count', 'N/A')], ["Boost Tier", server.premium_tier],
        ["Boost Count", server.premium_subscription_count], ["Verification Level", str(server.verification_level)],
        ["Explicit Content Filter", str(server.explicit_content_filter)], ["MFA Level", str(server.mfa_level)],
        ["Default Notifications", str(server.default_notifications)],
        ["System Channel ID", server.system_channel.id if server.system_channel else "N/A"],
        ["Rules Channel ID", server.rules_channel.id if server.rules_channel else "N/A"],
        ["Public Updates Channel ID", server.public_updates_channel.id if server.public_updates_channel else "N/A"],
        ["AFK Channel ID", server.afk_channel.id if server.afk_channel else "N/A"],
        ["AFK Timeout (seconds)", server.afk_timeout],
        ["Total Text Channels (Scan Start)", server_info.get('text_channel_count', 'N/A')],
        ["Total Voice Channels (Scan Start)", server_info.get('voice_channel_count', 'N/A')],
        ["Total Categories (Scan Start)", server_info.get('category_count', 'N/A')],
        ["Total Stages (Scan Start)", server_info.get('stage_count', 'N/A')],
        ["Total Forums (Scan Start)", server_info.get('forum_count', 'N/A')],
        ["Total Roles (excl. @everyone)", len(roles)], ["Total Emojis", len(server.emojis)],
        ["Total Stickers", len(server.stickers)],
        ["Total Reactions Scanned", server_info.get('reaction_count_overall', 'N/A')]
    ]
    await _write_csv_to_list("server_summary.csv", headers, rows, files_list_ref)

async def _create_scanned_channels_threads_csv(channel_details, bot, files_list_ref):
    """Tạo file CSV chi tiết về các kênh và luồng đã quét."""
    headers = [
        "Item Type", "Channel Type", "ID", "Name", "Parent Channel ID", "Parent Channel Name",
        "Category ID", "Category Name", "Created At", "Is NSFW", "Slowmode (s)", "Topic",
        "Message Count (Scan)", "Reaction Count (Scan)", "Scan Duration (s)",
        "Top Chatter ID", "Top Chatter Name", "Top Chatter Msg Count",
        "Is Archived", "Is Locked", "Thread Owner ID", "Error"
    ]
    rows = []
    for detail in channel_details:
        channel_type_str = detail.get("type", "unknown")
        is_voice = channel_type_str == str(discord.ChannelType.voice)

        # Parse top chatter info
        top_chatter_id, top_chatter_name, top_chatter_msg_count = "N/A", "N/A", 0
        top_chatter_str = detail.get('top_chatter')
        if isinstance(top_chatter_str, str):
             mention_match = re.search(r'<@!?(\d+)>', top_chatter_str)
             id_match = re.search(r'ID: `(\d+)`', top_chatter_str)
             name_match = re.search(r'\(`(.*?)`\)', top_chatter_str)
             count_match = re.search(r'- (\d{1,3}(?:,\d{3})*)\s*tin', top_chatter_str)
             if mention_match: top_chatter_id = mention_match.group(1)
             elif id_match: top_chatter_id = id_match.group(1)
             if name_match: top_chatter_name = name_match.group(1)
             if count_match:
                 try: top_chatter_msg_count = int(count_match.group(1).replace(',', ''))
                 except ValueError: pass

        is_nsfw_str = detail.get('nsfw', '')
        is_nsfw = isinstance(is_nsfw_str, str) and is_nsfw_str.startswith(utils.get_emoji('success', bot))
        slowmode_val = utils.parse_slowmode(detail.get('slowmode', '0')) if not is_voice else None
        scan_duration_s = detail.get('duration', datetime.timedelta(0)).total_seconds() if detail.get('processed') else 0
        topic_val = detail.get('topic', '') if not is_voice and detail.get('processed') else None

        channel_row = [
            "Channel", channel_type_str, detail.get('id', 'N/A'), detail.get('name', 'N/A'),
            None, None, # Parent channel (N/A for channels)
            detail.get('category_id', 'N/A'), detail.get('category', 'N/A'),
            detail.get('created_at').isoformat() if detail.get('created_at') else 'N/A',
            is_nsfw, slowmode_val, topic_val, detail.get('message_count', 0),
            detail.get('reaction_count'), round(scan_duration_s, 2),
            top_chatter_id, top_chatter_name, top_chatter_msg_count,
            None, None, None, # Thread specific fields
            detail.get('error', '')
        ]
        rows.append(channel_row)

        # Add thread rows if they exist for this channel
        if "threads_data" in detail:
            for thread_data in detail.get("threads_data", []):
                thread_row = [
                    "Thread", str(discord.ChannelType.public_thread), # Assuming public for now
                    thread_data.get('id', 'N/A'), thread_data.get('name', 'N/A'),
                    detail.get('id'), detail.get('name'), # Parent channel info
                    detail.get('category_id'), detail.get('category'), # Parent category info
                    thread_data.get('created_at'), # Already ISO format if available
                    None, None, None, # NSFW, Slowmode, Topic (N/A for threads)
                    thread_data.get('message_count', 0), thread_data.get('reaction_count'),
                    round(thread_data.get('scan_duration_seconds', 0), 2),
                    None, None, None, # Top chatter (N/A for threads in this context)
                    thread_data.get('archived'), thread_data.get('locked'), thread_data.get('owner_id'),
                    thread_data.get('error', '')
                ]
                rows.append(thread_row)

    await _write_csv_to_list("scanned_channels_threads.csv", headers, rows, files_list_ref)

async def _create_static_voice_stage_csv(voice_channel_static_data, files_list_ref):
    """Tạo file CSV thông tin tĩnh của kênh Voice/Stage."""
    headers = ["ID", "Name", "Type", "Category ID", "Category Name", "Created At", "User Limit", "Bitrate (bps)"]
    rows = [
        [
            vc.get('id'), vc.get('name'), vc.get('type'), vc.get('category_id'), vc.get('category'),
            vc.get('created_at').isoformat() if vc.get('created_at') else None,
            vc.get('user_limit'), utils.parse_bitrate(str(vc.get('bitrate','0')))
        ]
        for vc in voice_channel_static_data
    ]
    await _write_csv_to_list("static_voice_stage_channels.csv", headers, rows, files_list_ref)

async def _create_user_activity_csv(user_activity, files_list_ref):
    """Tạo file CSV chi tiết hoạt động của từng user."""
    headers = [
        "User ID", "Is Bot", "Message Count", "Link Count", "Image Count",
        "Emoji (Content) Count", "Sticker Sent Count", "Mention Given Count",
        "Mention Received Count", "Reply Count", "Reaction Received Count",
        "First Seen UTC", "Last Seen UTC", "Activity Span (s)"
    ]
    rows = []
    for user_id, data in user_activity.items():
        first_seen = data.get('first_seen')
        last_seen = data.get('last_seen')
        activity_span_secs = 0
        if first_seen and last_seen and last_seen >= first_seen:
            try:
                activity_span_secs = (last_seen - first_seen).total_seconds()
            except Exception:
                pass # Ignore errors calculating span
        rows.append([
            user_id, data.get('is_bot', False), data.get('message_count', 0),
            data.get('link_count', 0), data.get('image_count', 0),
            data.get('emoji_count', 0), data.get('sticker_count', 0),
            data.get('mention_given_count', 0), data.get('mention_received_count', 0),
            data.get('reply_count', 0), data.get('reaction_received_count', 0),
            first_seen.isoformat() if first_seen else None,
            last_seen.isoformat() if last_seen else None,
            round(activity_span_secs, 2)
        ])
    await _write_csv_to_list("user_activity_detail.csv", headers, rows, files_list_ref)

async def _create_roles_detail_csv(roles, files_list_ref):
    """Tạo file CSV chi tiết về các roles."""
    headers = [
        "ID", "Name", "Position", "Color", "Is Hoisted", "Is Mentionable",
        "Is Bot Role", "Member Count (Scan End)", "Created At", "Permissions Value"
    ]
    rows = [
        [
            role.id, role.name, role.position, str(role.color),
            role.hoist, role.mentionable, role.is_bot_managed(), len(role.members),
            role.created_at.isoformat(), role.permissions.value
        ]
        for role in roles
    ]
    await _write_csv_to_list("roles_detail.csv", headers, rows, files_list_ref)

async def _create_boosters_detail_csv(boosters, scan_timestamp, files_list_ref):
    """Tạo file CSV chi tiết về những người đang boost server."""
    headers = ["User ID", "Username", "Display Name", "Boost Start UTC", "Boost Duration (s)"]
    rows = []
    for member in boosters:
        boost_duration_secs = 0
        if member.premium_since:
            try:
                # Ensure both datetimes are timezone-aware (UTC) for comparison
                since_aware = member.premium_since.astimezone(datetime.timezone.utc) if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc)
                scan_aware = scan_timestamp.astimezone(datetime.timezone.utc) if scan_timestamp.tzinfo else scan_timestamp.replace(tzinfo=datetime.timezone.utc)
                if scan_aware >= since_aware:
                    boost_duration_secs = (scan_aware - since_aware).total_seconds()
            except Exception:
                pass # Ignore errors calculating duration
        rows.append([
            member.id, member.name, member.display_name,
            member.premium_since.isoformat() if member.premium_since else None,
            round(boost_duration_secs, 2) if boost_duration_secs >= 0 else 0
        ])
    await _write_csv_to_list("boosters_detail.csv", headers, rows, files_list_ref)

async def _create_invites_detail_csv(invites, files_list_ref):
    """Tạo file CSV chi tiết về các lời mời."""
    headers = [
        "Code", "Inviter ID", "Inviter Name", "Channel ID", "Channel Name",
        "Created At UTC", "Expires At UTC", "Uses", "Max Uses", "Is Temporary"
    ]
    rows = [
        [
            inv.code, inv.inviter.id if inv.inviter else None, inv.inviter.name if inv.inviter else None,
            inv.channel.id if inv.channel else None, inv.channel.name if inv.channel else None,
            inv.created_at.isoformat() if inv.created_at else None,
            inv.expires_at.isoformat() if inv.expires_at else None,
            inv.uses or 0, inv.max_uses or 0, inv.temporary
        ]
        for inv in invites
    ]
    await _write_csv_to_list("invites_detail.csv", headers, rows, files_list_ref)

async def _create_webhooks_detail_csv(webhooks, files_list_ref):
    """Tạo file CSV chi tiết về các webhooks."""
    headers = ["ID", "Name", "Creator ID", "Creator Name", "Channel ID", "Channel Name", "Created At UTC"]
    rows = [
        [
            wh.id, wh.name, wh.user.id if wh.user else None, wh.user.name if wh.user else None,
            wh.channel_id, getattr(wh.channel, 'name', None), # Lấy tên kênh nếu có
            wh.created_at.isoformat() if wh.created_at else None
        ]
        for wh in webhooks
    ]
    await _write_csv_to_list("webhooks_detail.csv", headers, rows, files_list_ref)

async def _create_integrations_detail_csv(integrations, files_list_ref):
    """Tạo file CSV chi tiết về các tích hợp server."""
    headers = [
        "ID", "Name", "Type", "Enabled", "Syncing", "Role ID", "Role Name",
        "Expire Behaviour", "Expire Grace Period (s)", "Account ID", "Account Name"
    ]
    rows = []
    for integ in integrations:
        integ_type = integ.type if isinstance(integ.type, str) else integ.type.name
        role_id = integ.role.id if hasattr(integ, 'role') and integ.role else None
        role_name = integ.role.name if hasattr(integ, 'role') and integ.role else None
        expire_behaviour = integ.expire_behaviour.name if hasattr(integ, 'expire_behaviour') and integ.expire_behaviour else None
        grace_period = integ.expire_grace_period if hasattr(integ, 'expire_grace_period') is not None else None
        syncing = integ.syncing if hasattr(integ, 'syncing') else None
        rows.append([
            integ.id, integ.name, integ_type, integ.enabled, syncing,
            role_id, role_name, expire_behaviour, grace_period,
            integ.account.id if integ.account else None,
            integ.account.name if integ.account else None
        ])
    await _write_csv_to_list("integrations_detail.csv", headers, rows, files_list_ref)

async def _create_audit_log_detail_csv(audit_logs, files_list_ref):
    """Tạo file CSV chi tiết từ dữ liệu audit log đã lấy từ DB."""
    headers = ["Log ID", "Timestamp UTC", "Action Type", "User ID", "Target ID", "Reason", "Changes (JSON)"]
    rows = []
    for log_entry in audit_logs:
        # Chuyển đổi dict 'extra_data' thành chuỗi JSON
        extra_data_json = json.dumps(log_entry.get('extra_data'), ensure_ascii=False, default=str) if log_entry.get('extra_data') else ""
        created_at_dt = log_entry.get('created_at')
        created_at_iso = created_at_dt.isoformat() if isinstance(created_at_dt, datetime.datetime) else str(created_at_dt)
        rows.append([
            log_entry.get('log_id'), created_at_iso,
            log_entry.get('action_type'), log_entry.get('user_id'),
            log_entry.get('target_id'), log_entry.get('reason'),
            extra_data_json
        ])
    await _write_csv_to_list("audit_log_detail.csv", headers, rows, files_list_ref)

async def _create_permission_audit_csvs(permission_audit, files_list_ref):
    """Tạo các file CSV liên quan đến phân tích quyền."""
    # Roles có quyền Admin
    admin_headers = ["Role ID", "Role Name", "Position", "Member Count (Scan End)"]
    admin_rows = [
        [r.get('id'), r.get('name'), r.get('position'), r.get('member_count', 0)]
        for r in permission_audit.get("roles_with_admin", [])
    ]
    await _write_csv_to_list("permission_admin_roles.csv", admin_headers, admin_rows, files_list_ref)

    # Kênh có quyền @everyone rủi ro
    everyone_headers = ["Channel ID", "Channel Name", "Permission Name", "Permission Value"]
    everyone_rows = []
    for item in permission_audit.get("risky_everyone_overwrites", []):
        for perm_name, perm_value in item.get('permissions', {}).items():
            everyone_rows.append([item.get('channel_id'), item.get('channel_name'), perm_name, perm_value])
    await _write_csv_to_list("permission_risky_everyone.csv", everyone_headers, everyone_rows, files_list_ref)

    # Roles khác có quyền rủi ro
    other_headers = ["Role ID", "Role Name", "Position", "Member Count (Scan End)", "Risky Permission Name"]
    other_rows = []
    for role_info in permission_audit.get("other_risky_role_perms", []):
        for perm_name in role_info.get('permissions', {}):
            other_rows.append([
                role_info.get('role_id'), role_info.get('role_name'), role_info.get('position'),
                role_info.get('member_count', 0), perm_name
            ])
    await _write_csv_to_list("permission_other_risky_roles.csv", other_headers, other_rows, files_list_ref)


# --- Các hàm tạo CSV phụ trợ ---

async def create_top_oldest_members_csv(
    oldest_members_data: List[Dict[str, Any]],
    files_list_ref: List[discord.File]
):
    """Tạo file CSV cho top thành viên lâu năm nhất."""
    if not oldest_members_data: return
    filename = "top_oldest_members.csv"
    log.info(f"💾 Đang tạo {filename}...")
    headers = ["Rank", "User ID", "Display Name", "Joined At UTC", "Time in Server (Days Approx)"]
    rows = []
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    for rank, data in enumerate(oldest_members_data, 1):
        joined_at = data.get('joined_at')
        days_in_server = "N/A"
        joined_at_iso = None
        if isinstance(joined_at, datetime.datetime):
            joined_at_iso = joined_at.isoformat()
            try:
                # Đảm bảo joined_at có timezone để so sánh
                join_aware = joined_at.astimezone(datetime.timezone.utc) if joined_at.tzinfo else joined_at.replace(tzinfo=datetime.timezone.utc)
                if now_utc >= join_aware:
                    days_in_server = (now_utc - join_aware).days
            except Exception:
                pass # Bỏ qua lỗi tính toán
        rows.append([
            rank, data.get('id', 'N/A'), data.get('display_name', 'N/A'),
            joined_at_iso, days_in_server
        ])
    try:
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo {filename}: {ex}", exc_info=True)


async def create_role_change_stats_csv(
    role_change_stats: Dict[str, Dict[str, collections.Counter]],
    guild: discord.Guild,
    files_list_ref: List[discord.File],
    filename_suffix: str
):
    """Tạo file CSV thống kê thay đổi role (thêm/bớt bởi mod)."""
    if not role_change_stats: return
    filename = f"role_change_stats{filename_suffix}.csv"
    log.info(f"💾 Đang tạo {filename}...")
    headers = ["Role ID", "Role Name", "Change Type", "Moderator ID", "Count"]
    rows = []
    for role_id_str, stats in role_change_stats.items():
        role = guild.get_role(int(role_id_str)) if role_id_str.isdigit() else None
        role_name = role.name if role else "Unknown/Deleted Role"
        # Thêm dòng cho mỗi mod đã thêm role này
        for mod_id, count in stats.get("added", {}).items():
            rows.append([role_id_str, role_name, "ADDED", mod_id, count])
        # Thêm dòng cho mỗi mod đã xóa role này
        for mod_id, count in stats.get("removed", {}).items():
            rows.append([role_id_str, role_name, "REMOVED", mod_id, count])
    # Sắp xếp để dễ đọc hơn
    rows.sort(key=lambda x: (str(x[0]), x[2], str(x[3]))) # Sort by Role ID, Change Type, Mod ID
    try:
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo {filename}: {ex}", exc_info=True)


async def create_user_role_change_csv(
    user_role_changes: Dict[int, Dict[str, Dict[str, int]]],
    guild: discord.Guild,
    files_list_ref: List[discord.File]
):
    """Tạo file CSV thống kê thay đổi role cho từng user."""
    if not user_role_changes: return
    filename = "role_change_stats_for_user.csv"
    log.info(f"💾 Đang tạo {filename}...")
    headers = ["User ID", "Role ID", "Role Name", "Change Type", "Count"]
    rows = []
    for user_id, role_stats in user_role_changes.items():
        for role_id_str, changes in role_stats.items():
            role = guild.get_role(int(role_id_str)) if role_id_str.isdigit() else None
            role_name = role.name if role else "Unknown/Deleted Role"
            # Thêm dòng nếu role được thêm cho user này
            if changes.get("added", 0) > 0:
                rows.append([user_id, role_id_str, role_name, "ADDED", changes["added"]])
            # Thêm dòng nếu role bị xóa khỏi user này
            if changes.get("removed", 0) > 0:
                rows.append([user_id, role_id_str, role_name, "REMOVED", changes["removed"]])
    # Sắp xếp để dễ đọc
    rows.sort(key=lambda x: (str(x[0]), str(x[1]), x[3])) # Sort by User ID, Role ID, Change Type
    try:
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo {filename}: {ex}", exc_info=True)


async def create_top_roles_granted_csv(
    role_change_stats: Dict[str, Dict[str, collections.Counter]],
    guild: discord.Guild,
    files_list_ref: List[discord.File]
):
    """Tạo file CSV xếp hạng các role được cấp nhiều nhất."""
    if not role_change_stats: return

    # Tính tổng số lần mỗi role được cấp (added)
    role_grant_counts = collections.Counter({
        role_id_str: sum(stats.get('added', {}).values())
        for role_id_str, stats in role_change_stats.items()
        if sum(stats.get('added', {}).values()) > 0 # Chỉ tính role được cấp ít nhất 1 lần
    })

    if not role_grant_counts: return # Không có role nào được cấp

    filename = "top_roles_granted.csv"
    log.info(f"💾 Đang tạo {filename}...")
    headers = ["Rank", "Role ID", "Role Name", "Times Granted"]
    rows = []
    for rank, (role_id_str, count) in enumerate(role_grant_counts.most_common(), 1):
        role = guild.get_role(int(role_id_str)) if role_id_str.isdigit() else None
        role_name = role.name if role else "Unknown/Deleted Role"
        rows.append([rank, role_id_str, role_name, count])
    try:
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo {filename}: {ex}", exc_info=True)


async def create_keyword_csv_reports(
    keyword_totals: collections.Counter,
    keyword_by_channel: Dict[int, collections.Counter],
    keyword_by_thread: Dict[int, collections.Counter],
    keyword_by_user: Dict[int, collections.Counter],
    keywords_searched: List[str], # Danh sách keyword đã tìm
    files_list_ref: List[discord.File]
):
    """Tạo các file CSV liên quan đến phân tích từ khóa."""
    if not keywords_searched or not keyword_totals:
        log.debug("Bỏ qua tạo keyword CSVs: không có keyword hoặc không tìm thấy.")
        return

    # 1. Tóm tắt tổng số lần xuất hiện của mỗi keyword
    try:
        kw_sum_headers = ["Keyword", "Total Count"]
        kw_sum_rows = sorted(list(keyword_totals.items()), key=lambda item: item[1], reverse=True)
        await _write_csv_to_list("keyword_summary.csv", kw_sum_headers, kw_sum_rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo keyword_summary.csv: {ex}", exc_info=True)

    # 2. Số lần xuất hiện theo Kênh/Luồng
    try:
        kw_loc_headers = ["Location ID (Channel/Thread)", "Keyword", "Count"]
        kw_loc_rows = []
        # Gộp dữ liệu từ kênh và luồng
        all_location_counts = {**keyword_by_channel, **keyword_by_thread}
        for loc_id, counts in all_location_counts.items():
            for keyword, count in counts.items():
                kw_loc_rows.append([loc_id, keyword, count])
        kw_loc_rows.sort(key=lambda x: (str(x[0]), x[1])) # Sort by Location ID, Keyword
        await _write_csv_to_list("keyword_by_location.csv", kw_loc_headers, kw_loc_rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo keyword_by_location.csv: {ex}", exc_info=True)

    # 3. Số lần xuất hiện theo User
    try:
        kw_user_headers = ["User ID", "Keyword", "Count"]
        kw_user_rows = []
        for user_id, counts in keyword_by_user.items():
            for keyword, count in counts.items():
                kw_user_rows.append([user_id, keyword, count])
        kw_user_rows.sort(key=lambda x: (str(x[0]), x[1])) # Sort by User ID, Keyword
        await _write_csv_to_list("keyword_by_user.csv", kw_user_headers, kw_user_rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI tạo keyword_by_user.csv: {ex}", exc_info=True)

# --- END OF FILE reporting/csv_writer.py ---