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
from typing import List, Dict, Any, Optional, Union, Set, Tuple
from collections import Counter, defaultdict
import utils
import config


log = logging.getLogger(__name__)

async def _write_csv_to_list(
    filename: str, headers: List[str], data_rows: List[List[Any]], files_list_ref: List[discord.File]
):
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
    try:
        writer.writerow(headers)
        for row in data_rows:
            sanitized_row = [utils.sanitize_for_csv(cell) for cell in row]
            writer.writerow(sanitized_row)
    except Exception as csv_write_err:
        log.error(f"Lỗi nghiêm trọng khi ghi dữ liệu vào CSV '{filename}': {csv_write_err}", exc_info=True)
        try: writer.writerow([f"CSV_WRITE_ERROR: {csv_write_err}"] * len(headers))
        except Exception: pass
    output.seek(0)
    csv_content_bytes = b'\xef\xbb\xbf' + output.getvalue().encode('utf-8')
    bytes_output = io.BytesIO(csv_content_bytes)
    files_list_ref.append(discord.File(bytes_output, filename=filename))
    log.debug(f"Đã tạo file CSV '{filename}' ({len(csv_content_bytes)} bytes) trong bộ nhớ.")


async def create_leaderboard_csv(
    counter: Optional[collections.Counter], filename: str, item_name: str,
    files_list_ref: List[discord.File], key_header: str = "User ID",
    filter_admins: bool = True, guild: Optional[discord.Guild] = None
):
    if not counter:
        log.debug(f"Bỏ qua tạo '{filename}': Không có dữ liệu Counter.")
        return

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins and guild:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    try:
        log.info(f"💾 Đang tạo file CSV leaderboard: {filename}...")
        headers = ["Rank", key_header, f"{item_name} Count"]
        rows = []
        rank = 0
        for key, count in counter.most_common():
            if filter_admins and admin_ids_to_filter and isinstance(key, int) and key in admin_ids_to_filter:
                continue
            if count > 0:
                rank += 1
                rows.append([rank, key, count])

        if not rows:
            log.debug(f"Bỏ qua tạo '{filename}': Không có dữ liệu sau khi lọc.")
            return
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI khi tạo leaderboard CSV '{filename}': {ex}", exc_info=True)


async def create_filtered_emoji_reaction_usage_csv(
    filtered_reaction_counts: collections.Counter,
    files_list_ref: List[discord.File]
):
    if not filtered_reaction_counts:
        log.debug("Bỏ qua tạo 'top_filtered_emoji_reactions.csv': Không có dữ liệu reaction đã lọc.")
        return
    filename = "top_filtered_emoji_reactions.csv"
    try:
        log.info(f"💾 Đang tạo file CSV sử dụng reaction (lọc): {filename}...")
        headers = ["Rank", "Emoji Key", "Count"]
        rows = [[rank, key, count] for rank, (key, count) in enumerate(filtered_reaction_counts.most_common(), 1)]
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI khi tạo CSV sử dụng reaction (lọc) '{filename}': {ex}", exc_info=True)

# --- Thêm hàm tạo CSV cho người thả reaction ---
async def create_top_reaction_givers_csv(
    user_reaction_given_counts: collections.Counter,
    user_reaction_emoji_given_counts: defaultdict,
    files_list_ref: List[discord.File],
    filter_admins: bool = True,
    guild: Optional[discord.Guild] = None
):
    if not user_reaction_given_counts:
        log.debug("Bỏ qua tạo 'top_reaction_givers.csv': Không có dữ liệu.")
        return
    filename = "top_reaction_givers.csv"

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins and guild:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    try:
        log.info(f"💾 Đang tạo file CSV: {filename}...")
        headers = ["Rank", "User ID", "Total Reactions Given (Filtered)", "Most Used Emoji Key"]
        rows = []
        rank = 0
        for user_id, total_count in user_reaction_given_counts.most_common():
            if filter_admins and admin_ids_to_filter and isinstance(user_id, int) and user_id in admin_ids_to_filter:
                continue
            if total_count > 0:
                rank += 1
                most_used_emoji_key = "N/A"
                user_specific_counts = user_reaction_emoji_given_counts.get(user_id, Counter())
                if user_specific_counts:
                    try:
                        most_used_key, _ = max(user_specific_counts.items(), key=lambda item: item[1])
                        most_used_emoji_key = str(most_used_key)
                    except ValueError: pass
                rows.append([rank, user_id, total_count, most_used_emoji_key])

        if not rows:
            log.debug(f"Bỏ qua tạo '{filename}': Không có dữ liệu sau khi lọc.")
            return
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex:
        log.error(f"‼️ LỖI khi tạo CSV người thả reaction '{filename}': {ex}", exc_info=True)
# --- Kết thúc hàm mới ---


async def create_csv_report(
    server: discord.Guild, bot: discord.Client, server_info: Dict[str, Any],
    channel_details: List[Dict[str, Any]], voice_channel_static_data: List[Dict[str, Any]],
    user_activity: Dict[int, Dict[str, Any]], roles: List[discord.Role],
    boosters: List[discord.Member], invites: List[discord.Invite], webhooks: List[discord.Webhook],
    integrations: List[discord.Integration], audit_logs: List[Dict[str, Any]],
    scan_timestamp: datetime.datetime, files_list_ref: List[discord.File],
    oldest_members_data: Optional[List[Dict[str, Any]]] = None,
    user_thread_creation_counts: Optional[collections.Counter] = None,
    tracked_role_grant_counts: Optional[defaultdict] = None,
    user_distinct_channel_counts: Optional[collections.Counter] = None,
    user_total_custom_emoji_content_counts: Optional[collections.Counter] = None,
    keyword_totals: Optional[collections.Counter] = None,
    keyword_by_channel: Optional[Dict[int, collections.Counter]] = None,
    keyword_by_thread: Optional[Dict[int, collections.Counter]] = None,
    keyword_by_user: Optional[Dict[int, collections.Counter]] = None,
    keywords_searched: Optional[List[str]] = None,
    filtered_reaction_emoji_counts: Optional[collections.Counter] = None,
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
    user_reaction_given_counts: Optional[collections.Counter] = None, # Thêm mới
    user_reaction_emoji_given_counts: Optional[defaultdict] = None, # Thêm mới
    user_other_file_counts: Optional[collections.Counter] = None,
    user_most_active_channel: Optional[Dict[int, Tuple[int, int]]] = None,
    user_emoji_received_counts: Optional[defaultdict] = None # Thêm mới cho CSV
) -> None:
    log.info("💾 Bắt đầu tạo các file CSV chính và phụ...")
    start_time = time.monotonic()

    # --- CSV CHÍNH ---
    try:
        overall_custom_sticker_counts = Counter()
        if sticker_usage_counts and server:
             for sid_str, count in sticker_usage_counts.items():
                 if sid_str.isdigit():
                     pass
        scan_data_summary = {
            "overall_custom_emoji_content_counts": user_total_custom_emoji_content_counts,
            "overall_custom_sticker_counts": overall_custom_sticker_counts,
            "filtered_reaction_emoji_counts": filtered_reaction_emoji_counts,
        }
        await _create_server_summary_csv(server, bot, server_info, roles, files_list_ref, scan_data=scan_data_summary)
    except Exception as ex: log.error(f"‼️ LỖI tạo server_summary.csv: {ex}", exc_info=True)

    try: await _create_scanned_channels_threads_csv(channel_details, bot, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo scanned_channels_threads.csv: {ex}", exc_info=True)

    try: await _create_static_voice_stage_csv(voice_channel_static_data, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo static_voice_stage_channels.csv: {ex}", exc_info=True)

    try: await _create_user_activity_csv(user_activity, files_list_ref, user_distinct_channel_counts, user_total_custom_emoji_content_counts, user_most_active_channel, user_reaction_given_counts, user_emoji_received_counts) # Thêm reaction_given và emoji_received
    except Exception as ex: log.error(f"‼️ LỖI tạo user_activity_detail.csv: {ex}", exc_info=True)

    try: await _create_roles_detail_csv(roles, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo roles_detail.csv: {ex}", exc_info=True)

    try: await _create_boosters_detail_csv(boosters, scan_timestamp, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo boosters_detail.csv: {ex}", exc_info=True)

    try: await _create_invites_detail_csv(invites, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo invites_detail.csv: {ex}", exc_info=True)

    try: await _create_webhooks_detail_csv(webhooks, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo webhooks_detail.csv: {ex}", exc_info=True)

    try: await _create_integrations_detail_csv(integrations, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo integrations_detail.csv: {ex}", exc_info=True)

    if audit_logs:
        try: await _create_audit_log_detail_csv(audit_logs, files_list_ref)
        except Exception as ex: log.error(f"‼️ LỖI tạo audit_log_detail.csv: {ex}", exc_info=True)

    # --- CSV PHỤ ---
    if oldest_members_data: await create_top_oldest_members_csv(oldest_members_data, files_list_ref)
    if tracked_role_grant_counts: await create_tracked_role_grants_csv(tracked_role_grant_counts, server, files_list_ref)

    await create_leaderboard_csv(user_link_counts, "top_link_users.csv", "Link", files_list_ref, filter_admins=True, guild=server)
    await create_leaderboard_csv(user_image_counts, "top_image_users.csv", "Ảnh", files_list_ref, filter_admins=True, guild=server)
    await create_leaderboard_csv(user_total_custom_emoji_content_counts, "top_custom_emoji_content_users.csv", "Custom Emoji (Content)", files_list_ref, filter_admins=True, guild=server)
    await create_leaderboard_csv(user_sticker_counts, "top_sticker_senders.csv", "Sticker Sent", files_list_ref, filter_admins=True, guild=server)
    await create_leaderboard_csv(user_mention_given_counts, "top_mentioning_users.csv", "Mention Given", files_list_ref, filter_admins=True, guild=server)
    await create_leaderboard_csv(user_mention_received_counts, "top_mentioned_users.csv", "Mention Received", files_list_ref, filter_admins=False, guild=server)
    await create_leaderboard_csv(user_reply_counts, "top_repliers.csv", "Reply", files_list_ref, filter_admins=True, guild=server)
    if user_reaction_received_counts: await create_leaderboard_csv(user_reaction_received_counts, "top_reaction_received_users.csv", "Reaction Received", files_list_ref, filter_admins=False, guild=server)
    # Thêm CSV cho người thả reaction
    if user_reaction_given_counts and user_reaction_emoji_given_counts: await create_top_reaction_givers_csv(user_reaction_given_counts, user_reaction_emoji_given_counts, files_list_ref, filter_admins=True, guild=server)

    if invite_usage_counts: await create_leaderboard_csv(invite_usage_counts, "top_inviters.csv", "Invite Use", files_list_ref, filter_admins=False, guild=server)
    if sticker_usage_counts: await create_leaderboard_csv(sticker_usage_counts, "top_sticker_usage.csv", "Sticker Usage", files_list_ref, key_header="Sticker ID", filter_admins=False)
    if filtered_reaction_emoji_counts: await create_filtered_emoji_reaction_usage_csv(filtered_reaction_emoji_counts, files_list_ref)
    if user_thread_creation_counts: await create_leaderboard_csv(user_thread_creation_counts, "top_thread_creators.csv", "Thread Created", files_list_ref, filter_admins=True, guild=server)
    if user_distinct_channel_counts: await create_leaderboard_csv(user_distinct_channel_counts, "top_distinct_channel_users.csv", "Distinct Channel", files_list_ref, filter_admins=True, guild=server)
    if user_other_file_counts: await create_leaderboard_csv(user_other_file_counts, "top_other_file_users.csv", "Other File Sent", files_list_ref, filter_admins=True, guild=server)

    if keywords_searched and keyword_totals:
        await create_keyword_csv_reports(keyword_totals, keyword_by_channel, keyword_by_thread, keyword_by_user, keywords_searched, files_list_ref)

    end_time = time.monotonic()
    log.info(f"✅ Hoàn thành tạo tất cả file CSV yêu cầu trong {end_time - start_time:.2f}s.")


# --- Các hàm tạo CSV cụ thể (internal helpers) ---
async def _create_server_summary_csv(server, bot, server_info, roles, files_list_ref, scan_data):
    headers = ["Metric", "Value"]
    owner_name = "N/A"
    if server.owner: owner_name = server.owner.name
    elif server.owner_id:
        owner = await utils.fetch_user_data(server, server.owner_id, bot_ref=bot)
        if owner: owner_name = owner.name

    total_custom_emojis = len(server.emojis)
    total_custom_stickers = len(server.stickers)
    filtered_reaction_count = sum(scan_data.get("filtered_reaction_emoji_counts", Counter()).values())

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
        ["Total Roles (excl. @everyone)", len(roles)],
        ["Total Custom Emojis", total_custom_emojis],
        ["Total Custom Stickers", total_custom_stickers],
        ["Total Reactions Scanned (Filtered)", filtered_reaction_count if config.ENABLE_REACTION_SCAN else 'N/A']
    ]
    await _write_csv_to_list("server_summary.csv", headers, rows, files_list_ref)

async def _create_scanned_channels_threads_csv(channel_details, bot, files_list_ref):
    headers = [
        "Item Type", "Channel Type", "ID", "Name", "Parent Channel ID", "Parent Channel Name",
        "Category ID", "Category Name", "Created At", "Is NSFW", "Slowmode (s)", "Topic",
        "Message Count (Scan)", "Reaction Count (Filtered)", "Scan Duration (s)",
        "Top Chatter ID", "Top Chatter Name", "Top Chatter Msg Count",
        "Is Archived", "Is Locked", "Thread Owner ID", "Error"
    ]
    rows = []
    for detail in channel_details:
        channel_type_str = detail.get("type", "unknown")
        is_voice = channel_type_str == str(discord.ChannelType.voice)
        top_chatter_id, top_chatter_name, top_chatter_msg_count = "N/A", "N/A", 0
        top_chatter_str = detail.get('top_chatter')
        if isinstance(top_chatter_str, str):
             mention_match = re.search(r'<@!?(\d+)>', top_chatter_str); id_match = re.search(r'ID: `(\d+)`', top_chatter_str)
             name_match = re.search(r'\(`(.*?)`\)', top_chatter_str); count_match = re.search(r'- (\d{1,3}(?:,\d{3})*)\s*tin', top_chatter_str)
             if mention_match: top_chatter_id = mention_match.group(1)
             elif id_match: top_chatter_id = id_match.group(1)
             if name_match: top_chatter_name = name_match.group(1)
             if count_match:
                 try: top_chatter_msg_count = int(count_match.group(1).replace(',', ''))
                 except ValueError: pass
        is_nsfw_str = detail.get('nsfw', ''); is_nsfw = isinstance(is_nsfw_str, str) and is_nsfw_str.startswith(utils.get_emoji('success', bot))
        slowmode_val = utils.parse_slowmode(detail.get('slowmode', '0')) if not is_voice else None
        scan_duration_s = detail.get('duration_seconds', 0)
        topic_val = detail.get('topic', '') if not is_voice and detail.get('processed') else None
        channel_row = [
            "Channel", channel_type_str, detail.get('id', 'N/A'), detail.get('name', 'N/A'), None, None,
            detail.get('category_id', 'N/A'), detail.get('category', 'N/A'),
            detail.get('created_at').isoformat() if detail.get('created_at') else 'N/A',
            is_nsfw, slowmode_val, topic_val, detail.get('message_count', 0),
            detail.get('reaction_count'), scan_duration_s,
            top_chatter_id, top_chatter_name, top_chatter_msg_count,
            None, None, None, detail.get('error', '')
        ]
        rows.append(channel_row)
        if "threads_data" in detail:
            for thread_data in detail.get("threads_data", []):
                thread_row = [
                    "Thread", str(discord.ChannelType.public_thread),
                    thread_data.get('id', 'N/A'), thread_data.get('name', 'N/A'),
                    detail.get('id'), detail.get('name'), detail.get('category_id'), detail.get('category'),
                    thread_data.get('created_at'), None, None, None,
                    thread_data.get('message_count', 0), thread_data.get('reaction_count'),
                    thread_data.get('scan_duration_seconds', 0),
                    None, None, None, thread_data.get('archived'), thread_data.get('locked'), thread_data.get('owner_id'),
                    thread_data.get('error', '')
                ]
                rows.append(thread_row)
    await _write_csv_to_list("scanned_channels_threads.csv", headers, rows, files_list_ref)

async def _create_static_voice_stage_csv(voice_channel_static_data, files_list_ref):
    headers = ["ID", "Name", "Type", "Category ID", "Category Name", "Created At", "User Limit", "Bitrate (bps)"]
    rows = [[vc.get('id'), vc.get('name'), vc.get('type'), vc.get('category_id'), vc.get('category'), vc.get('created_at').isoformat() if vc.get('created_at') else None, vc.get('user_limit'), utils.parse_bitrate(str(vc.get('bitrate','0')))] for vc in voice_channel_static_data]
    await _write_csv_to_list("static_voice_stage_channels.csv", headers, rows, files_list_ref)

async def _create_user_activity_csv(
    user_activity, files_list_ref, user_distinct_channel_counts,
    user_total_custom_emoji_content_counts, user_most_active_channel,
    user_reaction_given_counts,
    # <<< THÊM THAM SỐ MỚI >>>
    user_emoji_received_counts: Optional[defaultdict] = None
):
    headers = [
        "User ID", "Is Bot", "Message Count", "Link Count", "Image Count", "Other File Count",
        "Emoji (Content) Count", "Custom Emoji Server (Content) Count",
        "Sticker Sent Count", "Mention Given Count", "Distinct Mention Given Count",
        "Mention Received Count", "Reply Count", "Reaction Received Count (Filtered)",
        "Reaction Given Count (Filtered)",
        "Top Emoji Received Key", # <<< THÊM HEADER MỚI >>>
        "Distinct Channels Messaged", "Most Active Location ID", "Most Active Location Msg Count",
        "First Seen UTC", "Last Seen UTC", "Activity Span (s)"
    ]
    rows = []
    for user_id, data in user_activity.items():
        first_seen = data.get('first_seen'); last_seen = data.get('last_seen')
        activity_span_secs = data.get('activity_span_seconds', 0)
        distinct_mentions_given = len(data.get('distinct_mentions_set', set()))
        distinct_channels = user_distinct_channel_counts.get(user_id, 0)
        custom_emoji_content_count = user_total_custom_emoji_content_counts.get(user_id, 0)
        most_active_data = user_most_active_channel.get(user_id) if user_most_active_channel else None
        most_active_id = most_active_data[0] if most_active_data else None
        most_active_count = most_active_data[1] if most_active_data else 0
        reaction_given_count = user_reaction_given_counts.get(user_id, 0) if user_reaction_given_counts else 0

        # <<< TÌM TOP EMOJI NHẬN CHO CSV >>>
        top_emoji_received_key = "N/A"
        if user_emoji_received_counts:
            user_specific_counts = user_emoji_received_counts.get(user_id, Counter())
            if user_specific_counts:
                try:
                    most_received_key, _ = user_specific_counts.most_common(1)[0]
                    top_emoji_received_key = str(most_received_key) # Lưu key (ID hoặc Unicode)
                except (ValueError, IndexError): pass
        # <<< KẾT THÚC TÌM TOP EMOJI >>>

        rows.append([
            user_id, data.get('is_bot', False), data.get('message_count', 0),
            data.get('link_count', 0), data.get('image_count', 0), data.get('other_file_count', 0),
            data.get('emoji_count', 0), custom_emoji_content_count,
            data.get('sticker_count', 0),
            data.get('mention_given_count', 0), distinct_mentions_given,
            data.get('mention_received_count', 0), data.get('reply_count', 0),
            data.get('reaction_received_count', 0), reaction_given_count,
            top_emoji_received_key, # <<< THÊM DỮ LIỆU CỘT MỚI >>>
            distinct_channels, most_active_id, most_active_count,
            first_seen.isoformat() if first_seen else None,
            last_seen.isoformat() if last_seen else None,
            round(activity_span_secs, 2)
        ])
    await _write_csv_to_list("user_activity_detail.csv", headers, rows, files_list_ref)

async def _create_roles_detail_csv(roles, files_list_ref):
    headers = ["ID", "Name", "Position", "Color", "Is Hoisted", "Is Mentionable", "Is Bot Role", "Member Count (Scan End)", "Created At", "Permissions Value"]
    rows = [[role.id, role.name, role.position, str(role.color), role.hoist, role.mentionable, role.is_bot_managed(), len(role.members), role.created_at.isoformat(), role.permissions.value] for role in roles]
    await _write_csv_to_list("roles_detail.csv", headers, rows, files_list_ref)

async def _create_boosters_detail_csv(boosters, scan_timestamp, files_list_ref):
    headers = ["User ID", "Username", "Display Name", "Boost Start UTC", "Boost Duration (s)"]
    rows = []
    for member in boosters:
        boost_duration_secs = 0
        if member.premium_since:
            try:
                since_aware = member.premium_since.astimezone(datetime.timezone.utc) if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc)
                scan_aware = scan_timestamp.astimezone(datetime.timezone.utc) if scan_timestamp.tzinfo else scan_timestamp.replace(tzinfo=datetime.timezone.utc)
                if scan_aware >= since_aware: boost_duration_secs = (scan_aware - since_aware).total_seconds()
            except Exception: pass
        rows.append([member.id, member.name, member.display_name, member.premium_since.isoformat() if member.premium_since else None, round(boost_duration_secs, 2) if boost_duration_secs >= 0 else 0])
    await _write_csv_to_list("boosters_detail.csv", headers, rows, files_list_ref)

async def _create_invites_detail_csv(invites, files_list_ref):
    headers = ["Code", "Inviter ID", "Inviter Name", "Channel ID", "Channel Name", "Created At UTC", "Expires At UTC", "Uses", "Max Uses", "Is Temporary"]
    rows = [[inv.code, inv.inviter.id if inv.inviter else None, inv.inviter.name if inv.inviter else None, inv.channel.id if inv.channel else None, inv.channel.name if inv.channel else None, inv.created_at.isoformat() if inv.created_at else None, inv.expires_at.isoformat() if inv.expires_at else None, inv.uses or 0, inv.max_uses or 0, inv.temporary] for inv in invites]
    await _write_csv_to_list("invites_detail.csv", headers, rows, files_list_ref)

async def _create_webhooks_detail_csv(webhooks, files_list_ref):
    headers = ["ID", "Name", "Creator ID", "Creator Name", "Channel ID", "Channel Name", "Created At UTC"]
    rows = [[wh.id, wh.name, wh.user.id if wh.user else None, wh.user.name if wh.user else None, wh.channel_id, getattr(wh.channel, 'name', None), wh.created_at.isoformat() if wh.created_at else None] for wh in webhooks]
    await _write_csv_to_list("webhooks_detail.csv", headers, rows, files_list_ref)

async def _create_integrations_detail_csv(integrations, files_list_ref):
    headers = ["ID", "Name", "Type", "Enabled", "Syncing", "Role ID", "Role Name", "Expire Behaviour", "Expire Grace Period (s)", "Account ID", "Account Name"]
    rows = []
    for integ in integrations:
        integ_type = integ.type if isinstance(integ.type, str) else integ.type.name
        role_id = integ.role.id if hasattr(integ, 'role') and integ.role else None; role_name = integ.role.name if hasattr(integ, 'role') and integ.role else None
        expire_behaviour = integ.expire_behaviour.name if hasattr(integ, 'expire_behaviour') and integ.expire_behaviour else None
        grace_period = integ.expire_grace_period if hasattr(integ, 'expire_grace_period') is not None else None
        syncing = integ.syncing if hasattr(integ, 'syncing') else None
        rows.append([integ.id, integ.name, integ_type, integ.enabled, syncing, role_id, role_name, expire_behaviour, grace_period, integ.account.id if integ.account else None, integ.account.name if integ.account else None])
    await _write_csv_to_list("integrations_detail.csv", headers, rows, files_list_ref)

async def _create_audit_log_detail_csv(audit_logs, files_list_ref):
    headers = ["Log ID", "Timestamp UTC", "Action Type", "User ID", "Target ID", "Reason", "Changes (JSON)"]
    rows = []
    for log_entry in audit_logs:
        extra_data_json = json.dumps(log_entry.get('extra_data'), ensure_ascii=False, default=str) if log_entry.get('extra_data') else ""
        created_at_dt = log_entry.get('created_at'); created_at_iso = created_at_dt.isoformat() if isinstance(created_at_dt, datetime.datetime) else str(created_at_dt)
        rows.append([log_entry.get('log_id'), created_at_iso, log_entry.get('action_type'), log_entry.get('user_id'), log_entry.get('target_id'), log_entry.get('reason'), extra_data_json])
    await _write_csv_to_list("audit_log_detail.csv", headers, rows, files_list_ref)


async def create_top_oldest_members_csv(oldest_members_data: List[Dict[str, Any]], files_list_ref: List[discord.File]):
    if not oldest_members_data: return
    filename = "top_oldest_members.csv"; log.info(f"💾 Đang tạo {filename}...")
    headers = ["Rank", "User ID", "Display Name", "Joined At UTC", "Time in Server (Days Approx)"]
    rows = []
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    for rank, data in enumerate(oldest_members_data, 1):
        joined_at = data.get('joined_at'); days_in_server = "N/A"; joined_at_iso = None
        if isinstance(joined_at, datetime.datetime):
            joined_at_iso = joined_at.isoformat()
            try:
                join_aware = joined_at.astimezone(datetime.timezone.utc) if joined_at.tzinfo else joined_at.replace(tzinfo=datetime.timezone.utc)
                if now_utc >= join_aware: days_in_server = (now_utc - join_aware).days
            except Exception: pass
        rows.append([rank, data.get('id', 'N/A'), data.get('display_name', 'N/A'), joined_at_iso, days_in_server])
    try: await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo {filename}: {ex}", exc_info=True)

async def create_tracked_role_grants_csv(
    tracked_role_grants: collections.Counter, # {(user_id, role_id): count}
    guild: discord.Guild,
    files_list_ref: List[discord.File]
):
    if not tracked_role_grants: return
    filename = "tracked_role_grants.csv"
    log.info(f"💾 Đang tạo {filename}...")
    headers = ["User ID", "Role ID", "Role Name", "Grant Count"]
    rows = []
    for (user_id, role_id), count in tracked_role_grants.items():

        if count > 0:
            role = guild.get_role(role_id)
            role_name = role.name if role else "Unknown/Deleted Role"
            rows.append([user_id, role_id, role_name, count])
    rows.sort(key=lambda x: (str(x[0]), str(x[1])))
    try: await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo {filename}: {ex}", exc_info=True)

async def create_keyword_csv_reports(
    keyword_totals: collections.Counter, keyword_by_channel: Dict[int, collections.Counter],
    keyword_by_thread: Dict[int, collections.Counter], keyword_by_user: Dict[int, collections.Counter],
    keywords_searched: List[str], files_list_ref: List[discord.File]
):
    if not keywords_searched or not keyword_totals: log.debug("Bỏ qua tạo keyword CSVs: không có keyword hoặc không tìm thấy."); return
    try:
        kw_sum_headers = ["Keyword", "Total Count"]; kw_sum_rows = sorted(list(keyword_totals.items()), key=lambda item: item[1], reverse=True)
        await _write_csv_to_list("keyword_summary.csv", kw_sum_headers, kw_sum_rows, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo keyword_summary.csv: {ex}", exc_info=True)
    try:
        kw_loc_headers = ["Location ID (Channel/Thread)", "Keyword", "Count"]; kw_loc_rows = []
        all_location_counts = {**keyword_by_channel, **keyword_by_thread}
        for loc_id, counts in all_location_counts.items():
            for keyword, count in counts.items(): kw_loc_rows.append([loc_id, keyword, count])
        kw_loc_rows.sort(key=lambda x: (str(x[0]), x[1])); await _write_csv_to_list("keyword_by_location.csv", kw_loc_headers, kw_loc_rows, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo keyword_by_location.csv: {ex}", exc_info=True)
    try:
        kw_user_headers = ["User ID", "Keyword", "Count"]; kw_user_rows = []
        for user_id, counts in keyword_by_user.items():
            for keyword, count in counts.items(): kw_user_rows.append([user_id, keyword, count])
        kw_user_rows.sort(key=lambda x: (str(x[0]), x[1])); await _write_csv_to_list("keyword_by_user.csv", kw_user_headers, kw_user_rows, files_list_ref)
    except Exception as ex: log.error(f"‼️ LỖI tạo keyword_by_user.csv: {ex}", exc_info=True)

# --- END OF FILE reporting/csv_writer.py ---