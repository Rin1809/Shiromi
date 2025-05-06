# --- START OF FILE cogs/deep_scan_helpers/data_processing.py ---
import discord
from discord.ext import commands
import logging
import asyncio
import time
import datetime
from typing import Dict, Any, List, Union, Optional, Counter as TypingCounter, Set, Tuple
from collections import Counter, defaultdict

import config
import utils
import database

log = logging.getLogger(__name__)

# --- Thêm hằng số cho batch audit log ---
AUDIT_LOG_API_FETCH_LIMIT_PER_REQUEST = 100 # Discord API limit per request
AUDIT_LOG_BATCH_INSERT_SIZE = 200 # Số lượng entry audit log để ghi vào DB một lần
_audit_log_insert_batch_buffer: List[Dict[str, Any]] = [] # Buffer để gom entry trước khi ghi DB


# --- Hàm mới để ghi batch audit log vào DB ---
async def _flush_audit_log_insert_batch_to_db(
    batch_entries_data: List[Dict[str, Any]],
    scan_data: Dict[str, Any]
):
    if not batch_entries_data:
        return
    log.debug(f"Flushing {len(batch_entries_data)} audit log entries (đã serialize) to DB...")
    if not database.pool:
        log.error("DB pool not available for flushing audit logs.")
        scan_data["scan_errors"].append("Lỗi DB: Không thể flush audit logs (pool unavailable).")
        return
    try:
        async with database.pool.acquire() as conn:
            query = """
                INSERT INTO audit_log_cache (log_id, guild_id, user_id, target_id, action_type, reason, created_at, extra_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (log_id) DO UPDATE SET
                    guild_id = EXCLUDED.guild_id, user_id = EXCLUDED.user_id, target_id = EXCLUDED.target_id,
                    action_type = EXCLUDED.action_type, reason = EXCLUDED.reason, created_at = EXCLUDED.created_at,
                    extra_data = EXCLUDED.extra_data;
            """
            # Tạo list các tuple từ list các dict
            data_tuples_to_insert = [
                (
                    entry_data['log_id'], entry_data['guild_id'], entry_data['user_id'],
                    entry_data['target_id'], entry_data['action_type'], entry_data['reason'],
                    entry_data['created_at'], entry_data['extra_data']
                ) for entry_data in batch_entries_data
            ]
            await conn.executemany(query, data_tuples_to_insert)
            log.info(f"Successfully flushed {len(data_tuples_to_insert)} audit log entries to DB.")
    except Exception as e_flush:
        log.error(f"Error flushing audit log batch to DB: {e_flush}", exc_info=True)
        scan_data["scan_errors"].append(f"Lỗi DB: Flush audit logs thất bại ({len(batch_entries_data)} entries).")


async def process_additional_data(scan_data: Dict[str, Any]):
    """
    Fetch và xử lý dữ liệu phụ trợ sau khi quét xong kênh/luồng.
    Bao gồm: boosters, voice static info, invites, webhooks, integrations,
    oldest members, audit log scan & analysis (role grant tracking).
    Tính toán user channel counts, most active channel, activity span.
    Cập nhật scan_data với dữ liệu đã xử lý.
    """
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    scan_errors: List[str] = scan_data["scan_errors"]
    current_members_list: List[discord.Member] = scan_data["current_members_list"]
    user_activity = scan_data.get("user_activity", {})

    log.info(f"\n--- [bold green]{e('stats')} Xử lý Dữ liệu & Tạo Báo cáo cho {server.name}[/bold green] ---")
    _log_scan_summary(scan_data)

    log.info(f"{e('loading')} Đang fetch/tính toán dữ liệu phụ trợ...")

    # --- Lấy Boosters ---
    try:
        scan_data["boosters"] = sorted(
            [m for m in current_members_list if m.premium_since is not None],
            key=lambda m: m.premium_since or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
        )
        log.info(f"{e('boost')} Boosters: {len(scan_data['boosters'])}")
    except Exception as booster_err:
        log.error(f"Lỗi khi lấy danh sách boosters: {booster_err}")
        scan_errors.append(f"Lỗi lấy boosters: {booster_err}")
        scan_data["boosters"] = []

    # --- Lấy thông tin tĩnh kênh Voice/Stage ---
    await _fetch_static_voice_stage_info(scan_data)

    # --- Fetch Invites, Webhooks, Integrations ---
    await _fetch_invites_webhooks_integrations(scan_data)

    # --- Lấy Top Oldest Members ---
    await _fetch_top_oldest_members(scan_data)

    # --- Quét và Phân tích Audit Log (Tập trung vào Role Grant Tracking) ---
    await _scan_and_analyze_audit_logs(scan_data) # Hàm này đã được sửa đổi bên dưới

    # --- Tính toán các chỉ số phụ từ user_activity và user_channel_message_counts ---
    log.info(f"{e('stats')} Đang tính toán các chỉ số phụ của user...")
    user_channel_counts = scan_data.setdefault('user_distinct_channel_counts', Counter())
    user_most_active_channel_data = scan_data.setdefault('user_most_active_channel', {})
    user_channel_msg_counts = scan_data.get('user_channel_message_counts', {})
    calculated_distinct_channels = 0; calculated_most_active = 0; calculated_span = 0

    for user_id, data in user_activity.items():
        channels_set = data.get('channels_messaged_in', set())
        distinct_count = len(channels_set)
        if distinct_count > 0: user_channel_counts[user_id] = distinct_count; calculated_distinct_channels += 1
        channel_counts_for_user: Optional[Dict[int, int]] = user_channel_msg_counts.get(user_id)
        if channel_counts_for_user:
            try:
                most_active_location_id, message_count = max(channel_counts_for_user.items(), key=lambda item: item[1])
                user_most_active_channel_data[user_id] = (most_active_location_id, message_count); calculated_most_active += 1
            except ValueError: pass
            except Exception as e_most_active: log.error(f"Lỗi khi tính kênh HĐ nhiều nhất cho user {user_id}: {e_most_active}")
        first_seen = data.get('first_seen'); last_seen = data.get('last_seen')
        if first_seen and last_seen and last_seen >= first_seen:
            try:
                first_aware = first_seen if first_seen.tzinfo else first_seen.replace(tzinfo=datetime.timezone.utc)
                last_aware = last_seen if last_seen.tzinfo else last_seen.replace(tzinfo=datetime.timezone.utc)
                if last_aware >= first_aware:
                    span_seconds = (last_aware - first_aware).total_seconds()
                    if span_seconds > 0: data['activity_span_seconds'] = round(span_seconds, 2); calculated_span += 1
                    else: data['activity_span_seconds'] = 0.0
                else: log.warning(f"Dữ liệu first/last seen không hợp lệ cho user {user_id}. Đặt span = 0."); data['activity_span_seconds'] = 0.0
            except Exception as e_span: log.warning(f"Lỗi tính activity span cho user {user_id}: {e_span}"); data['activity_span_seconds'] = 0.0
        else: data['activity_span_seconds'] = 0.0

    log.info(f"Đã tính toán số kênh hoạt động cho {calculated_distinct_channels} users.")
    log.info(f"Đã tính toán kênh hoạt động nhiều nhất cho {calculated_most_active} users.")
    log.info(f"Đã tính toán khoảng thời gian hoạt động cho {calculated_span} users.")
    log.info("Hoàn thành fetch và xử lý dữ liệu phụ trợ.")


def _log_scan_summary(scan_data: Dict[str, Any]):
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    scan_end_time = scan_data.get("scan_end_time")
    overall_start_time = scan_data["overall_start_time"]
    overall_duration = scan_end_time - overall_start_time if scan_end_time else datetime.timedelta(0)
    scan_data["overall_duration"] = overall_duration
    channel_details: List[Dict[str, Any]] = scan_data["channel_details"]
    final_processed_channels = scan_data.get("processed_channels_count", 0)
    final_processed_threads = scan_data.get("processed_threads_count", 0)
    final_skipped_channels_actual = sum(1 for d in channel_details if not d.get("processed") and d.get("type") != str(discord.ChannelType.public_thread)) # Chỉ kênh
    final_skipped_threads_actual = sum(1 for d in channel_details if not d.get("processed") and d.get("type") == str(discord.ChannelType.public_thread)) # Chỉ luồng
    for detail in channel_details: # Cộng thêm luồng con của kênh đã xử lý nhưng luồng con bị lỗi
        if detail.get("processed") and detail.get("type") != str(discord.ChannelType.public_thread):
            final_skipped_threads_actual += sum(1 for t_data in detail.get("threads_data", []) if not t_data.get("processed") or t_data.get("error"))

    filtered_reaction_count = sum(scan_data.get("filtered_reaction_emoji_counts", Counter()).values())
    log.info(f"{e('clock')} Tổng TG quét tin nhắn: [bold magenta]{utils.format_timedelta(overall_duration, high_precision=True)}[/bold magenta]")
    log.info(f"{e('text_channel')}/{e('voice_channel')} Kênh Text/Voice: {final_processed_channels} xử lý, {final_skipped_channels_actual} bỏ qua/lỗi")
    log.info(f"{e('thread')} Luồng: {final_processed_threads} xử lý, {final_skipped_threads_actual} bỏ qua/lỗi")
    log.info(f"{e('stats')} Tổng tin nhắn: {scan_data['overall_total_message_count']:,}")
    if scan_data.get("can_scan_reactions"): log.info(f"{e('reaction')} Tổng biểu cảm (Lọc): {filtered_reaction_count:,}")
    log.info(f"{e('members')} Users có hoạt động: {len(scan_data['user_activity']):,}")


async def _fetch_static_voice_stage_info(scan_data: Dict[str, Any]):
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    voice_channel_static_data: List[Dict[str, Any]] = []
    skipped_voice_info_count = 0
    static_voice_stage_channels = server.voice_channels + server.stage_channels
    for vc in static_voice_stage_channels:
        try:
            if vc.permissions_for(server.me).view_channel:
                voice_channel_static_data.append({
                    "name": vc.name, "id": vc.id, "type": str(vc.type),
                    "category": vc.category.name if vc.category else "N/A",
                    "category_id": vc.category.id if vc.category else None,
                    "user_limit": vc.user_limit or "Không giới hạn",
                    "bitrate": f"{vc.bitrate // 1000} kbps", "created_at": vc.created_at
                })
            else: skipped_voice_info_count += 1; scan_errors.append(f"Info Kênh Voice/Stage #{vc.name}: Bỏ qua (Thiếu View)."); log.warning(f"Bỏ qua info kênh voice/stage #{vc.name}: Thiếu View.")
        except Exception as vc_info_err: skipped_voice_info_count += 1; error_msg = f"Lỗi lấy info kênh voice/stage #{vc.name}: {vc_info_err}"; log.error(f"{utils.get_emoji('error', bot)} {error_msg}", exc_info=True); scan_errors.append(error_msg)
    scan_data["voice_channel_static_data"] = voice_channel_static_data
    log.info(f"{utils.get_emoji('voice_channel', bot)} Static Voice/Stage Info: {len(voice_channel_static_data)} ({skipped_voice_info_count} bỏ qua).")


async def _fetch_invites_webhooks_integrations(scan_data: Dict[str, Any]):
    server: discord.Guild = scan_data["server"]; bot: commands.Bot = scan_data["bot"]; scan_errors: List[str] = scan_data["scan_errors"]; e = lambda name: utils.get_emoji(name, bot)
    if scan_data.get("can_scan_invites", False):
        try:
            invites_data = await server.invites(); scan_data["invites_data"] = invites_data
            invite_usage_counter = Counter({inv.inviter.id: inv.uses for inv in invites_data if inv.inviter and inv.uses is not None})
            scan_data["invite_usage_counts"] = invite_usage_counter
            log.info(f"{e('invite')} Fetched {len(invites_data)} invites. Calculated {len(invite_usage_counter)} inviter usages.")
        except Exception as e_inv: log.error(f"{e('error')} Lỗi fetch invites: {e_inv}", exc_info=True); scan_errors.append(f"Lỗi lấy invites: {e_inv}"); scan_data["invites_data"] = []; scan_data["invite_usage_counts"] = Counter()
    else: log.info(f"{e('info')} Bỏ qua fetch invites do thiếu quyền.")
    if scan_data.get("can_scan_webhooks", False):
        try: webhooks_data = await server.webhooks(); scan_data["webhooks_data"] = webhooks_data; log.info(f"{e('webhook')} Fetched {len(webhooks_data)} webhooks.")
        except Exception as e_wh: log.error(f"{e('error')} Lỗi fetch webhooks: {e_wh}", exc_info=True); scan_errors.append(f"Lỗi lấy webhooks: {e_wh}"); scan_data["webhooks_data"] = []
    else: log.info(f"{e('info')} Bỏ qua fetch webhooks do thiếu quyền.")
    if scan_data.get("can_scan_integrations", False):
        try: integrations_data = await server.integrations(); scan_data["integrations_data"] = integrations_data; log.info(f"{e('integration')} Fetched {len(integrations_data)} integrations.")
        except Exception as e_int: log.error(f"{e('error')} Lỗi fetch integrations: {e_int}", exc_info=True); scan_errors.append(f"Lỗi lấy integrations: {e_int}"); scan_data["integrations_data"] = []
    else: log.info(f"{e('info')} Bỏ qua fetch integrations do thiếu quyền.")


async def _fetch_top_oldest_members(scan_data: Dict[str, Any]):
    server: discord.Guild = scan_data["server"]; bot: commands.Bot = scan_data["bot"]; scan_errors: List[str] = scan_data["scan_errors"]; e = lambda name: utils.get_emoji(name, bot); current_members_list: List[discord.Member] = scan_data["current_members_list"]; oldest_members_data: List[Dict[str, Any]] = []
    from reporting.embeds_user import TOP_OLDEST_MEMBERS_LIMIT
    log.info(f"{e('calendar')} Đang xác định thành viên lâu năm nhất...")
    try:
        human_members_with_join = sorted([m for m in current_members_list if not m.bot and m.joined_at is not None], key=lambda m: m.joined_at)
        for member in human_members_with_join[:TOP_OLDEST_MEMBERS_LIMIT]: oldest_members_data.append({"id": member.id, "mention": member.mention, "display_name": member.display_name, "joined_at": member.joined_at})
        scan_data["oldest_members_data"] = oldest_members_data; log.info(f"Đã xác định top {len(oldest_members_data)} thành viên lâu năm nhất.")
    except Exception as oldest_err: log.error(f"Lỗi xác định thành viên lâu năm: {oldest_err}", exc_info=True); scan_errors.append(f"Lỗi lấy top thành viên lâu năm: {oldest_err}"); scan_data["oldest_members_data"] = []


async def _scan_and_analyze_audit_logs(scan_data: Dict[str, Any]):
    global _audit_log_insert_batch_buffer
    server: discord.Guild = scan_data["server"]; bot: commands.Bot = scan_data["bot"]; scan_errors: List[str] = scan_data["scan_errors"]; e = lambda name: utils.get_emoji(name, bot); can_scan_audit_log = scan_data.get("can_scan_audit_log", False); tracked_role_ids: Set[int] = config.TRACKED_ROLE_GRANT_IDS
    user_tracked_grants: Counter = scan_data.setdefault('tracked_role_grant_counts', Counter())
    user_thread_creation_counts = scan_data.setdefault('user_thread_creation_counts', Counter())
    _audit_log_insert_batch_buffer.clear() # Xóa buffer từ lần quét trước (nếu có)

    if not can_scan_audit_log: log.info("Bỏ qua quét Audit Log do thiếu quyền."); return
    audit_scan_start_time = discord.utils.utcnow(); log.info(f"[bold]{e('shield')} Bắt đầu quét Audit Log từ Discord API...[/bold]"); audit_log_entries_added = 0; newest_processed_id: Optional[int] = None
    try:
        last_scanned_log_id = await database.get_newest_audit_log_id_from_db(server.id); log.info(f"Audit log ID cuối đã quét từ DB: {last_scanned_log_id or 'Chưa có'}")
        current_after_id = last_scanned_log_id; max_iterations = 20; processed_in_this_scan = 0; newest_id_in_scan = last_scanned_log_id
        log.info(f"Bắt đầu fetch audit logs sau ID: {current_after_id or 'Ban đầu'}...")
        for iteration in range(max_iterations):
            logs_in_batch: List[discord.AuditLogEntry] = []; batch_fetch_start_time = time.monotonic()
            try:
                audit_iterator = server.audit_logs(limit=AUDIT_LOG_API_FETCH_LIMIT_PER_REQUEST, after=discord.Object(id=current_after_id) if current_after_id else None, oldest_first=True)
                relevant_logs_in_batch = [entry async for entry in audit_iterator if entry.action in config.AUDIT_LOG_ACTIONS_TO_TRACK]
                log.debug(f"  Audit log batch {iteration+1}: Fetched, found {len(relevant_logs_in_batch)} relevant action(s) in {time.monotonic() - batch_fetch_start_time:.2f}s.")
            except Exception as audit_fetch_err: log.error(f"  {e('error')} Lỗi fetch audit log batch {iteration+1}: {audit_fetch_err}", exc_info=True); scan_errors.append(f"Lỗi fetch Audit Log: {audit_fetch_err}"); break
            if not relevant_logs_in_batch: log.info(f"  Không tìm thấy entry audit log mới phù hợp (batch {iteration+1})."); break

            batch_newest_id_processed = None
            for entry in relevant_logs_in_batch:
                try:
                    # Serialize entry data cho batch insert
                    user_id_entry = entry.user.id if entry.user else None; target_id_entry: Optional[int] = None
                    target_obj_entry = entry.target
                    try:
                        if isinstance(target_obj_entry, discord.abc.Snowflake): target_id_entry = target_obj_entry.id
                        elif isinstance(target_obj_entry, dict) and 'id' in target_obj_entry: target_id_entry = int(target_obj_entry['id'])
                    except (ValueError, TypeError, AttributeError): pass
                    extra_data_entry = database._serialize_changes(entry.changes)
                    created_at_aware_entry = entry.created_at
                    if created_at_aware_entry.tzinfo is None: created_at_aware_entry = created_at_aware_entry.replace(tzinfo=datetime.timezone.utc)
                    _audit_log_insert_batch_buffer.append({
                        "log_id": entry.id, "guild_id": entry.guild.id, "user_id": user_id_entry,
                        "target_id": target_id_entry, "action_type": str(entry.action.name),
                        "reason": entry.reason, "created_at": created_at_aware_entry, "extra_data": extra_data_entry
                    })
                    audit_log_entries_added += 1; processed_in_this_scan += 1
                    if newest_id_in_scan is None or entry.id > newest_id_in_scan: newest_id_in_scan = entry.id
                    if batch_newest_id_processed is None or entry.id > batch_newest_id_processed: batch_newest_id_processed = entry.id
                    if entry.action == discord.AuditLogAction.thread_create and entry.user and not entry.user.bot: user_thread_creation_counts[entry.user.id] += 1
                    if len(_audit_log_insert_batch_buffer) >= AUDIT_LOG_BATCH_INSERT_SIZE:
                        await _flush_audit_log_insert_batch_to_db(_audit_log_insert_batch_buffer, scan_data)
                        _audit_log_insert_batch_buffer.clear()
                except Exception as entry_proc_err: log.error(f"  Lỗi xử lý audit log entry {entry.id}: {entry_proc_err}"); scan_errors.append(f"Lỗi xử lý Audit Log Entry ID: {entry.id}")
            log.debug(f"  Đã thêm {len(relevant_logs_in_batch)} entry vào buffer. ID mới nhất xử lý batch: {batch_newest_id_processed}")
            if len(relevant_logs_in_batch) < AUDIT_LOG_API_FETCH_LIMIT_PER_REQUEST or batch_newest_id_processed is None:
                log.info(f"  Đã fetch hết audit log mới hoặc batch không đầy/xử lý được (batch {iteration+1}). Dừng fetch.")
                break
            else: current_after_id = batch_newest_id_processed; log.info(f"  Fetch đầy batch, tiếp tục fetch sau ID: {current_after_id}..."); await asyncio.sleep(0.5)
            if iteration == max_iterations - 1: log.warning(f"Đạt giới hạn {max_iterations} lần fetch audit log."); scan_errors.append("Quét Audit Log dừng do giới hạn fetch."); break
        if _audit_log_insert_batch_buffer: # Flush nốt buffer còn lại
            await _flush_audit_log_insert_batch_to_db(_audit_log_insert_batch_buffer, scan_data)
            _audit_log_insert_batch_buffer.clear()
        newest_processed_id = newest_id_in_scan; scan_data["audit_log_entries_added"] = audit_log_entries_added; scan_data["newest_processed_audit_log_id"] = newest_processed_id
        if newest_processed_id and newest_processed_id != last_scanned_log_id: log.info(f"Cập nhật ID audit log mới nhất vào DB: {newest_processed_id}"); await database.update_newest_audit_log_id(server.id, newest_processed_id)
        scan_data["audit_log_scan_duration"] = discord.utils.utcnow() - audit_scan_start_time
        log.info(f"{e('success')} Hoàn thành quét Audit Log từ API. Thêm {audit_log_entries_added} entry vào DB trong [magenta]{utils.format_timedelta(scan_data['audit_log_scan_duration'])}[/].")
    except Exception as audit_err:
        log.error(f"{e('error')} Lỗi xử lý Audit Log: {audit_err}", exc_info=True); scan_errors.append(f"Lỗi xử lý Audit Log: {audit_err}")
        scan_data["audit_log_scan_duration"] = discord.utils.utcnow() - audit_scan_start_time
        if _audit_log_insert_batch_buffer: # Flush nếu lỗi
            log.warning("Flushing audit log buffer do có lỗi giữa chừng..."); await _flush_audit_log_insert_batch_to_db(_audit_log_insert_batch_buffer, scan_data); _audit_log_insert_batch_buffer.clear()

    if not tracked_role_ids: log.info("Không có role grant nào được cấu hình để theo dõi, bỏ qua phân tích."); return
    log.info(f"{e('role')} Đang phân tích thống kê role grant đặc biệt từ Audit Log trong DB...")
    try:
        role_update_logs = await database.get_audit_logs_for_report(server.id, limit=None, action_filter=[discord.AuditLogAction.member_role_update])
        log.info(f"Phân tích {len(role_update_logs)} member_role_update entry từ DB cho role grant tracking...")
        processed_role_logs = 0
        for log_entry_dict in role_update_logs:
            target_id_str = log_entry_dict.get('target_id'); extra_data = log_entry_dict.get('extra_data')
            if not target_id_str or not isinstance(extra_data, dict): continue
            try: target_id = int(target_id_str)
            except (ValueError, TypeError): continue
            before_roles_data = extra_data.get('before', {}).get('roles', []); after_roles_data = extra_data.get('after', {}).get('roles', [])
            before_role_ids = set();
            for r_data in before_roles_data:
                if isinstance(r_data, dict) and 'id' in r_data:
                    try: before_role_ids.add(int(r_data['id']))
                    except (ValueError, TypeError): pass
            after_role_ids = set()
            for r_data in after_roles_data:
                 if isinstance(r_data, dict) and 'id' in r_data:
                    try: after_role_ids.add(int(r_data['id']))
                    except (ValueError, TypeError): pass
            added_role_ids = after_role_ids - before_role_ids
            for added_role_id in added_role_ids:
                if added_role_id in tracked_role_ids: grant_key: Tuple[int, int] = (target_id, added_role_id); user_tracked_grants[grant_key] += 1
            processed_role_logs += 1
        log.info(f"Đã phân tích {processed_role_logs} role update log entry. Tìm thấy {len(user_tracked_grants)} grant records.")
    except Exception as role_stat_err: log.error(f"{e('error')} Lỗi phân tích thống kê role grant đặc biệt: {role_stat_err}", exc_info=True); scan_errors.append(f"Lỗi phân tích thống kê role grant: {role_stat_err}")
    await asyncio.sleep(0.1)
# --- END OF FILE cogs/deep_scan_helpers/data_processing.py ---