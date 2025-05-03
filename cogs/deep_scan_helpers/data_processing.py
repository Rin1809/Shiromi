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
    _log_scan_summary(scan_data) # Log tóm tắt quét

    log.info(f"{e('loading')} Đang fetch/tính toán dữ liệu phụ trợ...")

    # --- Lấy Boosters ---
    try:
        # Lấy từ current_members_list đã fetch ở init_scan
        scan_data["boosters"] = sorted(
            [m for m in current_members_list if m.premium_since is not None],
            key=lambda m: m.premium_since or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc) # Sắp xếp theo tgian boost sớm nhất
        )
        log.info(f"{e('boost')} Boosters: {len(scan_data['boosters'])}")
    except Exception as booster_err:
        log.error(f"Lỗi khi lấy danh sách boosters: {booster_err}")
        scan_errors.append(f"Lỗi lấy boosters: {booster_err}")
        scan_data["boosters"] = [] 

    # --- Lấy thông tin tĩnh kênh Voice/Stage ---
    await _fetch_static_voice_stage_info(scan_data)

    # --- Fetch Invites, Webhooks, Integrations (cho export, nếu cần) ---c
    await _fetch_invites_webhooks_integrations(scan_data)

    # --- Lấy Top Oldest Members ---
    await _fetch_top_oldest_members(scan_data)

    # --- Quét và Phân tích Audit Log (Tập trung vào Role Grant Tracking) ---
    await _scan_and_analyze_audit_logs(scan_data)

    # --- Tính toán các chỉ số phụ từ user_activity và user_channel_message_counts ---
    log.info(f"{e('stats')} Đang tính toán các chỉ số phụ của user...")
    # Lấy hoặc tạo các dict/counter cần thiết từ scan_data
    user_channel_counts = scan_data.setdefault('user_distinct_channel_counts', Counter())
    user_most_active_channel_data = scan_data.setdefault('user_most_active_channel', {})
    user_channel_msg_counts = scan_data.get('user_channel_message_counts', {})

    calculated_distinct_channels = 0
    calculated_most_active = 0
    calculated_span = 0

    # Duyệt qua user_activity để tính toán
    for user_id, data in user_activity.items():
        # 1. Tính distinct channels từ set đã lưu trong quá trình quét
        channels_set = data.get('channels_messaged_in', set())
        distinct_count = len(channels_set)
        if distinct_count > 0:
            user_channel_counts[user_id] = distinct_count
            calculated_distinct_channels += 1

        # 2. Tính kênh hoạt động nhiều nhất từ user_channel_message_counts
        channel_counts_for_user: Optional[Dict[int, int]] = user_channel_msg_counts.get(user_id)
        if channel_counts_for_user:
            try:
                # Tìm location_id có số count lớn nhất
                # Sử dụng max với key là lambda để lấy giá trị từ dict
                most_active_location_id, message_count = max(channel_counts_for_user.items(), key=lambda item: item[1])
                # Lưu kết quả vào dict user_most_active_channel
                user_most_active_channel_data[user_id] = (most_active_location_id, message_count)
                calculated_most_active += 1
            except ValueError:
                # Xảy ra nếu channel_counts_for_user rỗng (dù không nên)
                pass
            except Exception as e_most_active:
                log.error(f"Lỗi khi tính kênh hoạt động nhiều nhất cho user {user_id}: {e_most_active}")

        # 3. Tính activity span (giữ nguyên logic cũ)
        first_seen = data.get('first_seen')
        last_seen = data.get('last_seen')
        if first_seen and last_seen and last_seen >= first_seen:
            try:
                # Đảm bảo cả hai đều aware hoặc naive cùng timezone UTC
                first_aware = first_seen if first_seen.tzinfo else first_seen.replace(tzinfo=datetime.timezone.utc)
                last_aware = last_seen if last_seen.tzinfo else last_seen.replace(tzinfo=datetime.timezone.utc)

                if last_aware >= first_aware:
                    span_seconds = (last_aware - first_aware).total_seconds()
                    # Chỉ lưu nếu span > 0 để tránh các trường hợp lạ
                    if span_seconds > 0:
                         data['activity_span_seconds'] = round(span_seconds, 2)
                         calculated_span += 1
                    else:
                         data['activity_span_seconds'] = 0.0
                else: # last < first? Lỗi dữ liệu?
                    log.warning(f"Dữ liệu first/last seen không hợp lệ cho user {user_id}. Đặt span = 0.")
                    data['activity_span_seconds'] = 0.0
            except Exception as e_span:
                log.warning(f"Lỗi tính activity span cho user {user_id}: {e_span}")
                data['activity_span_seconds'] = 0.0 # Đặt về 0 nếu lỗi
        else:
            data['activity_span_seconds'] = 0.0 # Mặc định là 0 nếu thiếu first/last

    log.info(f"Đã tính toán số kênh hoạt động cho {calculated_distinct_channels} users.")
    log.info(f"Đã tính toán kênh hoạt động nhiều nhất cho {calculated_most_active} users.")
    log.info(f"Đã tính toán khoảng thời gian hoạt động cho {calculated_span} users.")
    # *** KẾT THÚC TÍNH TOÁN CHỈ SỐ PHỤ ***

    log.info("Hoàn thành fetch và xử lý dữ liệu phụ trợ.")


# --- Các hàm helper (_log_scan_summary, etc.) ---
def _log_scan_summary(scan_data: Dict[str, Any]):
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    scan_end_time = scan_data.get("scan_end_time")
    overall_start_time = scan_data["overall_start_time"]
    overall_duration = scan_end_time - overall_start_time if scan_end_time else datetime.timedelta(0)
    scan_data["overall_duration"] = overall_duration # Cập nhật lại duration

    channel_details: List[Dict[str, Any]] = scan_data["channel_details"]
    final_processed_channels = scan_data.get("processed_channels_count", 0) # Lấy từ scan_data
    final_processed_threads = scan_data.get("processed_threads_count", 0) # Lấy từ scan_data

    # Tính lại skipped dựa trên trạng thái 'processed' cuối cùng
    final_skipped_channels_actual = sum(1 for d in channel_details if not d.get("processed"))
    final_skipped_threads_actual = 0
    for detail in channel_details:
        threads_data = detail.get("threads_data", [])
        if isinstance(threads_data, list):
            # Đếm thread có lỗi hoặc không được xử lý (ví dụ do thiếu quyền)
            final_skipped_threads_actual += sum(1 for t in threads_data if t.get("error"))


    filtered_reaction_count = sum(scan_data.get("filtered_reaction_emoji_counts", Counter()).values())

    log.info(f"{e('clock')} Tổng TG quét tin nhắn: [bold magenta]{utils.format_timedelta(overall_duration, high_precision=True)}[/bold magenta]")
    log.info(f"{e('text_channel')}/{e('voice_channel')} Kênh Text/Voice: {final_processed_channels} xử lý, {final_skipped_channels_actual} bỏ qua/lỗi")
    log.info(f"{e('thread')} Luồng: {final_processed_threads} xử lý, {final_skipped_threads_actual} bỏ qua/lỗi")
    log.info(f"{e('stats')} Tổng tin nhắn: {scan_data['overall_total_message_count']:,}")
    if scan_data.get("can_scan_reactions"):
        log.info(f"{e('reaction')} Tổng biểu cảm (Lọc): {filtered_reaction_count:,}")
    log.info(f"{e('members')} Users có hoạt động: {len(scan_data['user_activity']):,}")


async def _fetch_static_voice_stage_info(scan_data: Dict[str, Any]):
    # Giữ nguyên logic hàm này
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    voice_channel_static_data: List[Dict[str, Any]] = []
    skipped_voice_info_count = 0
    # Lấy kênh từ cache của server
    static_voice_stage_channels = server.voice_channels + server.stage_channels

    for vc in static_voice_stage_channels:
        try:
            # Kiểm tra quyền xem kênh
            if vc.permissions_for(server.me).view_channel:
                voice_channel_static_data.append({
                    # "channel_obj": vc, # Không lưu object để tránh vấn đề serialization/memory
                    "name": vc.name,
                    "id": vc.id,
                    "type": str(vc.type),
                    "category": vc.category.name if vc.category else "N/A",
                    "category_id": vc.category.id if vc.category else None,
                    "user_limit": vc.user_limit or "Không giới hạn",
                    "bitrate": f"{vc.bitrate // 1000} kbps", # Giữ nguyên đơn vị kbps
                    "created_at": vc.created_at
                })
            else:
                skipped_voice_info_count += 1
                error_msg = f"Info Kênh Voice/Stage #{vc.name}: Bỏ qua (Thiếu View)."
                scan_errors.append(error_msg)
                log.warning(f"Bỏ qua info kênh voice/stage #{vc.name}: Thiếu View.")
        except Exception as vc_info_err:
            skipped_voice_info_count += 1
            error_msg = f"Lỗi lấy info kênh voice/stage #{vc.name}: {vc_info_err}"
            log.error(f"{utils.get_emoji('error', bot)} {error_msg}", exc_info=True)
            scan_errors.append(error_msg)

    scan_data["voice_channel_static_data"] = voice_channel_static_data
    log.info(f"{utils.get_emoji('voice_channel', bot)} Static Voice/Stage Info: {len(voice_channel_static_data)} ({skipped_voice_info_count} bỏ qua).")


async def _fetch_invites_webhooks_integrations(scan_data: Dict[str, Any]):
    # Giữ nguyên logic hàm này
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    e = lambda name: utils.get_emoji(name, bot)

    # Fetch Invites
    if scan_data.get("can_scan_invites", False):
        try:
            invites_data = await server.invites()
            scan_data["invites_data"] = invites_data
            # Tính invite usage counts
            invite_usage_counter = Counter()
            for inv in invites_data:
                 if inv.inviter and inv.uses is not None:
                     invite_usage_counter[inv.inviter.id] += inv.uses # Cộng dồn uses của cùng inviter
            scan_data["invite_usage_counts"] = invite_usage_counter
            log.info(f"{e('invite')} Fetched {len(invites_data)} invites. Calculated {len(invite_usage_counter)} inviter usages.")
        except discord.Forbidden: log.error(f"{e('error')} Lỗi quyền fetch invites."); scan_errors.append("Lỗi quyền lấy invites."); scan_data["invites_data"] = []; scan_data["invite_usage_counts"] = Counter()
        except discord.HTTPException as e_inv_http: log.error(f"{e('error')} Lỗi HTTP fetch invites: {e_inv_http.status}"); scan_errors.append(f"Lỗi HTTP lấy invites ({e_inv_http.status})."); scan_data["invites_data"] = []; scan_data["invite_usage_counts"] = Counter()
        except Exception as e_inv: log.error(f"{e('error')} Lỗi fetch invites: {e_inv}", exc_info=True); scan_errors.append(f"Lỗi lấy invites: {e_inv}"); scan_data["invites_data"] = []; scan_data["invite_usage_counts"] = Counter()
    else: log.info(f"{e('info')} Bỏ qua fetch invites do thiếu quyền.")

    # Fetch Webhooks
    if scan_data.get("can_scan_webhooks", False):
        try:
            webhooks_data = await server.webhooks(); scan_data["webhooks_data"] = webhooks_data
            log.info(f"{e('webhook')} Fetched {len(webhooks_data)} webhooks.")
        except discord.Forbidden: log.error(f"{e('error')} Lỗi quyền fetch webhooks."); scan_errors.append("Lỗi quyền lấy webhooks."); scan_data["webhooks_data"] = []
        except discord.HTTPException as e_wh_http: log.error(f"{e('error')} Lỗi HTTP fetch webhooks: {e_wh_http.status}"); scan_errors.append(f"Lỗi HTTP lấy webhooks ({e_wh_http.status})."); scan_data["webhooks_data"] = []
        except Exception as e_wh: log.error(f"{e('error')} Lỗi fetch webhooks: {e_wh}", exc_info=True); scan_errors.append(f"Lỗi lấy webhooks: {e_wh}"); scan_data["webhooks_data"] = []
    else: log.info(f"{e('info')} Bỏ qua fetch webhooks do thiếu quyền.")

    # Fetch Integrations
    if scan_data.get("can_scan_integrations", False):
        try:
            integrations_data = await server.integrations(); scan_data["integrations_data"] = integrations_data
            log.info(f"{e('integration')} Fetched {len(integrations_data)} integrations.")
        except discord.Forbidden: log.error(f"{e('error')} Lỗi quyền fetch integrations."); scan_errors.append("Lỗi quyền lấy integrations."); scan_data["integrations_data"] = []
        except discord.HTTPException as e_int_http: log.error(f"{e('error')} Lỗi HTTP fetch integrations: {e_int_http.status}"); scan_errors.append(f"Lỗi HTTP lấy integrations ({e_int_http.status})."); scan_data["integrations_data"] = []
        except Exception as e_int: log.error(f"{e('error')} Lỗi fetch integrations: {e_int}", exc_info=True); scan_errors.append(f"Lỗi lấy integrations: {e_int}"); scan_data["integrations_data"] = []
    else: log.info(f"{e('info')} Bỏ qua fetch integrations do thiếu quyền.")


async def _fetch_top_oldest_members(scan_data: Dict[str, Any]):
    # Giữ nguyên logic hàm này, chỉ đảm bảo hằng số TOP_OLDEST_MEMBERS_LIMIT đúng
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    e = lambda name: utils.get_emoji(name, bot)
    current_members_list: List[discord.Member] = scan_data["current_members_list"]
    oldest_members_data: List[Dict[str, Any]] = []
    # Lấy hằng số từ embeds_user hoặc định nghĩa lại ở đây nếu cần
    # from reporting.embeds_user import TOP_OLDEST_MEMBERS_LIMIT # Cách 1: Import
    TOP_OLDEST_MEMBERS_LIMIT = 30 # Cách 2: Định nghĩa lại

    log.info(f"{e('calendar')} Đang xác định thành viên lâu năm nhất...")
    try:
        # Lọc user thật và có joined_at, sắp xếp
        human_members_with_join = sorted(
            [m for m in current_members_list if not m.bot and m.joined_at is not None],
            key=lambda m: m.joined_at # Sắp xếp từ cũ nhất đến mới nhất
        )
        # Lấy top N người
        for member in human_members_with_join[:TOP_OLDEST_MEMBERS_LIMIT]:
            oldest_members_data.append({
                "id": member.id,
                "mention": member.mention, # Giữ mention để tiện hiển thị
                "display_name": member.display_name,
                "joined_at": member.joined_at
            })
        scan_data["oldest_members_data"] = oldest_members_data
        log.info(f"Đã xác định top {len(oldest_members_data)} thành viên lâu năm nhất.")
    except Exception as oldest_err:
        log.error(f"Lỗi xác định thành viên lâu năm: {oldest_err}", exc_info=True); scan_errors.append(f"Lỗi lấy top thành viên lâu năm: {oldest_err}")
        scan_data["oldest_members_data"] = [] # Đảm bảo là list rỗng nếu lỗi


async def _scan_and_analyze_audit_logs(scan_data: Dict[str, Any]):
    """Quét Audit Log từ Discord, lưu DB và phân tích role grant tracking."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    e = lambda name: utils.get_emoji(name, bot)
    can_scan_audit_log = scan_data.get("can_scan_audit_log", False)
    tracked_role_ids: Set[int] = config.TRACKED_ROLE_GRANT_IDS
    # Lấy Counter đã khởi tạo đúng cách từ scan_data
    user_tracked_grants: Counter = scan_data.setdefault('tracked_role_grant_counts', Counter())
    user_thread_creation_counts = scan_data.setdefault('user_thread_creation_counts', Counter())

    if not can_scan_audit_log:
        log.info("Bỏ qua quét Audit Log do thiếu quyền.")
        return

    audit_scan_start_time = discord.utils.utcnow()
    log.info(f"[bold]{e('shield')} Bắt đầu quét Audit Log từ Discord API...[/bold]")
    audit_log_entries_added = 0
    newest_processed_id: Optional[int] = None

    try:
        last_scanned_log_id = await database.get_newest_audit_log_id_from_db(server.id)
        log.info(f"Audit log ID cuối đã quét từ DB: {last_scanned_log_id or 'Chưa có'}")

        fetch_limit = 1000
        current_after_id = last_scanned_log_id
        max_iterations = 20
        processed_in_this_scan = 0
        newest_id_in_scan = last_scanned_log_id # Khởi tạo bằng ID cũ
        log.info(f"Bắt đầu fetch audit logs sau ID: {current_after_id or 'Ban đầu'}...")

        for iteration in range(max_iterations):
            logs_in_batch: List[discord.AuditLogEntry] = []
            batch_fetch_start_time = time.monotonic() # <<< SỬ DỤNG time.monotonic()
            try:
    
                audit_iterator = server.audit_logs(
                    limit=fetch_limit,
                    after=discord.Object(id=current_after_id) if current_after_id else None,
                    oldest_first=True # Vẫn lấy từ cũ đến mới
                )
                # ------------------------------------
                logs_in_batch = [entry async for entry in audit_iterator]

                batch_fetch_duration = time.monotonic() - batch_fetch_start_time # <<< SỬ DỤNG time.monotonic()
                relevant_logs_in_batch = [
                    entry for entry in logs_in_batch
                    if entry.action in config.AUDIT_LOG_ACTIONS_TO_TRACK
                ]
                log.debug(f"  Audit log batch {iteration+1}: Fetched {len(logs_in_batch)} entries, found {len(relevant_logs_in_batch)} relevant action(s) in {batch_fetch_duration:.2f}s.")
                logs_in_batch = relevant_logs_in_batch # Gán lại list đã lọc
                # --------------------------------------------------

            except discord.Forbidden: log.error(f"  {e('error')} Lỗi quyền khi fetch audit log batch {iteration+1}. Dừng."); scan_errors.append("Lỗi quyền fetch Audit Log."); break
            except discord.HTTPException as audit_http_err: log.error(f"  {e('error')} Lỗi mạng khi fetch audit log batch {iteration+1} (HTTP {audit_http_err.status}): {audit_http_err.text}"); scan_errors.append(f"Lỗi mạng fetch Audit Log ({audit_http_err.status})."); await asyncio.sleep(5); continue

            except AttributeError as ae:
                 if "'NoneType' object has no attribute 'history'" in str(ae): # Kiểm tra lỗi cụ thể hơn nếu cần
                      log.error(f"  {e('error')} Lỗi lấy audit log iterator (có thể do lỗi kết nối/quyền?). Dừng fetch batch {iteration+1}.")
                      scan_errors.append("Lỗi lấy audit log iterator.")
                 else:
                      log.error(f"  {e('error')} Lỗi AttributeError lạ khi fetch audit log batch {iteration+1}: {ae}", exc_info=True)
                      scan_errors.append(f"Lỗi AttributeError lạ fetch Audit Log: {ae}")
                 break
            # ----------------------------------------------------------
            except Exception as audit_fetch_err: log.error(f"  {e('error')} Lỗi fetch audit log batch {iteration+1}: {audit_fetch_err}", exc_info=True); scan_errors.append(f"Lỗi fetch Audit Log: {audit_fetch_err}"); break


            if not logs_in_batch: log.info(f"  Không tìm thấy entry audit log mới phù hợp (batch {iteration+1})."); break

            db_add_count_batch = 0
            batch_newest_id_processed = None
            for entry in logs_in_batch: # Duyệt qua list đã lọc
                 try:
                     await database.add_audit_log_entry(entry)
                     db_add_count_batch += 1
                     audit_log_entries_added += 1
                     processed_in_this_scan += 1

                     if newest_id_in_scan is None or entry.id > newest_id_in_scan:
                         newest_id_in_scan = entry.id
                     # -------------------------------------------------------------
                     if batch_newest_id_processed is None or entry.id > batch_newest_id_processed:
                         batch_newest_id_processed = entry.id # Vẫn cần để check fetch đầy batch

                     if entry.action == discord.AuditLogAction.thread_create and entry.user and not entry.user.bot:
                         user_thread_creation_counts[entry.user.id] += 1

                 except Exception as db_add_err: log.error(f"  Lỗi thêm audit log entry {entry.id} vào DB: {db_add_err}"); scan_errors.append(f"Lỗi ghi Audit Log vào DB (Entry ID: {entry.id})")

            log.debug(f"  Đã thêm {db_add_count_batch}/{len(logs_in_batch)} entry vào DB. ID mới nhất xử lý batch: {batch_newest_id_processed}")


            # Kiểm tra xem batch có đầy không VÀ ID mới nhất có được xử lý không
            if len(logs_in_batch) >= fetch_limit and batch_newest_id_processed:
                current_after_id = batch_newest_id_processed # Tiếp tục fetch sau ID mới nhất của batch này
                log.info(f"  Fetch đầy batch, tiếp tục fetch sau ID: {current_after_id}...");
                await asyncio.sleep(0.5)
            else:
                # Nếu batch không đầy HOẶC không xử lý được ID mới nhất => đã hết log mới
                log.info(f"  Đã fetch hết audit log mới hoặc batch không đầy (batch {iteration+1}).")
                break
            # -----------------------------------------

            if iteration == max_iterations - 1: log.warning(f"Đạt giới hạn {max_iterations} lần fetch audit log."); scan_errors.append("Quét Audit Log dừng do giới hạn fetch."); break


        newest_processed_id = newest_id_in_scan # ID mới nhất thực sự đã thấy trong lần quét này
        # -----------------------------------------------------------
        scan_data["audit_log_entries_added"] = audit_log_entries_added
        scan_data["newest_processed_audit_log_id"] = newest_processed_id

        if newest_processed_id and newest_processed_id != last_scanned_log_id:
             log.info(f"Cập nhật ID audit log mới nhất vào DB: {newest_processed_id}")
             await database.update_newest_audit_log_id(server.id, newest_processed_id)

        scan_data["audit_log_scan_duration"] = discord.utils.utcnow() - audit_scan_start_time
        log.info(f"{e('success')} Hoàn thành quét Audit Log từ API. Thêm {audit_log_entries_added} entry vào DB trong [magenta]{utils.format_timedelta(scan_data['audit_log_scan_duration'])}[/].")

    except Exception as audit_err:
        log.error(f"{e('error')} Lỗi xử lý Audit Log: {audit_err}", exc_info=True); scan_errors.append(f"Lỗi xử lý Audit Log: {audit_err}")
        scan_data["audit_log_scan_duration"] = discord.utils.utcnow() - audit_scan_start_time


    # --- Phân tích Role Grant Tracking từ DB ---
    if not tracked_role_ids:
        log.info("Không có role grant nào được cấu hình để theo dõi, bỏ qua phân tích.")
        return

    log.info(f"{e('role')} Đang phân tích thống kê role grant đặc biệt từ Audit Log trong DB...")
    try:
        # Lấy các log member_role_update từ DB
        role_update_logs = await database.get_audit_logs_for_report(
            server.id, limit=None, # Lấy tất cả phù hợp
            action_filter=[discord.AuditLogAction.member_role_update] # Lọc action ở đây
        )
        log.info(f"Phân tích {len(role_update_logs)} member_role_update entry từ DB cho role grant tracking...")
        processed_role_logs = 0
        # user_tracked_grants đã được lấy từ scan_data ở đầu hàm

        for log_entry_dict in role_update_logs:
            target_id_str = log_entry_dict.get('target_id')
            extra_data = log_entry_dict.get('extra_data') # Đây là dict đã serialize

            # Kiểm tra dữ liệu cần thiết
            if not target_id_str or not isinstance(extra_data, dict): continue
            try: target_id = int(target_id_str)
            except (ValueError, TypeError): continue

            # Trích xuất ID role trước và sau từ extra_data đã serialize
            before_roles_data = extra_data.get('before', {}).get('roles', []) # List các dict role
            after_roles_data = extra_data.get('after', {}).get('roles', [])   # List các dict role
            # Lấy set các ID role (dạng string)
            before_role_ids_str = {str(r.get('id')) for r in before_roles_data if isinstance(r, dict) and r.get('id')}
            after_role_ids_str = {str(r.get('id')) for r in after_roles_data if isinstance(r, dict) and r.get('id')}

            # Tìm các role ID đã được thêm
            added_role_ids_str = after_role_ids_str - before_role_ids_str

            for rid_str in added_role_ids_str:
                try:
                    role_id_int = int(rid_str)
                    # Kiểm tra xem role này có nằm trong danh sách cần theo dõi không
                    if role_id_int in tracked_role_ids:
                        # Tạo key tuple và tăng counter
                        grant_key: Tuple[int, int] = (target_id, role_id_int)
                        user_tracked_grants[grant_key] += 1
                except ValueError: continue # Bỏ qua nếu ID role không phải số

            processed_role_logs += 1
        log.info(f"Đã phân tích {processed_role_logs} role update log entry cho role grant tracking. Tìm thấy {len(user_tracked_grants)} grant records.")
        # scan_data['tracked_role_grant_counts'] đã được cập nhật trực tiếp

    except Exception as role_stat_err:
        log.error(f"{e('error')} Lỗi phân tích thống kê role grant đặc biệt: {role_stat_err}", exc_info=True)
        scan_errors.append(f"Lỗi phân tích thống kê role grant: {role_stat_err}")

    await asyncio.sleep(0.1) # Delay nhỏ sau khi xử lý xong
# --- END OF FILE cogs/deep_scan_helpers/data_processing.py ---