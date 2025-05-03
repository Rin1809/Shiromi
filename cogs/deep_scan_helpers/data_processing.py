# --- START OF FILE cogs/deep_scan_helpers/data_processing.py ---
import discord
from discord.ext import commands
import logging
import asyncio
import time
import datetime
from typing import Dict, Any, List, Union, Optional, Counter
from collections import Counter, defaultdict

import config
import utils
import database
from reporting import embeds_user # Cần hằng số

log = logging.getLogger(__name__)

async def process_additional_data(scan_data: Dict[str, Any]):
    """
    Fetch và xử lý dữ liệu phụ trợ sau khi quét xong kênh/luồng.
    Bao gồm: boosters, voice static info, invites, webhooks, integrations,
    oldest members, permission analysis, audit log scan & analysis.
    Cập nhật scan_data với dữ liệu đã xử lý.
    """
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    scan_errors: List[str] = scan_data["scan_errors"]
    current_members_list: List[discord.Member] = scan_data["current_members_list"]

    log.info(f"\n--- [bold green]{e('stats')} Xử lý Dữ liệu & Tạo Báo cáo cho {server.name}[/bold green] ---")
    # Log tóm tắt quét
    _log_scan_summary(scan_data)

    log.info(f"{e('loading')} Đang fetch dữ liệu phụ trợ...")

    # --- Lấy Boosters ---
    try:
        scan_data["boosters"] = [m for m in current_members_list if m.premium_since is not None]
        log.info(f"{e('boost')} Boosters: {len(scan_data['boosters'])}")
    except Exception as booster_err:
        log.error(f"Lỗi khi lấy danh sách boosters: {booster_err}")
        scan_errors.append(f"Lỗi lấy boosters: {booster_err}")

    # --- Lấy thông tin tĩnh kênh Voice/Stage ---
    await _fetch_static_voice_stage_info(scan_data)

    # --- Fetch Invites, Webhooks, Integrations (nếu có quyền) ---
    await _fetch_invites_webhooks_integrations(scan_data)

    # --- Lấy Top Oldest Members ---
    await _fetch_top_oldest_members(scan_data)

    # --- Phân tích Quyền Nâng cao ---
    await _analyze_advanced_permissions(scan_data)

    # --- Quét và Phân tích Audit Log ---
    await _scan_and_analyze_audit_logs(scan_data)

    log.info("Hoàn thành fetch và xử lý dữ liệu phụ trợ.")


def _log_scan_summary(scan_data: Dict[str, Any]):
    """Ghi log tóm tắt các thông số quét."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    overall_duration: datetime.timedelta = scan_data["overall_duration"]
    channel_details: List[Dict[str, Any]] = scan_data["channel_details"]

    # Tính toán lại số kênh đã xử lý/bỏ qua từ channel_details
    final_processed_channels = sum(1 for d in channel_details if d.get("processed"))
    final_skipped_channels = len(channel_details) - final_processed_channels

    log.info(f"{e('clock')} Tổng TG quét: [bold magenta]{utils.format_timedelta(overall_duration, high_precision=True)}[/bold magenta]")
    log.info(f"{e('text_channel')}/{e('voice_channel')} Kênh Text/Voice: {final_processed_channels} xử lý, {final_skipped_channels} bỏ qua/lỗi")
    log.info(f"{e('thread')} Luồng: {scan_data['processed_threads_count']} xử lý, {scan_data['skipped_threads_count']} bỏ qua/lỗi")
    log.info(f"{e('stats')} Tổng tin nhắn: {scan_data['overall_total_message_count']:,}")
    if scan_data.get("can_scan_reactions"):
        log.info(f"{e('reaction')} Tổng biểu cảm: {scan_data['overall_total_reaction_count']:,}")
    log.info(f"{e('members')} Users có hoạt động: {len(scan_data['user_activity']):,}")
    # Thêm các log tóm tắt khác nếu cần


async def _fetch_static_voice_stage_info(scan_data: Dict[str, Any]):
    """Lấy thông tin cấu hình tĩnh của kênh Voice và Stage."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    voice_channel_static_data: List[Dict[str, Any]] = []
    skipped_voice_info_count = 0

    # Lấy cả voice và stage channels từ cache
    static_voice_stage_channels = server.voice_channels + server.stage_channels

    for vc in static_voice_stage_channels:
        try:
            # Chỉ lấy thông tin nếu bot có quyền xem kênh
            if vc.permissions_for(server.me).view_channel:
                voice_channel_static_data.append({
                    "channel_obj": vc, # Giữ object để dùng sau nếu cần
                    "name": vc.name,
                    "id": vc.id,
                    "type": str(vc.type),
                    "category": vc.category.name if vc.category else "N/A",
                    "category_id": vc.category.id if vc.category else None,
                    "user_limit": vc.user_limit or "Không giới hạn",
                    "bitrate": f"{vc.bitrate // 1000} kbps", # Chuyển sang kbps
                    "created_at": vc.created_at
                })
            else:
                skipped_voice_info_count += 1
                scan_errors.append(f"Info Kênh #{vc.name}: Bỏ qua (Thiếu View).")
                log.warning(f"Bỏ qua info kênh #{vc.name}: Thiếu View.")
        except Exception as vc_info_err:
            skipped_voice_info_count += 1
            error_msg = f"Lỗi lấy info kênh #{vc.name}: {vc_info_err}"
            log.error(f"{utils.get_emoji('error', bot)} {error_msg}", exc_info=True)
            scan_errors.append(error_msg)

    scan_data["voice_channel_static_data"] = voice_channel_static_data
    log.info(f"{utils.get_emoji('voice_channel', bot)} Static Voice/Stage Info: {len(voice_channel_static_data)} ({skipped_voice_info_count} bỏ qua).")


async def _fetch_invites_webhooks_integrations(scan_data: Dict[str, Any]):
    """Fetch invites, webhooks, integrations nếu bot có quyền."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    e = lambda name: utils.get_emoji(name, bot)

    # Fetch Invites
    if scan_data.get("can_scan_invites", False):
        try:
            invites_data = await server.invites()
            scan_data["invites_data"] = invites_data
            # Tính toán invite usage counts từ dữ liệu fetch được
            scan_data["invite_usage_counts"] = Counter({
                inv.inviter.id: inv.uses for inv in invites_data
                if inv.inviter and inv.uses is not None
            })
            log.info(f"{e('invite')} Fetched {len(invites_data)} invites.")
        except Exception as e_inv:
            log.error(f"{e('error')} Lỗi fetch invites: {e_inv}", exc_info=True)
            scan_errors.append(f"Lỗi lấy invites: {e_inv}")
            scan_data["invites_data"] = []
            scan_data["invite_usage_counts"] = Counter()

    # Fetch Webhooks
    if scan_data.get("can_scan_webhooks", False):
        try:
            webhooks_data = await server.webhooks()
            scan_data["webhooks_data"] = webhooks_data
            log.info(f"{e('webhook')} Fetched {len(webhooks_data)} webhooks.")
        except Exception as e_wh:
            log.error(f"{e('error')} Lỗi fetch webhooks: {e_wh}", exc_info=True)
            scan_errors.append(f"Lỗi lấy webhooks: {e_wh}")
            scan_data["webhooks_data"] = []

    # Fetch Integrations
    if scan_data.get("can_scan_integrations", False):
        try:
            integrations_data = await server.integrations()
            scan_data["integrations_data"] = integrations_data
            log.info(f"{e('integration')} Fetched {len(integrations_data)} integrations.")
        except Exception as e_int:
            log.error(f"{e('error')} Lỗi fetch integrations: {e_int}", exc_info=True)
            scan_errors.append(f"Lỗi lấy integrations: {e_int}")
            scan_data["integrations_data"] = []


async def _fetch_top_oldest_members(scan_data: Dict[str, Any]):
    """Xác định các thành viên tham gia server lâu nhất."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    e = lambda name: utils.get_emoji(name, bot)
    current_members_list: List[discord.Member] = scan_data["current_members_list"]
    oldest_members_data: List[Dict[str, Any]] = []

    log.info(f"{e('calendar')} Đang xác định thành viên lâu năm nhất...")
    try:
        # Lọc member người dùng và có thông tin joined_at, sắp xếp theo joined_at tăng dần
        human_members_with_join = sorted(
            [m for m in current_members_list if not m.bot and m.joined_at is not None],
            key=lambda m: m.joined_at
        )
        # Lấy top N theo hằng số từ embeds_user
        for member in human_members_with_join[:embeds_user.TOP_OLDEST_MEMBERS_LIMIT]:
            oldest_members_data.append({
                "id": member.id,
                "mention": member.mention,
                "display_name": member.display_name,
                "joined_at": member.joined_at
            })
        scan_data["oldest_members_data"] = oldest_members_data
        log.info(f"Đã xác định top {len(oldest_members_data)} thành viên lâu năm nhất.")
    except Exception as oldest_err:
        log.error(f"Lỗi xác định thành viên lâu năm: {oldest_err}", exc_info=True)
        scan_errors.append(f"Lỗi lấy top thành viên lâu năm: {oldest_err}")
        scan_data["oldest_members_data"] = []


async def _analyze_advanced_permissions(scan_data: Dict[str, Any]):
    """Phân tích các quyền nâng cao và tiềm ẩn rủi ro."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    e = lambda name: utils.get_emoji(name, bot)
    all_roles_list: List[discord.Role] = scan_data["all_roles_list"]
    permission_audit_results: DefaultDict[str, list] = scan_data["permission_audit_results"]

    log.info(f"[bold]{e('shield')} Bắt đầu phân tích quyền nâng cao...[/bold]")
    perm_audit_start_time = time.monotonic()
    try:
        # 1. Roles có quyền Administrator
        admin_roles = []
        for role in all_roles_list:
            if role.permissions.administrator:
                admin_roles.append({
                    "id": str(role.id),
                    "name": role.name,
                    "position": role.position,
                    "member_count": len(role.members) # Lấy số member hiện tại có role này
                })
        permission_audit_results["roles_with_admin"] = admin_roles
        admin_role_ids = {r['id'] for r in admin_roles} # Set ID các role admin

        # 2. Kiểm tra quyền @everyone trong các kênh
        everyone_role = server.default_role # Role @everyone
        # Các quyền @everyone được coi là rủi ro nếu bật trong kênh
        risky_everyone_perms = {
            'send_messages', 'manage_messages', 'manage_channels', 'manage_roles',
            'manage_webhooks', 'mention_everyone', 'administrator', 'kick_members',
            'ban_members', 'use_external_emojis', 'attach_files', 'embed_links',
            'create_public_threads', 'create_private_threads', 'send_messages_in_threads'
        }
        risky_overwrites_found = []
        # Chỉ kiểm tra các kênh bot có thể xem (để lấy overwrites)
        channels_to_check_perms = [c for c in server.channels if c.permissions_for(server.me).view_channel]
        for channel in channels_to_check_perms:
             # Chỉ kênh có overwrites mới cần kiểm tra
             if hasattr(channel, 'overwrites_for'):
                 overwrites = channel.overwrites_for(everyone_role)
                 # Kiểm tra các quyền rủi ro được bật (explicitly True)
                 found_risky_in_channel = {
                     perm_name: True for perm_name in risky_everyone_perms
                     if getattr(overwrites, perm_name, None) is True # Phải là True, không phải None
                 }
                 if found_risky_in_channel:
                     risky_overwrites_found.append({
                         "channel_id": str(channel.id),
                         "channel_name": channel.name,
                         "channel_type_emoji": utils.get_channel_type_emoji(channel, bot),
                         "permissions": found_risky_in_channel
                     })
        permission_audit_results["risky_everyone_overwrites"] = risky_overwrites_found

        # 3. Kiểm tra các role khác có quyền nguy hiểm (không phải admin/bot)
        # Các quyền nguy hiểm chung cần kiểm tra
        risky_general_perms = {
            'manage_guild', 'manage_roles', 'manage_channels', 'manage_webhooks',
            'kick_members', 'ban_members', 'mention_everyone', 'moderate_members',
            'view_audit_log', 'manage_emojis_and_stickers', 'manage_events'
        }
        other_risky_roles = []
        for role in all_roles_list:
            # Bỏ qua nếu là role admin, role của bot, role tích hợp, hoặc role booster
            if str(role.id) in admin_role_ids or \
               role.is_bot_managed() or \
               role.is_integration() or \
               role.is_premium_subscriber():
                continue

            # Tìm các quyền nguy hiểm role này có
            found_risky_general = {
                perm_name: True for perm_name in risky_general_perms
                if getattr(role.permissions, perm_name, False) is True
            }
            if found_risky_general:
                other_risky_roles.append({
                    "role_id": str(role.id),
                    "role_name": role.name,
                    "position": role.position,
                    "member_count": len(role.members),
                    "permissions": found_risky_general
                })
        permission_audit_results["other_risky_role_perms"] = other_risky_roles

        perm_audit_duration = time.monotonic() - perm_audit_start_time
        log.info(f"{e('success')} Hoàn thành phân tích quyền trong [magenta]{perm_audit_duration:.2f}[/] giây.")
    except Exception as perm_err:
        log.error(f"{e('error')} Lỗi phân tích quyền: {perm_err}", exc_info=True)
        scan_errors.append(f"Lỗi phân tích quyền: {perm_err}")


async def _scan_and_analyze_audit_logs(scan_data: Dict[str, Any]):
    """Quét Audit Log từ Discord và phân tích thống kê role."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    e = lambda name: utils.get_emoji(name, bot)
    can_scan_audit_log = scan_data.get("can_scan_audit_log", False)

    if not can_scan_audit_log:
        log.info("Bỏ qua quét Audit Log do thiếu quyền.")
        return

    # --- Quét Audit Log mới từ Discord và lưu vào DB ---
    audit_scan_start_time = discord.utils.utcnow()
    log.info(f"[bold]{e('shield')} Bắt đầu quét Audit Log ({len(config.AUDIT_LOG_ACTIONS_TO_TRACK)} action(s))...[/bold]")
    audit_log_entries_added = 0
    newest_processed_id: Optional[int] = None

    try:
        # Lấy ID log cuối cùng đã quét từ DB
        last_scanned_log_id = await database.get_newest_audit_log_id_from_db(server.id)
        log.info(f"Audit log ID cuối đã quét từ DB: {last_scanned_log_id or 'Chưa có'}")

        fetch_limit = 1000 # Giới hạn mỗi lần fetch
        current_after_id = last_scanned_log_id # Bắt đầu fetch sau ID này
        max_iterations = 20 # Giới hạn số lần fetch để tránh vòng lặp vô hạn
        processed_in_this_scan = 0
        newest_id_in_scan = last_scanned_log_id # ID mới nhất tìm thấy trong lần quét này

        log.info(f"Bắt đầu fetch audit logs sau ID: {current_after_id or 'Ban đầu'}...")

        for iteration in range(max_iterations):
            logs_in_batch: List[discord.AuditLogEntry] = []
            batch_fetch_start_time = time.monotonic()
            try:
                # Fetch log mới nhất trước (oldest_first=False) sẽ hiệu quả hơn
                # Tuy nhiên, để đảm bảo không bỏ sót, fetch từ cũ đến mới (oldest_first=True) sau một ID cụ thể
                audit_iterator = server.audit_logs(
                    limit=fetch_limit,
                    after=discord.Object(id=current_after_id) if current_after_id else None,
                    oldest_first=True # Lấy từ cũ đến mới sau ID đã biết
                )

                async for entry in audit_iterator:
                    # Chỉ xử lý các action được cấu hình
                    if entry.action in config.AUDIT_LOG_ACTIONS_TO_TRACK:
                        logs_in_batch.append(entry)
                        # Đếm số thread tạo bởi user (nếu theo dõi action này)
                        if entry.action == discord.AuditLogAction.thread_create and entry.user and not entry.user.bot:
                            scan_data["user_thread_creation_counts"][entry.user.id] += 1
                        # Cập nhật ID mới nhất đã thấy
                        if newest_id_in_scan is None or entry.id > newest_id_in_scan:
                            newest_id_in_scan = entry.id

                batch_fetch_duration = time.monotonic() - batch_fetch_start_time
                log.debug(f"  Audit log batch {iteration+1}: Fetched entries, found {len(logs_in_batch)} relevant action(s) in {batch_fetch_duration:.2f}s.")

            except discord.Forbidden:
                 log.error(f"  {e('error')} Lỗi quyền khi fetch audit log batch {iteration+1}. Dừng quét audit log."); scan_errors.append("Lỗi quyền fetch Audit Log."); break
            except discord.HTTPException as audit_http_err:
                 log.error(f"  {e('error')} Lỗi mạng khi fetch audit log batch {iteration+1} (HTTP {audit_http_err.status}): {audit_http_err.text}"); scan_errors.append(f"Lỗi mạng fetch Audit Log ({audit_http_err.status})."); await asyncio.sleep(5); continue # Thử lại batch sau khi chờ
            except Exception as audit_fetch_err:
                log.error(f"  {e('error')} Lỗi fetch audit log batch {iteration+1}: {audit_fetch_err}", exc_info=True)
                scan_errors.append(f"Lỗi fetch Audit Log: {audit_fetch_err}")
                break # Dừng nếu có lỗi lạ

            if not logs_in_batch:
                log.info(f"  Không tìm thấy entry audit log mới phù hợp (batch {iteration+1}).")
                break # Không còn log mới

            # Thêm các entry đã fetch vào DB
            db_add_count_batch = 0
            batch_newest_id_processed = None
            for entry in logs_in_batch:
                 try:
                     await database.add_audit_log_entry(entry)
                     db_add_count_batch += 1
                     audit_log_entries_added += 1
                     processed_in_this_scan += 1
                     # Lưu ID mới nhất đã xử lý thành công
                     if newest_processed_id is None or entry.id > newest_processed_id:
                          newest_processed_id = entry.id
                     if batch_newest_id_processed is None or entry.id > batch_newest_id_processed:
                          batch_newest_id_processed = entry.id
                 except Exception as db_add_err:
                     log.error(f"  Lỗi thêm audit log entry {entry.id} vào DB: {db_add_err}")
                     scan_errors.append(f"Lỗi ghi Audit Log vào DB (Entry ID: {entry.id})")

            log.debug(f"  Đã thêm {db_add_count_batch}/{len(logs_in_batch)} entry vào DB. ID mới nhất xử lý batch: {batch_newest_id_processed}")

            # Nếu fetch đủ limit và có ID mới nhất trong batch, tiếp tục fetch sau ID đó
            # Điều này đảm bảo lấy hết log nếu có nhiều hơn `fetch_limit` entry mới
            if len(logs_in_batch) >= fetch_limit and batch_newest_id_processed:
                 current_after_id = batch_newest_id_processed
                 log.info(f"  Fetch đầy batch, tiếp tục fetch sau ID: {current_after_id}...")
                 await asyncio.sleep(0.5) # Nghỉ nhẹ giữa các lần fetch
            else:
                 log.info(f"  Đã fetch hết audit log mới (batch {iteration+1}).")
                 break # Đã fetch hết

            if iteration == max_iterations - 1:
                log.warning(f"Đạt giới hạn {max_iterations} lần fetch audit log.")
                scan_errors.append("Quét Audit Log dừng do giới hạn fetch.")
                break

        # Cập nhật ID mới nhất đã xử lý thành công vào DB
        scan_data["audit_log_entries_added"] = audit_log_entries_added
        scan_data["newest_processed_audit_log_id"] = newest_processed_id
        if newest_processed_id and newest_processed_id != last_scanned_log_id:
             log.info(f"Cập nhật ID audit log mới nhất vào DB: {newest_processed_id}")
             await database.update_newest_audit_log_id(server.id, newest_processed_id)

        scan_data["audit_log_scan_duration"] = discord.utils.utcnow() - audit_scan_start_time
        log.info(f"{e('success')} Hoàn thành quét Audit Log. Thêm {audit_log_entries_added} entry vào DB trong [magenta]{utils.format_timedelta(scan_data['audit_log_scan_duration'])}[/].")

    except Exception as audit_err:
        log.error(f"{e('error')} Lỗi xử lý Audit Log: {audit_err}", exc_info=True)
        scan_errors.append(f"Lỗi xử lý Audit Log: {audit_err}")
        scan_data["audit_log_scan_duration"] = discord.utils.utcnow() - audit_scan_start_time # Vẫn ghi lại thời gian quét

    # --- Phân tích Thống kê Role từ Audit Log trong DB ---
    log.info(f"{e('role')} Đang phân tích thống kê role từ Audit Log (nếu có)...")
    try:
        # Lấy các log member_role_update từ DB
        role_update_logs = await database.get_audit_logs_for_report(
            server.id,
            limit=None, # Lấy tất cả để thống kê đầy đủ
            action_filter=[discord.AuditLogAction.member_role_update]
        )
        log.info(f"Phân tích {len(role_update_logs)} member_role_update entry từ DB...")

        processed_role_logs = 0
        # Lấy cấu trúc dữ liệu từ scan_data
        role_change_stats: DefaultDict[str, Dict[str, Counter]] = scan_data["role_change_stats"]
        user_role_changes: DefaultDict[int, Dict[str, Dict[str, int]]] = scan_data["user_role_changes"]

        for log_entry_dict in role_update_logs:
            mod_id_str = log_entry_dict.get('user_id')
            target_id_str = log_entry_dict.get('target_id')
            extra_data = log_entry_dict.get('extra_data') # Đây là dict đã deserialize từ JSONB

            # Kiểm tra dữ liệu cần thiết
            if not mod_id_str or not target_id_str or not isinstance(extra_data, dict):
                continue
            try:
                mod_id = int(mod_id_str)
                target_id = int(target_id_str)
            except (ValueError, TypeError):
                log.debug(f"Bỏ qua role update log do ID không hợp lệ: mod='{mod_id_str}', target='{target_id_str}'")
                continue

            # Lấy danh sách ID role thêm/bớt từ extra_data (đã được serialize trước đó)
            # Định dạng mong đợi: extra_data = {'before': {'$remove': [{'id': '...', 'name': '...'}, ...]}, 'after': {'$add': [...]}}
            # Lưu ý: Tên key '$remove' và '$add' có thể khác tùy thuộc vào cách serialize ban đầu.
            # Cần điều chỉnh key dựa trên cấu trúc trong database.py (_serialize_changes)
            # Giả sử key là 'roles' chứa list role objects {'id': '...', 'name': '...'}
            before_roles_list = extra_data.get('before', {}).get('roles', [])
            after_roles_list = extra_data.get('after', {}).get('roles', [])

            # Xác định roles thêm/bớt dựa trên sự khác biệt ID
            before_role_ids = {str(r.get('id')) for r in before_roles_list if isinstance(r, dict) and r.get('id')}
            after_role_ids = {str(r.get('id')) for r in after_roles_list if isinstance(r, dict) and r.get('id')}

            added_role_ids = after_role_ids - before_role_ids
            removed_role_ids = before_role_ids - after_role_ids

            # Cập nhật thống kê
            for rid in added_role_ids:
                role_change_stats[rid]["added"][mod_id] += 1
                user_role_changes[target_id][rid]["added"] += 1
            for rid in removed_role_ids:
                role_change_stats[rid]["removed"][mod_id] += 1
                user_role_changes[target_id][rid]["removed"] += 1

            processed_role_logs += 1

        log.info(f"Đã phân tích {processed_role_logs} role update log entry.")
        # Không cần gán lại role_change_stats và user_role_changes vào scan_data vì chúng là defaultdict được thay đổi trực tiếp

    except Exception as role_stat_err:
        log.error(f"{e('error')} Lỗi phân tích thống kê role từ DB: {role_stat_err}", exc_info=True)
        scan_errors.append(f"Lỗi phân tích thống kê role: {role_stat_err}")

    await asyncio.sleep(0.2) # Nghỉ nhẹ trước khi chuyển sang giai đoạn báo cáo

# --- END OF FILE cogs/deep_scan_helpers/data_processing.py ---