# --- START OF FILE cogs/deep_scan_helpers/export_generation.py ---
import discord
import logging
import time
from typing import Dict, Any, List, Optional

import utils
import database # Cần DB để lấy audit log cho export

# Import các module ghi file
from reporting import csv_writer, json_writer

log = logging.getLogger(__name__)

async def generate_export_files(scan_data: Dict[str, Any]):
    """Tạo các file export CSV và/hoặc JSON nếu được yêu cầu."""
    export_csv: bool = scan_data["export_csv"]
    export_json: bool = scan_data["export_json"]
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    scan_errors: List[str] = scan_data["scan_errors"]
    files_to_send: List[discord.File] = scan_data["files_to_send"]

    if not export_csv and not export_json:
        return # Không có yêu cầu export

    log.info(f"\n--- [bold blue]{e('loading')} Đang tạo file xuất ({'CSV ' if export_csv else ''}{'JSON' if export_json else ''})[/bold blue] ---")
    start_time_export = time.monotonic()

    # --- Chuẩn bị dữ liệu chung cho export ---
    # (Lấy dữ liệu từ scan_data)
    server_info = { # Tạo dict server_info tương tự như trong report_gen
        'member_count_real': len([m for m in scan_data["current_members_list"] if not m.bot]),
        'bot_count': len([m for m in scan_data["current_members_list"] if m.bot]),
        # Thêm các thông tin khác nếu cần
    }
    channel_details = scan_data["channel_details"]
    voice_channel_static_data = scan_data["voice_channel_static_data"]
    user_activity = scan_data["user_activity"]
    roles = scan_data["all_roles_list"]
    boosters = scan_data["boosters"]
    invites = scan_data["invites_data"]
    webhooks = scan_data["webhooks_data"]
    integrations = scan_data["integrations_data"]
    permission_audit = scan_data["permission_audit_results"]
    scan_timestamp = scan_data["scan_end_time"] # Lấy thời gian kết thúc quét
    # Optional data
    oldest_members_data = scan_data["oldest_members_data"]
    role_change_stats = scan_data["role_change_stats"]
    user_role_changes = scan_data["user_role_changes"]
    user_thread_creation_counts = scan_data["user_thread_creation_counts"]
    keyword_totals = scan_data["keyword_counts"]
    keyword_by_channel = scan_data["channel_keyword_counts"]
    keyword_by_thread = scan_data["thread_keyword_counts"]
    keyword_by_user = scan_data["user_keyword_counts"]
    keywords_searched = scan_data["target_keywords"]
    # Counters
    reaction_emoji_counts = scan_data["reaction_emoji_counts"]
    sticker_usage_counts = scan_data["sticker_usage_counts"]
    invite_usage_counts = scan_data["invite_usage_counts"]
    user_link_counts = scan_data["user_link_counts"]
    user_image_counts = scan_data["user_image_counts"]
    user_emoji_counts = scan_data["user_emoji_counts"]
    user_sticker_counts = scan_data["user_sticker_counts"]
    user_mention_given_counts = scan_data["user_mention_given_counts"]
    user_mention_received_counts = scan_data["user_mention_received_counts"]
    user_reply_counts = scan_data["user_reply_counts"]
    user_reaction_received_counts = scan_data["user_reaction_received_counts"]

    # --- Fetch thêm Audit Log cho export (nếu cần) ---
    audit_logs_for_export = []
    if scan_data.get("can_scan_audit_log", False):
        try:
            # Lấy nhiều hơn cho export, ví dụ 5000 entry gần nhất
            log.info("Fetching thêm audit logs từ DB cho export (limit 5000)...")
            audit_logs_for_export = await database.get_audit_logs_for_report(server.id, limit=5000)
            log.info(f"Fetched {len(audit_logs_for_export)} audit log entries cho export.")
        except Exception as ex:
            log.error(f"{e('error')} Lỗi fetch audit logs để xuất: {ex}")
            scan_errors.append(f"Lỗi lấy audit log cho export: {ex}")

    # --- Tạo báo cáo CSV ---
    if export_csv:
        log.info(f"{e('csv_file')} Đang tạo báo cáo CSV...")
        csv_creation_start = time.monotonic()
        try:
            # Gọi hàm tạo CSV chính từ module csv_writer
            await csv_writer.create_csv_report(
                server=server, bot=bot, server_info=server_info,
                channel_details=channel_details,
                voice_channel_static_data=voice_channel_static_data,
                user_activity=user_activity, roles=roles, boosters=boosters,
                invites=invites, webhooks=webhooks, integrations=integrations,
                audit_logs=audit_logs_for_export, permission_audit=permission_audit,
                scan_timestamp=scan_timestamp, files_list_ref=files_to_send, # Truyền list để thêm file
                # Optional data
                oldest_members_data=oldest_members_data,
                role_change_stats=role_change_stats, user_role_changes=user_role_changes,
                user_thread_creation_counts=user_thread_creation_counts,
                keyword_totals=keyword_totals, keyword_by_channel=keyword_by_channel,
                keyword_by_thread=keyword_by_thread, keyword_by_user=keyword_by_user,
                keywords_searched=keywords_searched,
                # Counters
                reaction_emoji_counts=reaction_emoji_counts, sticker_usage_counts=sticker_usage_counts,
                invite_usage_counts=invite_usage_counts, user_link_counts=user_link_counts,
                user_image_counts=user_image_counts, user_emoji_counts=user_emoji_counts,
                user_sticker_counts=user_sticker_counts, user_mention_given_counts=user_mention_given_counts,
                user_mention_received_counts=user_mention_received_counts, user_reply_counts=user_reply_counts,
                user_reaction_received_counts=user_reaction_received_counts
            )
            csv_creation_duration = time.monotonic() - csv_creation_start
            log.info(f"{e('success')} Hoàn thành tạo các file CSV trong {csv_creation_duration:.2f}s.")
        except Exception as ex:
            error_msg = f"Lỗi tạo file CSV: {ex}"
            log.error(f"{e('error')} {error_msg}", exc_info=True)
            scan_errors.append(error_msg)

    # --- Tạo báo cáo JSON ---
    if export_json:
        log.info(f"{e('json_file')} Đang tạo báo cáo JSON...")
        json_creation_start = time.monotonic()
        try:
            # Gọi hàm tạo JSON từ module json_writer
            json_file = await json_writer.create_json_report(
                server=server, bot=bot, server_info=server_info,
                channel_details=channel_details,
                voice_channel_static_data=voice_channel_static_data,
                user_activity=user_activity, roles=roles, boosters=boosters,
                invites=invites, webhooks=webhooks, integrations=integrations,
                audit_logs=audit_logs_for_export, permission_audit=permission_audit,
                scan_timestamp=scan_timestamp,
                # Optional data
                oldest_members_data=oldest_members_data,
                role_change_stats=role_change_stats, user_role_changes=user_role_changes,
                user_thread_creation_counts=user_thread_creation_counts,
                keyword_totals=keyword_totals, keyword_by_channel=keyword_by_channel,
                keyword_by_thread=keyword_by_thread, keyword_by_user=keyword_by_user,
                keywords_searched=keywords_searched,
                # Counters
                reaction_emoji_counts=reaction_emoji_counts, sticker_usage_counts=sticker_usage_counts,
                invite_usage_counts=invite_usage_counts, user_link_counts=user_link_counts,
                user_image_counts=user_image_counts, user_emoji_counts=user_emoji_counts,
                user_sticker_counts=user_sticker_counts, user_mention_given_counts=user_mention_given_counts,
                user_mention_received_counts=user_mention_received_counts, user_reply_counts=user_reply_counts,
                user_reaction_received_counts=user_reaction_received_counts
            )
            if json_file:
                files_to_send.append(json_file)
                json_creation_duration = time.monotonic() - json_creation_start
                log.info(f"{e('success')} Hoàn thành tạo file JSON trong {json_creation_duration:.2f}s.")
            else:
                log.error(f"{e('error')} Hàm create_json_report không trả về file.")
                scan_errors.append("Lỗi tạo JSON: Không nhận được file.")
        except Exception as ex:
            error_msg = f"Lỗi tạo file JSON: {ex}"
            log.error(f"{e('error')} {error_msg}", exc_info=True)
            scan_errors.append(error_msg)

    # --- Kiểm tra kích thước file (nếu có file để gửi) ---
    if files_to_send:
        await _check_file_sizes(scan_data)

    end_time_export = time.monotonic()
    log.info(f"Hoàn thành tạo file export trong {end_time_export - start_time_export:.2f}s.")


async def _check_file_sizes(scan_data: Dict[str, Any]):
    """Kiểm tra tổng kích thước và kích thước file đơn lẻ trước khi gửi."""
    files_to_send: List[discord.File] = scan_data["files_to_send"]
    ctx: commands.Context = scan_data["ctx"]
    e = lambda name: utils.get_emoji(name, scan_data["bot"])
    scan_errors: List[str] = scan_data["scan_errors"]

    total_size_bytes = 0
    individual_file_too_large = False
    num_files = len(files_to_send)
    # Giới hạn của Discord là 25MB, để an toàn dùng 24.5MB
    FILE_SIZE_LIMIT_MB = 24.5
    FILE_SIZE_LIMIT_BYTES = FILE_SIZE_LIMIT_MB * 1024 * 1024

    log.info("Kiểm tra kích thước file export...")
    for i, f in enumerate(files_to_send):
        try:
            # Lấy kích thước file từ BytesIO/StringIO
            f.fp.seek(0, 2) # Di chuyển đến cuối file
            size = f.fp.tell() # Lấy vị trí cuối (kích thước)
            f.reset() # Đặt lại vị trí về đầu file để Discord đọc được
            total_size_bytes += size
            size_mb = size / (1024 * 1024)
            log.debug(f"File {i+1} ('{f.filename}'): {size_mb:.2f} MB")
            if size > FILE_SIZE_LIMIT_BYTES:
                individual_file_too_large = True
                log.error(f"File '{f.filename}' ({size_mb:.2f} MB) quá lớn (Giới hạn: {FILE_SIZE_LIMIT_MB} MB).")
                scan_errors.append(f"File export '{f.filename}' quá lớn.")
        except Exception as size_err:
            log.error(f"Không thể xác định kích thước file '{f.filename}': {size_err}")
            scan_errors.append(f"Không thể kiểm tra kích thước file '{f.filename}'.")
            # Coi như file có thể quá lớn nếu không kiểm tra được
            individual_file_too_large = True

    total_size_mb = total_size_bytes / (1024 * 1024)
    log.info(f"{e('info')} Tổng kích thước file xuất ({num_files} files): {total_size_mb:.2f} MB")

    if total_size_bytes >= FILE_SIZE_LIMIT_BYTES or individual_file_too_large:
        error_msg = f"{e('error')} Tổng kích thước file xuất ({total_size_mb:.2f} MB) hoặc file đơn lẻ vượt quá giới hạn {FILE_SIZE_LIMIT_MB}MB. Không thể đính kèm."
        await ctx.send(error_msg)
        scan_errors.append(f"Xuất file thất bại: Kích thước quá lớn ({total_size_mb:.2f}MB).")
        # Đóng và xóa các file đã tạo để giải phóng bộ nhớ
        for f in files_to_send:
            try:
                f.close()
            except Exception as close_err:
                log.warning(f"Lỗi đóng file '{f.filename}': {close_err}")
        files_to_send.clear() # Xóa list để không gửi nữa


# --- END OF FILE cogs/deep_scan_helpers/export_generation.py ---