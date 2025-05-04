# --- START OF FILE cogs/deep_scan_helpers/finalization.py ---
import discord
from discord.ext import commands
import logging
import time
import datetime
from typing import Dict, Any, List, Optional

import config
import utils

log = logging.getLogger(__name__)

async def finalize_scan(scan_data: Dict[str, Any]):
    """Gửi tin nhắn tổng kết cuối cùng và đóng các file (nếu có)."""
    ctx: commands.Context = scan_data["ctx"]
    bot: commands.Bot = scan_data["bot"]
    server: discord.Guild = scan_data["server"] # <<< ADDED: Lấy server object
    e = lambda name: utils.get_emoji(name, bot)
    files_to_send: List[discord.File] = scan_data["files_to_send"]
    scan_errors: List[str] = scan_data["scan_errors"]
    start_time_cmd: float = scan_data["start_time_cmd"]
    log_thread: Optional[discord.Thread] = scan_data.get("log_thread")
    report_messages_sent: int = scan_data.get("report_messages_sent", 0)

    log.info(f"{e('loading')} Đang hoàn tất lệnh quét...")

    # Xóa tin nhắn trạng thái ban đầu/cuối cùng nếu còn
    initial_status_msg = scan_data.get("initial_status_msg")
    status_message = scan_data.get("status_message")
    msg_to_delete = status_message or initial_status_msg
    if msg_to_delete:
        try:
            await msg_to_delete.delete()
        except (discord.NotFound, discord.HTTPException) as del_err:
            log.debug(f"Không thể xóa tin nhắn trạng thái ({msg_to_delete.id}): {del_err}")
        except Exception as del_e:
             log.warning(f"Lỗi lạ khi xóa tin nhắn trạng thái ({msg_to_delete.id}): {del_e}")

    # Tính tổng thời gian chạy lệnh
    end_time_cmd = time.monotonic()
    total_cmd_duration_secs = end_time_cmd - start_time_cmd
    total_cmd_duration_td = datetime.timedelta(seconds=total_cmd_duration_secs)
    scan_data["overall_duration"] = total_cmd_duration_td # Cập nhật thời gian tổng cuối cùng

    # Chuẩn bị nội dung tin nhắn cuối cùng
    final_message_lines = [
        f"{e('success')} **Đã Hoàn Thành Báo Cáo!**",
        f"{e('clock')} Tổng thời gian lệnh: **{utils.format_timedelta(total_cmd_duration_td, high_precision=True)}**",
        f"{e('stats')} Đã gửi **{report_messages_sent}** tin nhắn báo cáo."
    ]

    if log_thread:
        final_message_lines.append(f"{e('info')} Xem log chi tiết tại: {log_thread.mention}")
    else:
        final_message_lines.append(f"{e('info')} Log chi tiết chỉ có trên Console.")

    # Thêm thông tin về file xuất
    if files_to_send:
        file_tags = []
        if any(f.filename.endswith('.csv') for f in files_to_send):
            file_tags.append(f"{e('csv_file')} CSV")
        if any(f.filename.endswith('.json') for f in files_to_send):
            file_tags.append(f"{e('json_file')} JSON")
        file_tags_str = " / ".join(file_tags) or "file" # Fallback "file" nếu không xác định được
        final_message_lines.append(f"📎 Đính kèm **{len(files_to_send)}** {file_tags_str}.")
    elif scan_data["export_csv"] or scan_data["export_json"]:
        # Có yêu cầu export nhưng không có file (do lỗi hoặc kích thước)
        final_message_lines.append(f"{e('error')} Yêu cầu xuất file nhưng không thể tạo/gửi (kiểm tra log/lỗi).")

    # Thêm thông báo lỗi nếu có
    if scan_errors:
        final_message_lines.append(f"{e('warning')} Lưu ý: Có **{len(scan_errors)}** lỗi/cảnh báo (xem báo cáo lỗi hoặc log).")

    # --- Lấy Sticker cuối cùng (nếu cấu hình) ---
    final_sticker_to_send: Optional[discord.Sticker] = None
    can_use_final_sticker = False
    if config.FINAL_STICKER_ID:
        try:
            log.debug(f"Fetching final sticker ID: {config.FINAL_STICKER_ID}")
            final_sticker_to_send = await bot.fetch_sticker(config.FINAL_STICKER_ID)
            if final_sticker_to_send:
                log.debug(f"Fetched sticker: {final_sticker_to_send.name}")
                # <<< FIX: Kiểm tra xem bot có thể dùng sticker này không >>>
                if final_sticker_to_send.available:
                    # Nếu là sticker server, kiểm tra xem có thuộc server hiện tại không
                    if isinstance(final_sticker_to_send, discord.GuildSticker):
                        if final_sticker_to_send.guild_id == server.id:
                            can_use_final_sticker = True
                            log.debug("Bot có thể dùng sticker server này.")
                        else:
                            log.warning(f"Sticker {final_sticker_to_send.id} thuộc server khác ({final_sticker_to_send.guild_id}), bot không thể dùng.")
                    else: # Sticker mặc định (ít khả năng fetch được bằng ID)
                        can_use_final_sticker = True # Giả sử bot dùng được sticker mặc định nếu fetch được
                else:
                     log.warning(f"Sticker {final_sticker_to_send.id} không available.")
                # <<< END FIX >>>
            else:
                 log.warning(f"Không tìm thấy sticker ID {config.FINAL_STICKER_ID} sau khi fetch.")

        except discord.NotFound:
             log.warning(f"Không tìm thấy sticker ID {config.FINAL_STICKER_ID}.")
             scan_errors.append(f"Không tìm thấy sticker ID {config.FINAL_STICKER_ID}.")
        except discord.HTTPException as e_sticker_http:
             log.warning(f"Lỗi HTTP khi fetch sticker {config.FINAL_STICKER_ID}: {e_sticker_http.status}")
             scan_errors.append(f"Lỗi HTTP lấy sticker ID {config.FINAL_STICKER_ID}.")
        except Exception as e_sticker:
            log.warning(f"Lỗi không xác định fetch sticker {config.FINAL_STICKER_ID}: {e_sticker}", exc_info=True)
            scan_errors.append(f"Lỗi lấy sticker ID {config.FINAL_STICKER_ID}.")

    # --- Gửi tin nhắn cuối cùng và file (nếu có) ---
    final_message = "\n".join(final_message_lines)
    try:
        # Tạo dict kwargs để dễ quản lý
        kwargs_send: Dict[str, Any] = {
            "content": final_message,
            "allowed_mentions": discord.AllowedMentions.none() # Không ping ai trong tin nhắn cuối
        }
        if files_to_send:
            kwargs_send["files"] = files_to_send # List các discord.File

        # <<< FIX: Chỉ thêm sticker nếu bot có thể dùng >>>
        if can_use_final_sticker and final_sticker_to_send:
            kwargs_send["stickers"] = [final_sticker_to_send] # Phải là list
        elif final_sticker_to_send and not can_use_final_sticker:
            log.info("Không gửi sticker cuối cùng do bot không có quyền sử dụng.")
        # <<< END FIX >>>

        await ctx.send(**kwargs_send)
        log.info(f"{e('success')} Đã gửi tin nhắn báo cáo cuối cùng.")

    except discord.HTTPException as e_final:
        log.error(f"{e('error')} Lỗi gửi tin nhắn/file cuối cùng (HTTP {e_final.status}): {e_final.text}", exc_info=True)
        try:
            # Thử gửi lại chỉ nội dung text nếu gửi file/sticker lỗi
            await ctx.send(f"{final_message}\n\n{e('error')} **Lỗi:** Không thể gửi file đính kèm hoặc sticker.")
        except Exception:
            log.error("Không thể gửi lại tin nhắn cuối cùng sau lỗi HTTP.")
    except Exception as e_final_unkn:
        log.error(f"{e('error')} Lỗi không xác định gửi tin nhắn/file cuối cùng: {e_final_unkn}", exc_info=True)
        try:
            await ctx.send(f"{final_message}\n\n{e('error')} **Lỗi không xác định khi gửi báo cáo cuối cùng.**")
        except Exception:
            log.error("Không thể gửi lại tin nhắn cuối cùng sau lỗi không xác định.")
    finally:
        # --- QUAN TRỌNG: Đóng file handles ---
        if files_to_send:
            log.debug(f"Đóng {len(files_to_send)} file handles...")
            for f in files_to_send:
                try:
                    f.close()
                except Exception as close_err:
                    log.warning(f"Lỗi đóng file '{f.filename}': {close_err}")
            log.debug("Đóng file handles hoàn tất.")

    # --- Log kết thúc quét ---
    log.info(f"\n--- [bold green]{e('success')} Quét Sâu Toàn Bộ cho {scan_data['server'].name} ({scan_data['server'].id}) HOÀN TẤT[/bold green] ---")
    log.info(f"{e('clock')} Tổng thời gian thực thi lệnh: [bold magenta]{total_cmd_duration_secs:.2f}[/] giây")
    if scan_errors:
        log.warning(f"{e('warning')} Quét hoàn thành với [yellow]{len(scan_errors)}[/] lỗi/cảnh báo.")
    else:
        log.info(f"{e('success')} Quét hoàn thành không có lỗi/cảnh báo đáng kể.")

# --- END OF FILE cogs/deep_scan_helpers/finalization.py ---