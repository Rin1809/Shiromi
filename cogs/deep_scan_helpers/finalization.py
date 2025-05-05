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
    """Gửi tin nhắn trung gian sau khi quét log và dọn dẹp."""
    ctx: commands.Context = scan_data["ctx"]
    bot: commands.Bot = scan_data["bot"]
    server: discord.Guild = scan_data["server"]
    e = lambda name: utils.get_emoji(name, bot)
    scan_errors: List[str] = scan_data["scan_errors"] # Giữ lại để log cuối

    log.info(f"{e('loading')} Đang hoàn tất quét log và chuẩn bị gửi báo cáo...")

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

    # --- Gửi tin nhắn thông báo hoàn tất quét log vào kênh GỐC (A) ---
    report_channel_mention = "kênh được chỉ định" # Mặc định
    report_channel_obj: Optional[discord.TextChannel] = None
    if config.REPORT_CHANNEL_ID:
        ch = server.get_channel(config.REPORT_CHANNEL_ID)
        if isinstance(ch, discord.TextChannel):
            report_channel_obj = ch
            report_channel_mention = ch.mention # Dùng mention để tạo link
        elif ch:
             report_channel_mention = f"ID {config.REPORT_CHANNEL_ID} (không phải kênh text)"
        else:
             report_channel_mention = f"ID {config.REPORT_CHANNEL_ID} (không tìm thấy)"
    elif isinstance(ctx.channel, discord.TextChannel):
        # Nếu không có kênh báo cáo riêng, link đến kênh gốc
        report_channel_obj = ctx.channel
        report_channel_mention = ctx.channel.mention


    intermediate_message_lines = [
        f"# Đây là {config.BOT_NAME} <:a_eneuroAYAYA:1367434562245890048> !! \n\n",
        "## ℹ️ Đã thu thập xong log !! \n\n",
        "## 📄 Đã viết xong báo cáo !! \n\n ",
        f"Ghé qua {report_channel_mention} để xem báo cáo về hoạt động của server trong 1 năm qua nhe !"
    ]
    intermediate_message = "\n".join(intermediate_message_lines)
    intermediate_sticker = await utils.fetch_sticker_object(config.INTERMEDIATE_STICKER_ID, bot, server)
    kwargs_intermediate: Dict[str, Any] = {
        "content": intermediate_message,
        # Vẫn để none() để tránh lỗi TypeError, chấp nhận kênh không phải link xanh
        "allowed_mentions": discord.AllowedMentions.none()
    }
    if intermediate_sticker: kwargs_intermediate["stickers"] = [intermediate_sticker]
    try:
        await ctx.send(**kwargs_intermediate)
        log.info(f"Đã gửi tin nhắn hoàn tất quét log vào kênh gốc #{ctx.channel.name}.")
    except discord.HTTPException as send_err:
        log.error(f"Lỗi gửi tin nhắn trung gian vào kênh gốc #{ctx.channel.name}: {send_err.status} {send_err.text}")
    except Exception as send_err:
        log.error(f"Lỗi không xác định gửi tin nhắn trung gian vào kênh gốc: {send_err}")
    # ------------------------------------------------------------

    # --- Log kết thúc quét (chỉ log, không gửi tin nhắn kết quả lệnh ở đây) ---
    end_time_cmd = time.monotonic()
    start_time_cmd: float = scan_data.get("start_time_cmd", end_time_cmd) # Lấy start_time
    total_cmd_duration_secs = end_time_cmd - start_time_cmd
    scan_data["overall_duration"] = datetime.timedelta(seconds=total_cmd_duration_secs) # Cập nhật lại thời gian cuối

    log.info(f"\n--- [bold green]{e('success')} Hoàn tất quét log cho {scan_data['server'].name} ({scan_data['server'].id})[/bold green] ---")
    log.info(f"{e('clock')} Thời gian quét log: [bold magenta]{utils.format_timedelta(scan_data['overall_duration'], high_precision=True)}[/]")
    if scan_errors:
        log.warning(f"{e('warning')} Quét log hoàn thành với [yellow]{len(scan_errors)}[/] lỗi/cảnh báo.")
    else:
        log.info(f"{e('success')} Quét log hoàn thành không có lỗi/cảnh báo đáng kể.")

# --- END OF FILE cogs/deep_scan_helpers/finalization.py ---