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
    scan_errors: List[str] = scan_data["scan_errors"]
    scan_id: Optional[int] = scan_data.get("scan_id") 

    log.info(f"{e('loading')} Đang hoàn tất quét log và chuẩn bị gửi báo cáo...")

    # Xóa tin nhắn trạng thái ban đầu/cuối cùng nếu còn
    initial_status_msg = scan_data.get("initial_status_msg")
    status_message = scan_data.get("status_message")
    msg_to_delete = status_message or initial_status_msg
    if msg_to_delete:
        try: await msg_to_delete.delete()
        except (discord.NotFound, discord.HTTPException) as del_err: log.debug(f"Không thể xóa tin nhắn trạng thái ({msg_to_delete.id}): {del_err}")
        except Exception as del_e: log.warning(f"Lỗi lạ khi xóa tin nhắn trạng thái ({msg_to_delete.id}): {del_e}")

    # --- Xác định kênh báo cáo và link website ---
    report_channel_mention = "kênh được chỉ định"
    report_channel_obj: Optional[discord.TextChannel] = None
    if config.REPORT_CHANNEL_ID:
        ch = server.get_channel(config.REPORT_CHANNEL_ID)
        if isinstance(ch, discord.TextChannel): report_channel_obj = ch; report_channel_mention = ch.mention
        elif ch: report_channel_mention = f"ID {config.REPORT_CHANNEL_ID} (không phải kênh text)"
        else: report_channel_mention = f"ID {config.REPORT_CHANNEL_ID} (không tìm thấy)"
    elif isinstance(ctx.channel, discord.TextChannel): report_channel_obj = ctx.channel; report_channel_mention = ctx.channel.mention

    # Tạo link website
    website_link = "Link tra cứu không khả dụng."
    if config.WEBSITE_BASE_URL and config.WEBSITE_BASE_URL != "http://localhost:3000":
        # Ưu tiên link chỉ có guild ID để web tự lấy scan mới nhất
        website_link = f"{config.WEBSITE_BASE_URL}/scan/{server.id}"
    else:
        log.warning("WEBSITE_BASE_URL chưa được cấu hình đúng, link tra cứu sẽ không hoạt động.")

    # --- Gửi tin nhắn trung gian vào kênh GỐC (A) ---
    intermediate_message_lines = [
        f"# Là {config.BOT_NAME} đây <:a_eneuroAYAYA:1367434562245890048> !! \n",
        "## ℹ️ Đã thu thập xong log !!",
        "## 📄 Đã viết xong báo cáo !! \n\n",
        f"🔍 Tra cứu kết quả của mấy bạn khác tại: \n\n"
        f"## [Bấm vô đây: Hôm qua ᓚᘏᗢ | きのう]({website_link}) \n\n", # <<< THÊM LINK WEB
        f"👉 Ghé qua {report_channel_mention} để xem báo cáo tổng hợp của Server trong 1 năm qua trên Discord nhe!"
    ]
    intermediate_message = "\n".join(intermediate_message_lines)
    intermediate_sticker = await utils.fetch_sticker_object(config.INTERMEDIATE_STICKER_ID, bot, server)
    kwargs_intermediate: Dict[str, Any] = {
        "content": intermediate_message,
        "allowed_mentions": discord.AllowedMentions.none(),
        # Ngăn Discord tự tạo preview cho link web nếu không muốn
        "suppress_embeds": True
    }
    if intermediate_sticker: kwargs_intermediate["stickers"] = [intermediate_sticker]
    try:
        await ctx.send(**kwargs_intermediate)
        log.info(f"Đã gửi tin nhắn hoàn tất quét log vào kênh gốc #{ctx.channel.name}.")
    except discord.HTTPException as send_err: log.error(f"Lỗi gửi tin nhắn trung gian vào kênh gốc #{ctx.channel.name}: {send_err.status} {send_err.text}")
    except Exception as send_err: log.error(f"Lỗi không xác định gửi tin nhắn trung gian vào kênh gốc: {send_err}")
    # ------------------------------------------------------------

    # --- Log kết thúc quét ---
    end_time_cmd = time.monotonic()
    start_time_cmd: float = scan_data.get("start_time_cmd", end_time_cmd)
    total_cmd_duration_secs = end_time_cmd - start_time_cmd
    scan_data["overall_duration"] = datetime.timedelta(seconds=total_cmd_duration_secs)

    log.info(f"\n--- [bold green]{e('success')} Hoàn tất xử lý sau quét cho {scan_data['server'].name} (Scan ID: {scan_id})[/bold green] ---")
    log.info(f"{e('clock')} Thời gian tổng lệnh: [bold magenta]{utils.format_timedelta(scan_data['overall_duration'], high_precision=True)}[/]")
    if scan_errors: log.warning(f"{e('warning')} Quét hoàn thành với [yellow]{len(scan_errors)}[/] lỗi/cảnh báo.")
    else: log.info(f"{e('success')} Quét hoàn thành không có lỗi/cảnh báo đáng kể.")

# --- END OF FILE cogs/deep_scan_helpers/finalization.py ---