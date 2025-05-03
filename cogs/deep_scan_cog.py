# --- START OF FILE cogs/deep_scan_cog.py ---
import discord
from discord.ext import commands
import time
import datetime
import logging
import asyncio
from typing import List, Dict, Any, Optional, Union, Counter, DefaultDict

# Import các module cần thiết
import config
import utils
import database
import discord_logging

# Import các module reporting (sẽ dùng trong các hàm helper)
from reporting import embeds_guild, embeds_user, embeds_items, embeds_analysis
from reporting import csv_writer, json_writer

# Import các hàm helper từ thư mục con
from .deep_scan_helpers import init_scan, scan_channels, data_processing, report_generation, export_generation, finalization

log = logging.getLogger(__name__)

class ServerDeepScan(commands.Cog):
    """Cog chứa lệnh quét sâu server."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.command(
        name='svexp',
        aliases=['sds', 'serverdeepscan'],
        help=(
            'Thực hiện quét sâu toàn bộ dữ liệu của server hiện tại.\n'
            '- Quét tin nhắn trong tất cả các kênh text/voice/thread bot có quyền đọc.\n'
            '- Thu thập thông tin chi tiết về kênh, role, user, invite, webhook, integration...\n'
            '- Phân tích hoạt động của user, tần suất sử dụng emoji/sticker/keyword.\n'
            '- Kiểm tra các quyền tiềm ẩn rủi ro.\n'
            '- Lưu trữ và phân tích một phần Audit Log (nếu có quyền).\n'
            '- Tạo báo cáo chi tiết dưới dạng Embeds.\n'
            '- **Tùy chọn:** Xuất dữ liệu thô ra file CSV và/hoặc JSON.\n\n'
            '**Tham số:**\n'
            '`export_csv` (true/false, mặc định: false): Xuất báo cáo CSV.\n'
            '`export_json` (true/false, mặc định: false): Xuất báo cáo JSON.\n'
            '`keywords` (text, tùy chọn): Danh sách các từ khóa cần đếm, cách nhau bởi dấu phẩy (vd: "keyword1, key word 2").\n\n'
            '**Ví dụ:**\n'
            f'`{config.COMMAND_PREFIX}svexp` (Chỉ tạo báo cáo Embeds)\n'
            f'`{config.COMMAND_PREFIX}svexp true` (Báo cáo Embeds + CSV)\n'
            f'`{config.COMMAND_PREFIX}svexp export_json=true` (Báo cáo Embeds + JSON)\n'
            f'`{config.COMMAND_PREFIX}svexp true true keywords="chào bạn, tạm biệt"` (Embeds + CSV + JSON + đếm keywords)\n\n'
            '**Lưu ý:**\n'
            '- Lệnh này **CHỈ DÀNH CHO OWNER BOT**.\n'
            '- Yêu cầu bot có các **Privileged Intents** (Members, Message Content).\n'
            '- Cần nhiều quyền để quét đầy đủ (đọc lịch sử, xem kênh, quản lý server...). Bot sẽ báo nếu thiếu quyền cơ bản.\n'
            '- Quá trình quét có thể mất **RẤT LÂU** tùy thuộc vào kích thước và lịch sử server.\n'
            '- Lệnh có cooldown dài (mặc định 2 giờ/server) để tránh quá tải.'
        ),
        brief='(OWNER ONLY) Quét sâu server, tạo báo cáo, tùy chọn xuất file.'
    )
    @commands.is_owner()
    @commands.cooldown(1, 7200, commands.BucketType.guild) # Cooldown 2 giờ mỗi server
    @commands.guild_only()
    async def server_deep_scan(
        self,
        ctx: commands.Context,
        export_csv: bool = False,
        export_json: bool = False,
        *, # Các tham số sau phải dùng keyword=value
        keywords: Optional[str] = None
    ):
        """Thực hiện quét sâu toàn bộ server."""
        start_time_cmd = time.monotonic()
        overall_start_time = discord.utils.utcnow()
        e = lambda name: utils.get_emoji(name, self.bot) # Emoji helper

        # ---- Khởi tạo ----
        # Các biến trạng thái và cấu trúc dữ liệu sẽ được quản lý bởi các hàm helper
        scan_data = {
            "server": ctx.guild,
            "bot": self.bot,
            "ctx": ctx,
            "start_time_cmd": start_time_cmd,
            "overall_start_time": overall_start_time,
            "export_csv": export_csv,
            "export_json": export_json,
            "keywords_str": keywords,
            "scan_errors": [],
            "log_thread": None,
            "status_message": None,
            "initial_status_msg": None,
            # Dữ liệu quét sẽ được thêm vào đây bởi các hàm helper
            "target_keywords": [],
            "accessible_channels": [],
            "skipped_channels_count": 0,
            "channel_details": [],
            "user_activity": DefaultDict(lambda: {
                'first_seen': None, 'last_seen': None, 'message_count': 0, 'is_bot': False,
                'link_count': 0, 'image_count': 0, 'emoji_count': 0, 'sticker_count': 0,
                'mention_given_count': 0, 'mention_received_count': 0, 'reply_count': 0,
                'reaction_received_count': 0
            }),
            "overall_total_message_count": 0,
            "overall_total_reaction_count": 0,
            "processed_channels_count": 0,
            "processed_threads_count": 0,
            "skipped_threads_count": 0,
            # Counters
            "keyword_counts": Counter(),
            "channel_keyword_counts": DefaultDict(Counter),
            "thread_keyword_counts": DefaultDict(Counter),
            "user_keyword_counts": DefaultDict(Counter),
            "reaction_emoji_counts": Counter(),
            "sticker_usage_counts": Counter(),
            "invite_usage_counts": Counter(),
            "user_link_counts": Counter(),
            "user_image_counts": Counter(),
            "user_emoji_counts": Counter(),
            "user_sticker_counts": Counter(),
            "user_mention_given_counts": Counter(),
            "user_mention_received_counts": Counter(),
            "user_reply_counts": Counter(),
            "user_reaction_received_counts": Counter(),
            "user_thread_creation_counts": Counter(),
            # Dữ liệu fetch cuối
            "current_members_list": [],
            "initial_member_status_counts": Counter(),
            "channel_counts": Counter(),
            "all_roles_list": [],
            "boosters": [],
            "voice_channel_static_data": [],
            "invites_data": [],
            "webhooks_data": [],
            "integrations_data": [],
            "oldest_members_data": [],
            "audit_log_entries_added": 0,
            "newest_processed_audit_log_id": None,
            "permission_audit_results": DefaultDict(list),
            "role_change_stats": DefaultDict(lambda: {"added": Counter(), "removed": Counter()}),
            "user_role_changes": DefaultDict(lambda: DefaultDict(lambda: {"added": 0, "removed": 0})),
            # Thời gian
            "overall_duration": datetime.timedelta(0),
            "audit_log_scan_duration": datetime.timedelta(0),
            "files_to_send": [], # Danh sách file Discord để gửi
        }

        try:
            log.info(f"{e('loading')} Khởi tạo quét sâu cho server: [bold cyan]{scan_data['server'].name}[/] ({scan_data['server'].id})")
            if config.ENABLE_REACTION_SCAN:
                log.warning("[bold yellow]!!! Quét biểu cảm (Reaction Scan) đang BẬT. Quá trình quét có thể chậm hơn !!![/bold yellow]")

            # ---- Giai đoạn 1: Khởi tạo và Kiểm tra ----
            init_successful = await init_scan.initialize_scan(scan_data)
            if not init_successful:
                 ctx.command.reset_cooldown(ctx)
                 # Hàm initialize_scan đã gửi tin nhắn lỗi nếu cần
                 return

            # ---- Giai đoạn 2: Quét Kênh và Luồng ----
            await scan_channels.scan_all_channels_and_threads(scan_data)

            # ---- Giai đoạn 3: Fetch/Xử lý Dữ liệu Phụ trợ & Phân tích ----
            await data_processing.process_additional_data(scan_data)

            # ---- Giai đoạn 4: Tạo Báo cáo Embeds ----
            await report_generation.generate_and_send_reports(scan_data)

            # ---- Giai đoạn 5: Tạo File Xuất (Nếu có yêu cầu) ----
            if scan_data["export_csv"] or scan_data["export_json"]:
                await export_generation.generate_export_files(scan_data)

        except commands.BotMissingPermissions as bmp_error:
             # Lỗi thiếu quyền đã được xử lý và log ở init_scan hoặc scan_channels
             # Chỉ cần đảm bảo cooldown được reset nếu lỗi xảy ra ở giai đoạn đầu
             log.error(f"Quét dừng do thiếu quyền bot: {bmp_error.missing_permissions}")
             if not scan_data.get("scan_started", False): # Nếu lỗi xảy ra trước khi quét chính thức
                  ctx.command.reset_cooldown(ctx)
        except ConnectionError as conn_err:
             # Lỗi kết nối DB đã được xử lý ở init_scan
             log.error(f"Quét dừng do lỗi kết nối: {conn_err}")
             if not scan_data.get("scan_started", False):
                  ctx.command.reset_cooldown(ctx)
        except Exception as e:
            # Bắt các lỗi không mong muốn khác
            log.critical(f"{e('error')} LỖI KHÔNG MONG MUỐN trong quá trình quét sâu:", exc_info=True)
            scan_data["scan_errors"].append(f"Lỗi nghiêm trọng không xác định: {type(e).__name__} - {e}")
            try:
                await ctx.send(f"{e('error')} Đã xảy ra lỗi nghiêm trọng không mong muốn trong quá trình quét. Báo cáo có thể không đầy đủ. Chi tiết đã được ghi lại.")
            except Exception:
                pass

        finally:
            # ---- Giai đoạn 6: Hoàn tất và Dọn dẹp ----
            await finalization.finalize_scan(scan_data)

            # Dọn dẹp logging target
            discord_logging.set_log_target_thread(None)
            log.info(f"[dim]Hoàn tất dọn dẹp sau lệnh {ctx.command.name}.[/dim]")


async def setup(bot: commands.Bot):
    """Hàm setup để thêm Cog vào bot."""
    await bot.add_cog(ServerDeepScan(bot))
    log.info("Cog ServerDeepScan đã được tải.")

# --- END OF FILE cogs/deep_scan_cog.py ---