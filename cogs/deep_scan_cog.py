# --- START OF FILE cogs/deep_scan_cog.py ---
import discord
from discord.ext import commands
import time
import datetime
import logging
import asyncio
from typing import List, Dict, Any, Optional, Union, Counter as TypingCounter, DefaultDict as TypingDefaultDict
from collections import Counter, defaultdict # Đảm bảo có defaultdict

# Import các module cần thiết
import config
import utils
import database
import discord_logging

# Import các hàm helper từ thư mục con
from .deep_scan_helpers import (
    initialize_scan, scan_all_channels_and_threads, process_additional_data,
    generate_and_send_reports, generate_export_files, finalize_scan,
    send_personalized_dm_reports
)

log = logging.getLogger(__name__)

class ServerDeepScan(commands.Cog):
    """Cog chứa lệnh quét sâu server."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # --- HÀM LOGIC CỐT LÕI (Internal Helper) ---
    async def _perform_deep_scan(
        self,
        ctx: commands.Context,
        export_csv: bool,
        export_json: bool,
        admin_dm_test: bool, # Tham số này sẽ được truyền từ lệnh gọi
        keywords: Optional[str]
    ):
        """Hàm thực hiện logic quét sâu chính."""
        start_time_cmd = time.monotonic()
        overall_start_time = discord.utils.utcnow()
        e = lambda name: utils.get_emoji(name, self.bot) # <<< Lambda e giữ nguyên

        # --- Khởi tạo scan_data (Đảm bảo ĐỦ KEY) ---
        scan_data: Dict[str, Any] = {
            "server": ctx.guild, "bot": self.bot, "ctx": ctx,
            "start_time_cmd": start_time_cmd, "overall_start_time": overall_start_time,
            "export_csv": export_csv,
            "export_json": export_json,
            "admin_dm_test": admin_dm_test,
            "keywords_str": keywords, "scan_errors": [], "log_thread": None,
            "status_message": None, "initial_status_msg": None,
            "target_keywords": [], "accessible_channels": [],
            "skipped_channels_count": 0, "channel_details": [],
            "user_activity": TypingDefaultDict(lambda: {
                'first_seen': None, 'last_seen': None, 'message_count': 0, 'is_bot': False,
                'link_count': 0, 'image_count': 0, 'other_file_count': 0,
                'emoji_count': 0, 'sticker_count': 0, 'mention_given_count': 0,
                'mention_received_count': 0, 'reply_count': 0, 'reaction_received_count': 0,
                'channels_messaged_in': set(),
                'distinct_mentions_set': set(),
                'activity_span_seconds': 0.0,
            }),
            # ----- KHỞI TẠO CÁC KEY QUAN TRỌNG -----
            "overall_total_message_count": 0, # <<< Đảm bảo khởi tạo là 0
            "overall_total_reaction_count": 0, # <<< Đảm bảo khởi tạo là 0
            "processed_channels_count": 0,
            "processed_threads_count": 0,
            "skipped_threads_count": 0,
            # ----- KẾT THÚC KHỞI TẠO -----
            # Counters
            "keyword_counts": Counter(),
            "channel_keyword_counts": TypingDefaultDict(Counter),
            "thread_keyword_counts": TypingDefaultDict(Counter),
            "user_keyword_counts": TypingDefaultDict(Counter),
            "reaction_emoji_counts": Counter(),
            "filtered_reaction_emoji_counts": Counter(),
            "sticker_usage_counts": Counter(),
            "overall_custom_sticker_counts": Counter(),
            "invite_usage_counts": Counter(),
            "user_link_counts": Counter(),
            "user_image_counts": Counter(),
            "user_other_file_counts": Counter(),
            "user_emoji_counts": Counter(),
            "user_custom_emoji_content_counts": TypingDefaultDict(Counter),
            "overall_custom_emoji_content_counts": Counter(),
            "user_sticker_counts": Counter(),
            "user_mention_given_counts": Counter(),
            "user_distinct_mention_given_counts": TypingDefaultDict(set),
            "user_mention_received_counts": Counter(),
            "user_reply_counts": Counter(),
            "user_reaction_received_counts": Counter(),
            "user_thread_creation_counts": Counter(),
            "tracked_role_grant_counts": Counter(),
            "user_distinct_channel_counts": Counter(),
            "user_channel_message_counts": TypingDefaultDict(lambda: defaultdict(int)),
            "user_most_active_channel": {},
             # Dữ liệu fetch cuối
            "current_members_list": [], "initial_member_status_counts": Counter(),
            "channel_counts": Counter(), "all_roles_list": [], "boosters": [],
            "voice_channel_static_data": [], "invites_data": [], "webhooks_data": [],
            "integrations_data": [], "oldest_members_data": [],
            "audit_log_entries_added": 0, "newest_processed_audit_log_id": None,
            # Thời gian
            "scan_end_time": None,
            "overall_duration": datetime.timedelta(0),
            "audit_log_scan_duration": datetime.timedelta(0),
            "files_to_send": [],
            "report_messages_sent": 0,
            # Thêm cache cần thiết
            "server_emojis_cache": {},
            "server_sticker_ids_cache": set(),
            # Thêm cờ quyền
            "can_scan_invites": False,
            "can_scan_webhooks": False,
            "can_scan_integrations": False,
            "can_scan_audit_log": False,
            "can_scan_reactions": False,
            "can_scan_archived_threads": False,
            # Thêm dữ liệu cần cho export (ví dụ)
            "permission_audit_results": defaultdict(list),
            "role_change_stats": Counter(),
            "user_role_changes": defaultdict(list),
             # Thêm lại các counter nếu cần
             "user_activity_message_counts": Counter(),
             "user_total_custom_emoji_content_counts": Counter(),
        }

        # --- Phần logic try...except...finally ---
        try:
            log.info(f"{e('loading')} Khởi tạo quét sâu cho server: [bold cyan]{scan_data['server'].name}[/] ({scan_data['server'].id})")
            if config.ENABLE_REACTION_SCAN:
                log.warning("[bold yellow]!!! Quét biểu cảm (Reaction Scan) đang BẬT. Quá trình quét có thể chậm hơn !!![/bold yellow]")
            if scan_data["admin_dm_test"]:
                 log.info("[bold magenta]!!! Chế độ TEST DM đang BẬT. DM sẽ chỉ gửi đến ADMIN_USER_ID !!![/bold magenta]")
            else:
                 log.info("[bold green]!!! Chế độ Gửi DM Bình Thường đang BẬT. DM sẽ gửi đến role cấu hình !!![/bold green]")

            # ---- Giai đoạn 1: Khởi tạo và Kiểm tra ----
            init_successful = await initialize_scan(scan_data)
            if not init_successful:
                 if ctx.command: ctx.command.reset_cooldown(ctx)
                 return

            # ---- Giai đoạn 2: Quét Kênh và Luồng ----
            # Hàm này sẽ cập nhật các key trong scan_data, bao gồm 'overall_total_message_count'
            await scan_all_channels_and_threads(scan_data)
            scan_data["scan_end_time"] = discord.utils.utcnow()

            # ---- Giai đoạn 3: Fetch/Xử lý Dữ liệu Phụ trợ & Phân tích ----
            # Hàm này cũng có thể truy cập 'overall_total_message_count' để log
            await process_additional_data(scan_data)

            # ---- Giai đoạn 4: Tạo Báo cáo Embeds CÔNG KHAI ----
            await generate_and_send_reports(scan_data)

            # ---- Giai đoạn 5: Tạo File Xuất ----
            if scan_data["export_csv"] or scan_data["export_json"]:
                await generate_export_files(scan_data) # Gọi hàm tạo file
            else:
                 log.info("Bỏ qua tạo file export do không có yêu cầu.")


            # ---- Giai đoạn 6: Gửi Báo cáo DM Cá nhân ----
            should_send_dm = config.DM_REPORT_RECIPIENT_ROLE_ID or scan_data["admin_dm_test"]
            if should_send_dm:
                 is_testing = scan_data["admin_dm_test"]
                 log.debug(f"[Core Logic] Preparing to send DM. is_testing_mode flag from scan_data: {is_testing}")
                 log.info(f"{e('loading')} Bắt đầu gửi báo cáo DM cá nhân...")
                 asyncio.create_task(send_personalized_dm_reports(scan_data, is_testing_mode=is_testing), name=f"DMReportSender-{ctx.guild.id}")
                 log.info("Đã tạo task gửi DM chạy nền.")
            else:
                 log.info("Bỏ qua gửi DM do chưa cấu hình role người nhận và không bật test mode.")

        except commands.BotMissingPermissions as bmp_error:
             log.error(f"Quét dừng do thiếu quyền bot: {bmp_error.missing_permissions}")
             if not scan_data.get("scan_started", False) and ctx.command: ctx.command.reset_cooldown(ctx)
        except ConnectionError as conn_err:
             log.error(f"Quét dừng do lỗi kết nối: {conn_err}")
             if not scan_data.get("scan_started", False) and ctx.command: ctx.command.reset_cooldown(ctx)
        # ----- SỬA KHỐI EXCEPT NÀY -----
        except Exception as ex: # <<< Đổi tên biến exception thành 'ex'
            # <<< Dùng lambda 'e' gốc để lấy emoji >>>
            log.critical(f"{e('error')} LỖI KHÔNG MONG MUỐN trong quá trình quét sâu:", exc_info=True)
            # <<< Dùng 'ex' để lấy thông tin lỗi >>>
            scan_data["scan_errors"].append(f"Lỗi nghiêm trọng không xác định: {type(ex).__name__} - {ex}")
            try:
                # <<< Dùng lambda 'e' gốc để lấy emoji >>>
                await ctx.send(f"{e('error')} Đã xảy ra lỗi nghiêm trọng không mong muốn trong quá trình quét. Báo cáo có thể không đầy đủ. Chi tiết đã được ghi lại.")
            except Exception: pass
        # ----- KẾT THÚC SỬA EXCEPT -----

        finally:
            # ---- Giai đoạn 7: Hoàn tất và Dọn dẹp ----
            await finalize_scan(scan_data)
            discord_logging.set_log_target_thread(None)
            log.info(f"[dim]Hoàn tất dọn dẹp sau lệnh {ctx.command.name if ctx.command else 'unknown'}.[/dim]")

    # --- LỆNH !test (GỬI DM CHO ADMIN) ---
    @commands.command(
        name='testexp',
        aliases=['sds', 'serverdeepscan'],
        help=(
             '**(ADMIN TEST)** Thực hiện quét sâu và gửi báo cáo DM **CHỈ CHO ADMIN BOT**.\n'
             'Hữu ích để kiểm tra nội dung DM mà không gửi hàng loạt.\n\n'
             '**Tham số:** (Sử dụng `tên=giá_trị`)\n'
             '`export_csv=true/false` (tùy chọn, mặc định: false): Xuất báo cáo CSV.\n'
             '`export_json=true/false` (tùy chọn, mặc định: false): Xuất báo cáo JSON.\n'
             '`keywords="text"` (tùy chọn): Danh sách từ khóa cần đếm.\n\n'
             '**Ví dụ:**\n'
             f'`{config.COMMAND_PREFIX}test` (Chỉ test DM, không export)\n'
             f'`{config.COMMAND_PREFIX}test export_csv=true` (Test DM và xuất CSV)\n'
             f'`{config.COMMAND_PREFIX}test keywords="abc"` (Test DM và tìm keyword)\n\n'
             '**Lưu ý:**\n'
             '- Lệnh này **CHỈ DÀNH CHO OWNER BOT**.\n'
             '- Yêu cầu bot có **Privileged Intents** và các quyền cần thiết.\n'
             '- Quá trình quét có thể mất **RẤT LÂU**.\n'
             '- Lệnh có cooldown dài.'
        ),
        brief='(OWNER ONLY - ADMIN TEST) Quét sâu, gửi DM test cho admin.'
    )
    @commands.is_owner()
    @commands.cooldown(1, 7200, commands.BucketType.guild) # Giữ cooldown
    @commands.guild_only()
    async def server_deep_scan_test(
        self,
        ctx: commands.Context,
        *, # Keyword-only arguments
        export_csv: bool = False,
        export_json: bool = False,
        keywords: Optional[str] = None
    ):
        """Thực hiện quét sâu và gửi DM test cho admin."""
        await self._perform_deep_scan(
            ctx=ctx,
            export_csv=export_csv,
            export_json=export_json,
            admin_dm_test=True, # <<< LUÔN LUÔN TRUE CHO LỆNH NÀY
            keywords=keywords
        )

    # --- LỆNH !shiromirun (GỬI DM CHO ROLE THƯỜNG) ---
    @commands.command(
        name='shiromirun',
        help=(
             '**(NORMAL RUN)** Thực hiện quét sâu và gửi báo cáo DM cho các thành viên có role được cấu hình.\n'
             'Đây là chế độ hoạt động bình thường.\n\n'
             '**Tham số:** (Sử dụng `tên=giá_trị`)\n'
             '`export_csv=true/false` (tùy chọn, mặc định: false): Xuất báo cáo CSV.\n'
             '`export_json=true/false` (tùy chọn, mặc định: false): Xuất báo cáo JSON.\n'
             '`keywords="text"` (tùy chọn): Danh sách từ khóa cần đếm.\n\n'
             '**Ví dụ:**\n'
             f'`{config.COMMAND_PREFIX}shiromirun` (Chạy bình thường, không export)\n'
             f'`{config.COMMAND_PREFIX}shiromirun export_csv=true export_json=true` (Chạy và xuất cả hai)\n\n'
             '**Lưu ý:**\n'
             '- Lệnh này **CHỈ DÀNH CHO OWNER BOT**.\n'
             '- Yêu cầu cấu hình `DM_REPORT_RECIPIENT_ROLE_ID` trong .env.\n'
             '- Yêu cầu bot có **Privileged Intents** và các quyền cần thiết.\n'
             '- Quá trình quét có thể mất **RẤT LÂU**.\n'
             '- Lệnh có cooldown dài.'
        ),
        brief='(OWNER ONLY - NORMAL RUN) Quét sâu, gửi DM cho role cấu hình.'
    )
    @commands.is_owner()
    @commands.cooldown(1, 7200, commands.BucketType.guild) # Giữ cooldown
    @commands.guild_only()
    async def server_deep_scan_normal(
        self,
        ctx: commands.Context,
        *, # Keyword-only arguments
        export_csv: bool = False,
        export_json: bool = False,
        keywords: Optional[str] = None
    ):
        """Thực hiện quét sâu và gửi DM bình thường."""
        await self._perform_deep_scan(
            ctx=ctx,
            export_csv=export_csv,
            export_json=export_json,
            admin_dm_test=False, # <<< LUÔN LUÔN FALSE CHO LỆNH NÀY
            keywords=keywords
        )


async def setup(bot: commands.Bot):
    """Hàm setup để thêm Cog vào bot."""
    await bot.add_cog(ServerDeepScan(bot))
    log.info("Cog ServerDeepScan đã được tải với 2 lệnh quét.")

# --- END OF FILE cogs/deep_scan_cog.py ---