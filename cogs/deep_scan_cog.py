# --- START OF FILE cogs/deep_scan_cog.py ---
import discord
from discord.ext import commands
import time
import datetime
import logging
import asyncio
from typing import List, Dict, Any, Optional, Union, Counter as TypingCounter, DefaultDict as TypingDefaultDict, Tuple
from collections import Counter, defaultdict

# Import các module cần thiết
import config
import utils
import database
import discord_logging

# Import các helper functions từ thư mục deep_scan_helpers
from .deep_scan_helpers import (
    initialize_scan, scan_all_channels_and_threads, process_additional_data,
    generate_and_send_reports, generate_export_files, finalize_scan,
    send_personalized_dm_reports # Import hàm gửi DM
)
# Import hàm chuẩn bị ranking data từ dm_sender
from .deep_scan_helpers.dm_sender import _prepare_ranking_data

# Import các module tạo embeds (nhưng không gọi hàm _prepare_ranking_data từ đây nữa)
# from reporting import embeds_dm # Không cần import này để gọi _prepare_ranking_data

log = logging.getLogger(__name__)

# --- HÀM LƯU KẾT QUẢ VÀO DB (Giữ nguyên) ---
async def save_aggregated_results_to_db(scan_data: Dict[str, Any], ranking_data: Dict[str, Dict[int, int]]):
    """Thu thập và lưu kết quả tổng hợp của từng user vào database."""
    scan_id = scan_data.get("scan_id")
    if not scan_id:
        log.error("Thiếu scan_id trong scan_data, không thể lưu kết quả user.")
        return

    log.info(f"Bắt đầu thu thập và chuẩn bị lưu kết quả user cho scan_id: {scan_id}...")
    user_results_to_save: List[Dict[str, Any]] = []
    processed_user_ids = set() # Để tránh trùng lặp nếu user_activity có key lạ

    # Lấy cache member để lấy tên hiển thị
    member_cache: Dict[int, discord.Member] = {m.id: m for m in scan_data.get("current_members_list", [])}

    for user_id, user_act_data in scan_data.get("user_activity", {}).items():
        if not isinstance(user_id, int) or user_id in processed_user_ids:
            continue
        processed_user_ids.add(user_id)

        member = member_cache.get(user_id)
        display_name = member.display_name if member else f"User {user_id}"
        avatar_url = str(member.display_avatar.url) if member and member.display_avatar else None

        user_result = {
            "user_id": user_id,
            "display_name_at_scan": display_name[:100],
            "avatar_url_at_scan": avatar_url,
            "is_bot": user_act_data.get('is_bot', False),
            "message_count": user_act_data.get('message_count', 0),
            "link_count": user_act_data.get('link_count', 0),
            "image_count": user_act_data.get('image_count', 0),
            "other_file_count": user_act_data.get('other_file_count', 0),
            "sticker_count": user_act_data.get('sticker_count', 0),
            "mention_given_count": user_act_data.get('mention_given_count', 0),
            "mention_received_count": user_act_data.get('mention_received_count', 0),
            "reply_count": user_act_data.get('reply_count', 0),
            "reaction_received_count": user_act_data.get('reaction_received_count', 0),
            "reaction_given_count": user_act_data.get('reaction_given_count', 0),
            "distinct_channels_count": len(user_act_data.get('channels_messaged_in', set())),
            "first_seen_utc": user_act_data.get('first_seen'),
            "last_seen_utc": user_act_data.get('last_seen'),
            "activity_span_seconds": user_act_data.get('activity_span_seconds', 0),
            "ranking_data": {},
            "achievement_data": {},
        }

        # Lấy dữ liệu xếp hạng đã tính toán trước
        for rank_key, ranks in ranking_data.items():
            rank = ranks.get(user_id)
            if rank:
                user_result["ranking_data"][rank_key] = rank

        # Lấy dữ liệu thành tích (ví dụ: emoji, sticker top)
        user_custom_emoji_counts: Counter = scan_data.get("user_custom_emoji_content_counts", defaultdict(Counter)).get(user_id, Counter())
        if user_custom_emoji_counts:
            try:
                top_emoji_id, top_emoji_count = user_custom_emoji_counts.most_common(1)[0]
                user_result["achievement_data"]["top_content_emoji"] = {"id": top_emoji_id, "count": top_emoji_count}
            except IndexError: pass

        user_sticker_id_counts: Counter = scan_data.get("user_sticker_id_counts", defaultdict(Counter)).get(user_id, Counter())
        if user_sticker_id_counts:
            try:
                top_sticker_id_str, top_sticker_count = user_sticker_id_counts.most_common(1)[0]
                user_result["achievement_data"]["top_sticker"] = {"id": top_sticker_id_str, "count": top_sticker_count}
            except IndexError: pass

        user_results_to_save.append(user_result)

    if user_results_to_save:
        log.info(f"Chuẩn bị lưu {len(user_results_to_save)} kết quả user vào database...")
        try:
            await database.save_user_scan_results(scan_id, user_results_to_save)
            await database.update_scan_status(scan_id, status='running', website_ready=True) # Đánh dấu web ready sau khi lưu thành công
        except Exception as e_save:
            log.error(f"Lỗi khi thực hiện save_user_scan_results hoặc update_scan_status: {e_save}", exc_info=True)
            # Ghi nhận lỗi vào scan_data để báo cáo cuối cùng
            scan_data["scan_errors"].append(f"DB Save/Update Error: {e_save}")
            # Không cần update status lại ở đây vì nó đã lỗi
    else:
        log.info("Không có kết quả user nào để lưu vào database.")


class ServerDeepScan(commands.Cog):
    """Cog chứa lệnh quét sâu server."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_check(self, ctx: commands.Context) -> bool:
        """
        Kiểm tra chung cho tất cả các lệnh trong Cog này.
        Cho phép owner (từ ADMIN_USER_ID trong config) hoặc PROXY_BOT_ID.
        """
        if await ctx.bot.is_owner(ctx.author):
            log.debug(f"Owner {ctx.author} (ID: {ctx.author.id}) is running command '{ctx.command.name}'. Allowing.")
            return True

        if config.PROXY_BOT_ID and ctx.author.id == config.PROXY_BOT_ID:
            log.info(f"PROXY_BOT_ID ({config.PROXY_BOT_ID}) is running command '{ctx.command.name}'. Allowing.")
            return True

        log.warning(f"User {ctx.author} (ID: {ctx.author.id}) (not owner or proxy) tried to run '{ctx.command.name}'. Denying.")
        # Không gửi tin nhắn lỗi ở đây nữa, để on_command_error xử lý chung
        return False # CheckFailure sẽ được raise và on_command_error sẽ bắt

    async def _perform_deep_scan(
        self,
        ctx: commands.Context,
        export_csv: bool,
        export_json: bool,
        admin_dm_test: bool,
        keywords: Optional[str]
    ):
        start_time_cmd = time.monotonic()
        overall_start_time = discord.utils.utcnow()
        e = lambda name: utils.get_emoji(name, self.bot)

        # Khởi tạo scan_data với các giá trị mặc định
        scan_data: Dict[str, Any] = {
            "server": ctx.guild, "bot": self.bot, "ctx": ctx,
            "start_time_cmd": start_time_cmd, "overall_start_time": overall_start_time,
            "export_csv": export_csv, "export_json": export_json,
            "admin_dm_test": admin_dm_test, "keywords_str": keywords,
            "scan_errors": [], "log_thread": None, "status_message": None,
            "initial_status_msg": None, "target_keywords": [],
            "accessible_channels": [], "skipped_channels_count": 0,
            "channel_details": [],
            "user_activity": TypingDefaultDict(lambda: { # Sử dụng typing để IDE hiểu rõ hơn
                'first_seen': None, 'last_seen': None, 'message_count': 0, 'is_bot': False,
                'link_count': 0, 'image_count': 0, 'other_file_count': 0,
                'emoji_count': 0, 'sticker_count': 0, 'mention_given_count': 0,
                'mention_received_count': 0, 'reply_count': 0, 'reaction_received_count': 0,
                'reaction_given_count': 0,
                'channels_messaged_in': set(),
                'distinct_mentions_set': set(),
                'activity_span_seconds': 0.0,
            }),
            "overall_total_message_count": 0, "overall_total_reaction_count": 0,
            "overall_total_filtered_reaction_count": 0, "processed_channels_count": 0,
            "processed_threads_count": 0, "skipped_threads_count": 0,
            "keyword_counts": Counter(), "channel_keyword_counts": defaultdict(Counter),
            "thread_keyword_counts": defaultdict(Counter), "user_keyword_counts": defaultdict(Counter),
            "reaction_emoji_counts": Counter(), "filtered_reaction_emoji_counts": Counter(),
            "sticker_usage_counts": Counter(), "overall_custom_sticker_counts": Counter(),
            "invite_usage_counts": Counter(), "user_link_counts": Counter(),
            "user_image_counts": Counter(), "user_other_file_counts": Counter(),
            "user_emoji_counts": Counter(), "user_custom_emoji_content_counts": defaultdict(Counter),
            "overall_custom_emoji_content_counts": Counter(), "user_sticker_counts": Counter(),
            "user_mention_given_counts": Counter(), "user_distinct_mention_given_counts": TypingDefaultDict(set),
            "user_mention_received_counts": Counter(), "user_reply_counts": Counter(),
            "user_reaction_received_counts": Counter(), "user_reaction_given_counts": Counter(),
            "user_reaction_emoji_given_counts": defaultdict(Counter),
            "user_thread_creation_counts": Counter(),
            "tracked_role_grant_counts": Counter(),
            "user_distinct_channel_counts": Counter(),
            "user_channel_message_counts": defaultdict(lambda: defaultdict(int)),
            "user_most_active_channel": {},
            "user_activity_message_counts": Counter(),
            "user_total_custom_emoji_content_counts": Counter(),
            "user_sticker_id_counts": defaultdict(Counter),
            "server_hourly_activity": Counter(),
            "channel_hourly_activity": defaultdict(Counter),
            "thread_hourly_activity": defaultdict(Counter),
            "user_hourly_activity": defaultdict(Counter),
            "user_emoji_received_counts": defaultdict(Counter),
            "current_members_list": [], "initial_member_status_counts": Counter(),
            "channel_counts": Counter(), "all_roles_list": [], "boosters": [],
            "voice_channel_static_data": [], "invites_data": [], "webhooks_data": [],
            "integrations_data": [], "oldest_members_data": [],
            "audit_log_entries_added": 0, "newest_processed_audit_log_id": None,
            "scan_end_time": None, "overall_duration": datetime.timedelta(0),
            "audit_log_scan_duration": datetime.timedelta(0),
            "files_to_send": [], "report_messages_sent": 0,
            "server_emojis_cache": {}, "server_sticker_ids_cache": set(),
            "server_stickers_cache_objects": {}, # Có thể cần cho sticker ít dùng
            "can_scan_invites": False, "can_scan_webhooks": False,
            "can_scan_integrations": False, "can_scan_audit_log": False,
            "can_scan_reactions": False, "can_scan_archived_threads": False,
            "permission_audit_results": defaultdict(list),
            "role_change_stats": Counter(), "user_role_changes": defaultdict(list),
            "scan_id": None, # Sẽ được điền sau khi tạo record
            "scan_started": False # Cờ để biết đã qua init chưa
        }

        scan_id: Optional[int] = None
        try:
            # Tạo bản ghi quét trong DB trước
            scan_id = await database.create_scan_record(ctx.guild.id, ctx.author.id)
            if not scan_id:
                await ctx.send(f"{e('error')} Không thể tạo bản ghi quét trong database. Vui lòng thử lại sau.")
                if ctx.command: ctx.command.reset_cooldown(ctx)
                return
            scan_data["scan_id"] = scan_id # Lưu scan_id vào data
            log.info(f"Đã khởi tạo quét với scan_id: {scan_id}")

            log.info(f"{e('loading')} Khởi tạo quét sâu cho server: [bold cyan]{scan_data['server'].name}[/] ({scan_data['server'].id})")
            if config.ENABLE_REACTION_SCAN: log.warning("[bold yellow]!!! Quét biểu cảm (Reaction Scan) đang BẬT. Quá trình quét có thể chậm hơn !!![/bold yellow]")
            if scan_data["admin_dm_test"]: log.info("[bold magenta]!!! Chế độ TEST DM đang BẬT. DM sẽ chỉ gửi đến ADMIN_USER_ID !!![/bold magenta]")
            else: log.info("[bold green]!!! Chế độ Gửi DM Bình Thường đang BẬT. DM sẽ gửi đến role cấu hình !!![/bold green]")

            # Bước 1: Khởi tạo và kiểm tra
            init_successful = await initialize_scan(scan_data)
            if not init_successful:
                 # Cập nhật trạng thái DB nếu init lỗi
                 await database.update_scan_status(scan_id, status='failed', error="Initialization failed", end_time=discord.utils.utcnow())
                 if ctx.command: ctx.command.reset_cooldown(ctx)
                 return

            # Bước 2: Quét kênh và luồng
            await scan_all_channels_and_threads(scan_data)
            scan_data["scan_end_time"] = discord.utils.utcnow() # Thời điểm quét kênh xong

            # Bước 3: Xử lý dữ liệu phụ trợ (audit log, boosters, v.v.)
            await process_additional_data(scan_data)

            # Bước 4: Lưu kết quả tổng hợp của user vào DB cho website
            try:
                log.info(f"{e('loading')} Đang chuẩn bị dữ liệu xếp hạng để lưu...")
                # <<< GỌI HÀM TỪ dm_sender >>>
                ranking_data_for_db = await _prepare_ranking_data(scan_data, ctx.guild)

                log.info(f"Đã tính toán ranking_data. Bắt đầu lưu kết quả user vào DB (scan_id: {scan_id})...")
                await save_aggregated_results_to_db(scan_data, ranking_data_for_db)
                log.info(f"Đã lưu kết quả user và đánh dấu website_accessible=True cho scan_id: {scan_id}")
            except Exception as db_save_err:
                log.error(f"{e('error')} Lỗi nghiêm trọng khi lưu kết quả user vào DB: {db_save_err}", exc_info=True)
                scan_data["scan_errors"].append(f"Lỗi lưu DB Web: {db_save_err}")
                # Cập nhật lỗi vào DB nhưng vẫn tiếp tục để gửi báo cáo Discord
                await database.update_scan_status(scan_id, status='running', error=f"DB Save Error: {db_save_err}")

            # Bước 5: Tạo và gửi báo cáo Embeds vào kênh Discord
            await generate_and_send_reports(scan_data)

            # Bước 6: Tạo file export nếu yêu cầu
            if scan_data["export_csv"] or scan_data["export_json"]:
                await generate_export_files(scan_data)
            else:
                 log.info("Bỏ qua tạo file export do không có yêu cầu.")

            # Bước 7: Gửi DM cá nhân (chạy nền) nếu cấu hình
            should_send_dm = config.DM_REPORT_RECIPIENT_ROLE_ID or scan_data["admin_dm_test"]
            if should_send_dm:
                 is_testing = scan_data["admin_dm_test"]
                 log.debug(f"[Core Logic] Preparing to send DM. is_testing_mode flag from scan_data: {is_testing}")
                 log.info(f"{e('loading')} Bắt đầu gửi báo cáo DM cá nhân...")
                 asyncio.create_task(send_personalized_dm_reports(scan_data, is_testing_mode=is_testing), name=f"DMReportSender-{ctx.guild.id}")
                 log.info("Đã tạo task gửi DM chạy nền.")
            else:
                 log.info("Bỏ qua gửi DM do chưa cấu hình role người nhận và không bật test mode.")

            # Bước 8: Cập nhật trạng thái cuối cùng trong DB
            final_status = 'completed'
            final_error = None
            if scan_data["scan_errors"]:
                final_status = 'completed_with_errors'
                final_error_messages = []
                for err_item in scan_data["scan_errors"]:
                    if isinstance(err_item, str): final_error_messages.append(err_item[:250])
                    elif isinstance(err_item, Exception): final_error_messages.append(f"{type(err_item).__name__}: {str(err_item)[:200]}")
                    else: final_error_messages.append(f"Unknown error type: {type(err_item).__name__}")
                final_error = "; ".join(final_error_messages[:5]) # Lấy 5 lỗi đầu
                if len(final_error) > 1000: final_error = final_error[:1000] + "..." # Giới hạn độ dài lỗi

            await database.update_scan_status(
                scan_id,
                status=final_status,
                end_time=discord.utils.utcnow(), # Thời gian hoàn thành toàn bộ lệnh
                website_ready=True, # Web đã ready từ sau bước 4
                error=final_error
            )
            log.info(f"Scan {scan_id} được đánh dấu là '{final_status}' trong DB." + (f" Lỗi: {final_error}" if final_error else ""))

        # --- Xử lý các Exception cụ thể ---
        except commands.BotMissingPermissions as bmp_error:
             log.error(f"Quét dừng do thiếu quyền bot: {bmp_error.missing_permissions}")
             error_msg = f"Bot Missing Permissions: {', '.join(bmp_error.missing_permissions)}"
             if scan_id: await database.update_scan_status(scan_id, status='failed', error=error_msg[:1000], end_time=discord.utils.utcnow())
             # Chỉ reset cooldown nếu lỗi xảy ra *trước khi* quá trình quét chính bắt đầu
             if not scan_data.get("scan_started", False) and ctx.command: ctx.command.reset_cooldown(ctx)
             # Gửi thông báo lỗi chung qua on_command_error
             raise bmp_error # Ném lại lỗi để on_command_error xử lý

        except ConnectionError as conn_err:
             log.error(f"Quét dừng do lỗi kết nối: {conn_err}")
             error_msg = f"Connection Error: {str(conn_err)[:250]}"
             if scan_id: await database.update_scan_status(scan_id, status='failed', error=error_msg, end_time=discord.utils.utcnow())
             if not scan_data.get("scan_started", False) and ctx.command: ctx.command.reset_cooldown(ctx)
             # Có thể gửi thông báo lỗi ở đây hoặc để on_command_error
             await ctx.send(f"{e('error')} Lỗi kết nối trong quá trình quét. Vui lòng thử lại sau.")

        except Exception as ex: # Bắt các lỗi không mong muốn khác
            log.critical(f"{e('error')} LỖI KHÔNG MONG MUỐN trong quá trình quét sâu:", exc_info=True)
            error_msg = f"Unexpected Error: {type(ex).__name__} - {str(ex)[:200]}"
            scan_data["scan_errors"].append(f"Lỗi nghiêm trọng không xác định: {type(ex).__name__} - {str(ex)[:200]}")
            # Cập nhật DB với trạng thái failed
            if scan_id: await database.update_scan_status(scan_id, status='failed', error=error_msg, end_time=discord.utils.utcnow())
            # Thông báo cho người dùng
            try: await ctx.send(f"{e('error')} Đã xảy ra lỗi nghiêm trọng không mong muốn trong quá trình quét. Báo cáo có thể không đầy đủ hoặc không được tạo.")
            except Exception: pass
            # Reset cooldown nếu lỗi xảy ra sớm
            if not scan_data.get("scan_started", False) and ctx.command: ctx.command.reset_cooldown(ctx)
            # Không cần raise lại nếu đã xử lý ở đây, trừ khi muốn on_command_error log thêm

        # --- Khối Finally: Luôn chạy để dọn dẹp ---
        finally:
            # Bước 9: Gửi tin nhắn hoàn tất cuối cùng và dọn dẹp
            await finalize_scan(scan_data) # Gửi tin nhắn trung gian A, dọn dẹp status msg
            discord_logging.set_log_target_thread(None) # Reset target log
            log.info(f"[dim]Hoàn tất dọn dẹp sau lệnh {ctx.command.name if ctx.command else 'unknown'}.[/dim]")


    # --- Các lệnh command ---
    @commands.command(
        name='romi',
        aliases=['sds', 'serverdeepscan'],
        help=(
            "Thực hiện quét sâu server (CHẾ ĐỘ TEST).\n"
            "Các báo cáo DM sẽ được gửi đến ADMIN_USER_ID trong file .env.\n"
            "Usage: `Shiromi romi [export_csv=True/False] [export_json=True/False] [keywords=từ khóa1,từ khóa2]`\n"
            "Mặc định không export file và không tìm keywords."
        ),
        brief='(OWNER/PROXY) Quét sâu, gửi DM test cho admin.'
    )
    @commands.cooldown(1, 7200, commands.BucketType.guild) # Cooldown 2 giờ mỗi server
    @commands.guild_only()
    async def server_deep_scan_test(self, ctx: commands.Context, export_csv: bool = False, export_json: bool = False, *, keywords: Optional[str] = None):
        """Lệnh quét sâu ở chế độ test."""
        await self._perform_deep_scan(ctx=ctx, export_csv=export_csv, export_json=export_json, admin_dm_test=True, keywords=keywords)


    @commands.command(
        name='Shiromi',
        help=(
            "Thực hiện quét sâu server (CHẾ ĐỘ BÌNH THƯỜNG).\n"
            "Các báo cáo DM sẽ được gửi đến những người dùng có role được cấu hình trong DM_REPORT_RECIPIENT_ROLE_ID.\n"
            "Usage: `Shiromirun [export_csv=True/False] [export_json=True/False] [keywords=từ khóa1,từ khóa2]`\n"
            "Mặc định không export file và không tìm keywords."
        ),
        brief='(OWNER/PROXY) Quét sâu, gửi DM cho role cấu hình.'
    )
    @commands.cooldown(1, 7200, commands.BucketType.guild) # Cooldown 2 giờ mỗi server
    @commands.guild_only()
    async def server_deep_scan_normal(self, ctx: commands.Context, export_csv: bool = False, export_json: bool = False, *, keywords: Optional[str] = None):
        """Lệnh quét sâu ở chế độ bình thường."""
        await self._perform_deep_scan(ctx=ctx, export_csv=export_csv, export_json=export_json, admin_dm_test=False, keywords=keywords)

    # Lệnh ping_shiromi để test Cog và quyền
    @commands.command(name='ping_shiromi', help="Kiểm tra bot Shiromi có hoạt động không.", brief="(OWNER/PROXY) Ping bot.")
    async def ping_shiromi_command(self, ctx: commands.Context):
        """Lệnh ping đơn giản để kiểm tra bot."""
        latency = self.bot.latency * 1000 # ms
        log.info(f"Lệnh ping_shiromi được gọi bởi {ctx.author} (ID: {ctx.author.id}). Latency: {latency:.2f}ms")
        await ctx.send(f"Pong! <:a_ct_meomeoden:1367435015836598303> Latency: `{latency:.2f}ms`. Được gọi bởi {ctx.author.mention}")


async def setup(bot: commands.Bot):
    """Hàm setup để thêm Cog vào bot."""
    await bot.add_cog(ServerDeepScan(bot))
    log.info("Cog ServerDeepScan đã được tải với các lệnh quét và ping.")

# --- END OF FILE cogs/deep_scan_cog.py ---