# --- START OF FILE cogs/deep_scan_cog.py ---
import discord
from discord.ext import commands
import time
import datetime
import logging
import asyncio
from typing import List, Dict, Any, Optional, Union, Counter as TypingCounter, DefaultDict as TypingDefaultDict, Tuple
from collections import Counter, defaultdict

import config
import utils
import database
import discord_logging


from .deep_scan_helpers import (
    initialize_scan, scan_all_channels_and_threads, process_additional_data,
    generate_and_send_reports, generate_export_files, finalize_scan,
    send_personalized_dm_reports
)

from reporting import embeds_dm

log = logging.getLogger(__name__)

# --- HÀM LƯU KẾT QUẢ VÀO DB (MỚI) ---
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
        avatar_url = str(member.display_avatar.url) if member and member.display_avatar else None # LẤY AVATAR URL

        # Thu thập dữ liệu cơ bản từ user_activity
        user_result = {
            "user_id": user_id,
            "display_name_at_scan": display_name[:100], # Giới hạn độ dài
            "avatar_url_at_scan": avatar_url, # LƯU AVATAR URL
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
            "ranking_data": {}, # Sẽ điền bên dưới
            "achievement_data": {}, # Sẽ điền bên dưới
        }

        # Thu thập dữ liệu ranking
        for rank_key, ranks in ranking_data.items():
            rank = ranks.get(user_id)
            if rank:
                user_result["ranking_data"][rank_key] = rank

        # Thu thập dữ liệu thành tích (ví dụ)
        # Lấy top emoji/sticker của user (tương tự logic trong embeds_dm)
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
                # Cần fetch tên sticker nếu muốn lưu tên
                user_result["achievement_data"]["top_sticker"] = {"id": top_sticker_id_str, "count": top_sticker_count}
            except IndexError: pass

        # Thêm user_result vào list chờ lưu
        user_results_to_save.append(user_result)

    # Lưu hàng loạt vào DB
    if user_results_to_save:
        log.info(f"Chuẩn bị lưu {len(user_results_to_save)} kết quả user vào database...")
        await database.save_user_scan_results(scan_id, user_results_to_save)
        # Sau khi lưu thành công, đánh dấu scan là sẵn sàng cho web
        await database.update_scan_status(scan_id, status='running', website_ready=True)
    else:
        log.info("Không có kết quả user nào để lưu vào database.")
# --- KẾT THÚC HÀM LƯU ---


class ServerDeepScan(commands.Cog):
    """Cog chứa lệnh quét sâu server."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def _perform_deep_scan(
        self,
        ctx: commands.Context,
        export_csv: bool,
        export_json: bool,
        admin_dm_test: bool,
        keywords: Optional[str]
    ):
        """Hàm thực hiện logic quét sâu chính."""
        start_time_cmd = time.monotonic()
        overall_start_time = discord.utils.utcnow()
        e = lambda name: utils.get_emoji(name, self.bot)

        # --- KHỞI TẠO SCAN_DATA (Giữ nguyên cấu trúc cũ) ---
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
            "tracked_role_grant_counts": Counter(), # {(user_id, role_id): count}
            "user_distinct_channel_counts": Counter(),
            "user_channel_message_counts": defaultdict(lambda: defaultdict(int)),
            "user_most_active_channel": {},
            "user_activity_message_counts": Counter(),
            "user_total_custom_emoji_content_counts": Counter(),
            "user_sticker_id_counts": defaultdict(Counter),
            "server_hourly_activity": Counter(),
            "channel_hourly_activity": defaultdict(Counter),
            "thread_hourly_activity": defaultdict(Counter),
            "user_hourly_activity": defaultdict(Counter), # THÊM MỚI
            "user_emoji_received_counts": defaultdict(Counter), # THÊM MỚI
            "current_members_list": [], "initial_member_status_counts": Counter(),
            "channel_counts": Counter(), "all_roles_list": [], "boosters": [],
            "voice_channel_static_data": [], "invites_data": [], "webhooks_data": [],
            "integrations_data": [], "oldest_members_data": [],
            "audit_log_entries_added": 0, "newest_processed_audit_log_id": None,
            "scan_end_time": None, "overall_duration": datetime.timedelta(0),
            "audit_log_scan_duration": datetime.timedelta(0),
            "files_to_send": [], "report_messages_sent": 0,
            "server_emojis_cache": {}, "server_sticker_ids_cache": set(),
            "server_stickers_cache_objects": {},
            "can_scan_invites": False, "can_scan_webhooks": False,
            "can_scan_integrations": False, "can_scan_audit_log": False,
            "can_scan_reactions": False, "can_scan_archived_threads": False,
            "permission_audit_results": defaultdict(list),
            "role_change_stats": Counter(), "user_role_changes": defaultdict(list),
            "scan_id": None # <<< THÊM MỚI: Để lưu scan_id
        }

        scan_id: Optional[int] = None # Biến cục bộ để lưu scan_id
        try:
            # --- TẠO BẢN GHI QUÉT MỚI ---
            scan_id = await database.create_scan_record(ctx.guild.id, ctx.author.id)
            if not scan_id:
                await ctx.send(f"{e('error')} Không thể tạo bản ghi quét trong database. Vui lòng thử lại sau.")
                if ctx.command: ctx.command.reset_cooldown(ctx)
                return
            scan_data["scan_id"] = scan_id # Lưu scan_id vào dữ liệu quét
            log.info(f"Đã khởi tạo quét với scan_id: {scan_id}")
            # --- KẾT THÚC TẠO BẢN GHI ---

            log.info(f"{e('loading')} Khởi tạo quét sâu cho server: [bold cyan]{scan_data['server'].name}[/] ({scan_data['server'].id})")
            if config.ENABLE_REACTION_SCAN:
                log.warning("[bold yellow]!!! Quét biểu cảm (Reaction Scan) đang BẬT. Quá trình quét có thể chậm hơn !!![/bold yellow]")
            if scan_data["admin_dm_test"]:
                 log.info("[bold magenta]!!! Chế độ TEST DM đang BẬT. DM sẽ chỉ gửi đến ADMIN_USER_ID !!![/bold magenta]")
            else:
                 log.info("[bold green]!!! Chế độ Gửi DM Bình Thường đang BẬT. DM sẽ gửi đến role cấu hình !!![/bold green]")

            # Giai đoạn 1: Khởi tạo và Kiểm tra
            init_successful = await initialize_scan(scan_data)
            if not init_successful:
                 await database.update_scan_status(scan_id, status='failed', error="Initialization failed", end_time=discord.utils.utcnow())
                 if ctx.command: ctx.command.reset_cooldown(ctx)
                 return

            # Giai đoạn 2: Quét Kênh và Luồng
            await scan_all_channels_and_threads(scan_data)
            scan_data["scan_end_time"] = discord.utils.utcnow()

            # Giai đoạn 3: Fetch/Xử lý Dữ liệu Phụ trợ & Phân tích
            await process_additional_data(scan_data)

            # --- GIAI ĐOẠN 3.5: LƯU KẾT QUẢ VÀO DB CHO WEB ---
            try:
                log.info(f"{e('loading')} Đang chuẩn bị dữ liệu xếp hạng để lưu...")
                # Tính toán ranking_data (giống như trong dm_sender)
                ranking_data_for_db = await embeds_dm._prepare_ranking_data(scan_data, ctx.guild)
                log.info(f"Đã tính toán ranking_data. Bắt đầu lưu kết quả user vào DB (scan_id: {scan_id})...")
                await save_aggregated_results_to_db(scan_data, ranking_data_for_db)
                log.info(f"Đã lưu kết quả user và đánh dấu website_accessible=True cho scan_id: {scan_id}")
            except Exception as db_save_err:
                log.error(f"{e('error')} Lỗi nghiêm trọng khi lưu kết quả user vào DB: {db_save_err}", exc_info=True)
                scan_data["scan_errors"].append(f"Lỗi lưu DB Web: {db_save_err}")
                # Không dừng quét, nhưng ghi nhận lỗi
                await database.update_scan_status(scan_id, status='running', error=f"DB Save Error: {db_save_err}") # Ghi lỗi vào scan record
            # --- KẾT THÚC GIAI ĐOẠN 3.5 ---

            # Giai đoạn 4: Tạo Báo cáo Embeds CÔNG KHAI
            await generate_and_send_reports(scan_data)

            # Giai đoạn 5: Tạo File Xuất
            if scan_data["export_csv"] or scan_data["export_json"]:
                await generate_export_files(scan_data)
            else:
                 log.info("Bỏ qua tạo file export do không có yêu cầu.")

            # Giai đoạn 6: Gửi Báo cáo DM Cá nhân
            should_send_dm = config.DM_REPORT_RECIPIENT_ROLE_ID or scan_data["admin_dm_test"]
            if should_send_dm:
                 is_testing = scan_data["admin_dm_test"]
                 log.debug(f"[Core Logic] Preparing to send DM. is_testing_mode flag from scan_data: {is_testing}")
                 log.info(f"{e('loading')} Bắt đầu gửi báo cáo DM cá nhân...")
                 asyncio.create_task(send_personalized_dm_reports(scan_data, is_testing_mode=is_testing), name=f"DMReportSender-{ctx.guild.id}")
                 log.info("Đã tạo task gửi DM chạy nền.")
            else:
                 log.info("Bỏ qua gửi DM do chưa cấu hình role người nhận và không bật test mode.")

            # --- ĐÁNH DẤU QUÉT HOÀN THÀNH TRONG DB ---
            final_status = 'completed'
            final_error = None
            if scan_data["scan_errors"]:
                # Có thể đặt là 'completed_with_errors' nếu muốn phân biệt
                final_status = 'completed' # Giữ là completed, lỗi sẽ được ghi lại
                final_error_messages = []
                for err_item in scan_data["scan_errors"]:
                    if isinstance(err_item, str):
                        final_error_messages.append(err_item[:250]) # Giới hạn độ dài từng lỗi
                    elif isinstance(err_item, Exception):
                        final_error_messages.append(f"{type(err_item).__name__}: {str(err_item)[:200]}")
                final_error = "; ".join(final_error_messages[:5]) # Lấy tối đa 5 lỗi đầu, giới hạn tổng độ dài
                if len(final_error) > 1000: final_error = final_error[:1000] + "..."


            await database.update_scan_status(
                scan_id,
                status=final_status,
                end_time=discord.utils.utcnow(),
                website_ready=True, # Đảm bảo cờ này vẫn là True
                error=final_error
            )
            log.info(f"Scan {scan_id} được đánh dấu là '{final_status}' trong DB." + (f" Lỗi: {final_error}" if final_error else ""))
            # --- KẾT THÚC ĐÁNH DẤU ---

        except commands.BotMissingPermissions as bmp_error:
             log.error(f"Quét dừng do thiếu quyền bot: {bmp_error.missing_permissions}")
             error_msg = f"Bot Missing Permissions: {', '.join(bmp_error.missing_permissions)}"
             if scan_id: await database.update_scan_status(scan_id, status='failed', error=error_msg[:1000], end_time=discord.utils.utcnow())
             if not scan_data.get("scan_started", False) and ctx.command: ctx.command.reset_cooldown(ctx)
        except ConnectionError as conn_err:
             log.error(f"Quét dừng do lỗi kết nối: {conn_err}")
             error_msg = f"Connection Error: {str(conn_err)[:250]}"
             if scan_id: await database.update_scan_status(scan_id, status='failed', error=error_msg, end_time=discord.utils.utcnow())
             if not scan_data.get("scan_started", False) and ctx.command: ctx.command.reset_cooldown(ctx)
        except Exception as ex:
            log.critical(f"{e('error')} LỖI KHÔNG MONG MUỐN trong quá trình quét sâu:", exc_info=True)
            error_msg = f"Unexpected Error: {type(ex).__name__} - {str(ex)[:200]}"
            scan_data["scan_errors"].append(f"Lỗi nghiêm trọng không xác định: {type(ex).__name__} - {str(ex)[:200]}")
            if scan_id: await database.update_scan_status(scan_id, status='failed', error=error_msg, end_time=discord.utils.utcnow())
            try: await ctx.send(f"{e('error')} Đã xảy ra lỗi nghiêm trọng không mong muốn trong quá trình quét. Báo cáo có thể không đầy đủ.")
            except Exception: pass
            if not scan_data.get("scan_started", False) and ctx.command: ctx.command.reset_cooldown(ctx)
        finally:
            # Giai đoạn 7: Hoàn tất và Dọn dẹp (finalize_scan đã chứa logic gửi link)
            await finalize_scan(scan_data) # finalize_scan sẽ dùng scan_data['scan_id'] và config.WEBSITE_BASE_URL
            discord_logging.set_log_target_thread(None) # Đặt lại target log sau khi lệnh hoàn tất
            log.info(f"[dim]Hoàn tất dọn dẹp sau lệnh {ctx.command.name if ctx.command else 'unknown'}.[/dim]")

    # --- Lệnh !romi (test) và !shiromirun (normal) ---
    @commands.command(
        name='romi',
        aliases=['sds', 'serverdeepscan'],
        help=(
            "Thực hiện quét sâu server (CHẾ ĐỘ TEST).\n"
            "Các báo cáo DM sẽ được gửi đến ADMIN_USER_ID trong file .env.\n"
            "Usage: `Shiromi [export_csv=True/False] [export_json=True/False] [keywords=từ khóa1,từ khóa2]`\n"
            "Mặc định không export file và không tìm keywords."
        ),
        brief='(OWNER ONLY - ADMIN TEST) Quét sâu, gửi DM test cho admin.'
    )
    @commands.is_owner()
    @commands.cooldown(1, 7200, commands.BucketType.guild) # Cooldown 2 giờ/server
    @commands.guild_only()
    async def server_deep_scan_test(self, ctx: commands.Context, export_csv: bool = False, export_json: bool = False, *, keywords: Optional[str] = None):
        """
        OWNER ONLY.
        Thực hiện quét sâu dữ liệu của server Discord này.
        CHẾ ĐỘ TEST: Báo cáo DM chi tiết sẽ được gửi đến ADMIN_USER_ID.
        """
        await self._perform_deep_scan(ctx=ctx, export_csv=export_csv, export_json=export_json, admin_dm_test=True, keywords=keywords)


    @commands.command(
        name='shiromirun',
        help=(
            "Thực hiện quét sâu server (CHẾ ĐỘ BÌNH THƯỜNG).\n"
            "Các báo cáo DM sẽ được gửi đến những người dùng có role được cấu hình trong DM_REPORT_RECIPIENT_ROLE_ID.\n"
            "Usage: `Shiromirun [export_csv=True/False] [export_json=True/False] [keywords=từ khóa1,từ khóa2]`\n"
            "Mặc định không export file và không tìm keywords."
        ),
        brief='(OWNER ONLY - NORMAL RUN) Quét sâu, gửi DM cho role cấu hình.'
    )
    @commands.is_owner()
    @commands.cooldown(1, 7200, commands.BucketType.guild) # Cooldown 2 giờ/server
    @commands.guild_only()
    async def server_deep_scan_normal(self, ctx: commands.Context, export_csv: bool = False, export_json: bool = False, *, keywords: Optional[str] = None):
        """
        OWNER ONLY.
        Thực hiện quét sâu dữ liệu của server Discord này.
        CHẾ ĐỘ BÌNH THƯỜNG: Báo cáo DM chi tiết sẽ được gửi đến những người dùng có role được cấu hình.
        """
        await self._perform_deep_scan(ctx=ctx, export_csv=export_csv, export_json=export_json, admin_dm_test=False, keywords=keywords)


async def setup(bot: commands.Bot):
    """Hàm setup để thêm Cog vào bot."""
    await bot.add_cog(ServerDeepScan(bot))
    log.info("Cog ServerDeepScan đã được tải với 2 lệnh quét.")

# --- END OF FILE cogs/deep_scan_cog.py ---