# --- START OF FILE cogs/deep_scan_helpers/report_generation.py ---
import discord
from discord.ext import commands
import logging
import asyncio
from typing import Dict, Any, List, Optional, Union
import time
from collections import Counter, defaultdict
import datetime
import config
import utils
import database
from reporting import embeds_guild, embeds_user, embeds_items, embeds_analysis, embeds_dm

log = logging.getLogger(__name__)

# --- Hàm Gửi Embed Helper (Giữ nguyên) ---
async def _send_report_embeds(
    scan_data: Dict[str, Any],
    embed_list: List[discord.Embed],
    type_name: str,
    target_channel: Union[discord.TextChannel, discord.Thread] # Kênh đích
):
    scan_errors: List[str] = scan_data["scan_errors"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    report_messages_sent = scan_data.setdefault("report_messages_sent", 0)

    if not target_channel:
        log.error(f"Không có kênh đích hợp lệ để gửi báo cáo '{type_name}'.")
        scan_errors.append(f"Lỗi gửi '{type_name}': Kênh đích không hợp lệ.")
        return

    if not embed_list:
        log.info(f"{e('info')} Không có dữ liệu để tạo báo cáo '{type_name}', bỏ qua.")
        return

    log.info(f"{e('loading')} Đang gửi {len(embed_list)} embed(s) cho báo cáo '{type_name}' vào kênh #{target_channel.name} ({target_channel.id})...")
    for i, embed in enumerate(embed_list):
        if not isinstance(embed, discord.Embed):
             log.warning(f"Bỏ qua gửi mục không phải Embed trong '{type_name}': {type(embed)}")
             continue
        try:
            await target_channel.send(embed=embed)
            report_messages_sent += 1
            log.debug(f"  Đã gửi embed {i+1}/{len(embed_list)} cho '{type_name}' vào #{target_channel.name}.")
            await asyncio.sleep(1.6) # Delay nhẹ giữa các tin nhắn
        except discord.Forbidden:
            error_msg = f"Lỗi gửi '{type_name}' (Embed {i+1}) vào #{target_channel.name}: Thiếu quyền."
            log.error(f"{e('error')} {error_msg}")
            scan_errors.append(error_msg)
            break
        except discord.HTTPException as send_err:
            error_msg = f"Lỗi gửi '{type_name}' (Embed {i+1} vào #{target_channel.name}, HTTP {send_err.status}): {send_err.text}"
            log.error(f"{e('error')} {error_msg}")
            scan_errors.append(error_msg)
            if send_err.status == 429:
                retry_after = send_err.retry_after or 5.0
                wait_time = retry_after + config.LOG_RETRY_AFTER_BUFFER
                log.warning(f"    Bị rate limit, chờ {wait_time:.2f}s...")
                await asyncio.sleep(wait_time)
            elif send_err.status >= 500:
                log.warning(f"    Lỗi server Discord ({send_err.status}), chờ 5s...")
                await asyncio.sleep(5.0)
            else:
                log.warning(f"    Lỗi client Discord ({send_err.status}), chờ 3s...")
                await asyncio.sleep(3.0)
        except Exception as send_e:
            error_msg = f"Lỗi không xác định gửi '{type_name}' (Embed {i+1} vào #{target_channel.name}): {send_e}"
            log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg); await asyncio.sleep(2.0)

    scan_data["report_messages_sent"] = report_messages_sent

# --- Hàm Chính Tạo và Gửi Báo cáo CÔNG KHAI (Cập nhật) ---
async def generate_and_send_reports(scan_data: Dict[str, Any]):
    """Tạo và gửi các báo cáo embeds CÔNG KHAI theo thứ tự nhóm Ít/Nhiều."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    scan_errors: List[str] = scan_data["scan_errors"]
    ctx: commands.Context = scan_data["ctx"]
    report_messages_sent = scan_data.setdefault("report_messages_sent", 0)
    files_to_send: List[discord.File] = scan_data["files_to_send"] # Lấy list file
    log_thread: Optional[discord.Thread] = scan_data.get("log_thread") # Lấy log thread


    # Xác định kênh gửi báo cáo (Giữ nguyên)
    report_channel: Union[discord.TextChannel, discord.Thread] = ctx.channel # Mặc định là kênh gốc
    report_channel_id = config.REPORT_CHANNEL_ID
    report_channel_mention = ctx.channel.mention # Mention mặc định
    if report_channel_id:
        found_channel = server.get_channel(report_channel_id)
        if isinstance(found_channel, discord.TextChannel):
            perms = found_channel.permissions_for(server.me)
            if perms.send_messages and perms.embed_links:
                report_channel = found_channel
                report_channel_mention = report_channel.mention
                log.info(f"Sẽ gửi báo cáo vào kênh được chỉ định: {report_channel.mention}")
            else:
                error_msg = f"Bot thiếu quyền 'Send Messages' hoặc 'Embed Links' trong kênh báo cáo {found_channel.mention}. Gửi vào kênh gốc."
                log.error(error_msg)
                scan_errors.append(error_msg)
        elif found_channel:
            error_msg = f"ID kênh báo cáo ({report_channel_id}) không phải là kênh Text. Gửi vào kênh gốc."
            log.error(error_msg)
            scan_errors.append(error_msg)
        else:
            error_msg = f"Không tìm thấy kênh báo cáo với ID {report_channel_id}. Gửi vào kênh gốc."
            log.error(error_msg)
            scan_errors.append(error_msg)

    log.info(f"\n--- [bold green]{e('loading')} Đang Tạo Báo Cáo Embeds Công Khai vào kênh {report_channel.mention}[/bold green] ---")
    start_time_reports = time.monotonic()

    # Chuẩn bị dữ liệu cần thiết (Giữ nguyên)
    user_activity = scan_data["user_activity"]
    user_link_counts = scan_data.get("user_link_counts", Counter())
    user_image_counts = scan_data.get("user_image_counts", Counter())
    user_total_custom_emoji_content_counts = scan_data.get("user_total_custom_emoji_content_counts", Counter())
    user_sticker_counts = scan_data.get("user_sticker_counts", Counter())
    user_mention_received_counts = scan_data.get("user_mention_received_counts", Counter())
    user_mention_given_counts = scan_data.get("user_mention_given_counts", Counter())
    user_reply_counts = scan_data.get("user_reply_counts", Counter())
    user_reaction_received_counts = scan_data.get("user_reaction_received_counts", Counter())
    user_reaction_given_counts = scan_data.get("user_reaction_given_counts", Counter())
    user_reaction_emoji_given_counts = scan_data.get("user_reaction_emoji_given_counts", defaultdict(Counter))
    filtered_reaction_counts = scan_data.get("filtered_reaction_emoji_counts", Counter())
    sticker_usage_counts = scan_data.get("sticker_usage_counts", Counter())
    user_thread_creation_counts = scan_data.get("user_thread_creation_counts", Counter())
    oldest_members_data = scan_data.get("oldest_members_data", [])
    boosters = scan_data.get("boosters", [])
    tracked_role_grant_counts = scan_data.get("tracked_role_grant_counts", Counter())
    channel_details = scan_data.get("channel_details", [])
    voice_channel_static_data = scan_data.get("voice_channel_static_data", []) # Giữ lại nếu cần
    user_distinct_channel_counts = scan_data.get("user_distinct_channel_counts", Counter())
    server_hourly_activity = scan_data.get("server_hourly_activity", Counter())
    channel_hourly_activity = scan_data.get("channel_hourly_activity", defaultdict(Counter))
    thread_hourly_activity = scan_data.get("thread_hourly_activity", defaultdict(Counter))
    overall_total_reaction_count = scan_data.get("overall_total_reaction_count", 0) # Thô
    overall_filtered_reaction_count = scan_data.get("overall_total_filtered_reaction_count", 0) # Đã lọc
    user_emoji_received_counts = scan_data.get("user_emoji_received_counts", defaultdict(Counter))
    overall_custom_emoji_content_counts = scan_data.get("overall_custom_emoji_content_counts", Counter())


    # === KHỐI TẠO EMBEDS (Giữ nguyên) ===
    summary_embeds = []
    analysis_embeds = []
    least_activity_embeds = []
    most_activity_embeds = []
    special_embeds = []
    error_embeds = []
    # Helper _try_create_and_add_embed (Giữ nguyên)
    async def _try_create_and_add_embed(embed_creation_func, target_list, error_list, *args, **kwargs):
        func_name = embed_creation_func.__name__
        try:
            if asyncio.iscoroutinefunction(embed_creation_func):
                embed_or_list = await embed_creation_func(*args, **kwargs)
            else:
                embed_or_list = embed_creation_func(*args, **kwargs)
            if isinstance(embed_or_list, list):
                for embed in embed_or_list:
                    if isinstance(embed, discord.Embed): target_list.append(embed)
                    elif embed is not None: log.debug(f"Hàm '{func_name}' trả về phần tử không phải Embed trong list.")
            elif isinstance(embed_or_list, discord.Embed):
                target_list.append(embed_or_list)
            elif embed_or_list is not None:
                 log.debug(f"Hàm '{func_name}' trả về giá trị không phải Embed hoặc list.")
        except Exception as ex:
            error_msg = f"Lỗi tạo embed '{func_name}': {ex}"
            log.error(f"{e('error')} {error_msg}", exc_info=True)
            error_list.append(error_msg)

    log.info(f"--- {e('loading')} Đang tạo các embeds ---")
    # Tạo các embeds (Giữ nguyên logic gọi các hàm tạo embed)
    # ... (Toàn bộ các lệnh await _try_create_and_add_embed(...)) ...
    # --- Nhóm 1: Tổng Quan & Phân Tích Chung ---
    log.info(f"--- {e('info')} Nhóm 1: Tổng Quan & Phân Tích Chung ---")
    await _try_create_and_add_embed(
        embeds_guild.create_summary_embed, summary_embeds, scan_errors,
        server, bot, scan_data["processed_channels_count"], scan_data["processed_threads_count"],
        scan_data["skipped_channels_count"], scan_data["skipped_threads_count"],
        scan_data["overall_total_message_count"], len(user_activity), scan_data["overall_duration"],
        scan_data["initial_member_status_counts"], scan_data["channel_counts"],
        len(scan_data["all_roles_list"]), scan_data["overall_start_time"],
        scan_data, ctx=ctx, overall_total_reaction_count=overall_filtered_reaction_count
    )
    await _try_create_and_add_embed( # Hàm này trả về list
        embeds_analysis.create_keyword_analysis_embeds, analysis_embeds, scan_errors,
        scan_data.get("keyword_counts", Counter()),
        scan_data.get("channel_keyword_counts", defaultdict(Counter)),
        scan_data.get("thread_keyword_counts", defaultdict(Counter)),
        scan_data.get("user_keyword_counts", defaultdict(Counter)),
        server, bot, scan_data.get("target_keywords", [])
    )
    await _try_create_and_add_embed(
        embeds_items.create_unused_emoji_embed, analysis_embeds, scan_errors,
        server, overall_custom_emoji_content_counts, bot
    )
    # --- Nhóm 2: Hoạt Động Ít Nhất ---
    log.info(f"--- {e('info')} Nhóm 2: Hoạt Động Ít Nhất ---")
    await _try_create_and_add_embed(
        embeds_guild.create_umbra_hour_embed, least_activity_embeds, scan_errors,
        server_hourly_activity=server_hourly_activity, channel_hourly_activity=channel_hourly_activity,
        thread_hourly_activity=thread_hourly_activity, guild=server, bot=bot
    )
    if config.ENABLE_REACTION_SCAN:
        await _try_create_and_add_embed(
            embeds_analysis.create_least_filtered_reaction_embed, least_activity_embeds, scan_errors,
            filtered_reaction_counts, bot=bot
        )
    await _try_create_and_add_embed(
        embeds_items.create_least_sticker_usage_embed, least_activity_embeds, scan_errors,
        sticker_usage_counts, bot=bot, guild=server, scan_data=scan_data
    )
    await _try_create_and_add_embed(
        embeds_guild.create_least_channel_activity_embed, least_activity_embeds, scan_errors,
        guild=server, bot=bot, channel_details=channel_details
    )
    await _try_create_and_add_embed(
        embeds_user.create_least_active_users_embed, least_activity_embeds, scan_errors,
        user_activity, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_least_repliers_embed, least_activity_embeds, scan_errors,
        user_reply_counts, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_least_mentioned_users_embed, least_activity_embeds, scan_errors,
        user_mention_received_counts, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_least_mentioning_users_embed, least_activity_embeds, scan_errors,
        user_mention_given_counts, guild=server, bot=bot
    )
    if config.ENABLE_REACTION_SCAN:
        await _try_create_and_add_embed(
            embeds_user.create_least_reaction_givers_embed, least_activity_embeds, scan_errors,
            user_reaction_given_counts, guild=server, bot=bot
        )
        await _try_create_and_add_embed(
            embeds_user.create_least_reaction_received_users_embed, least_activity_embeds, scan_errors,
            user_reaction_received_counts, guild=server, bot=bot
        )
    await _try_create_and_add_embed(
        embeds_user.create_least_custom_emoji_users_embed, least_activity_embeds, scan_errors,
        scan_data, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_least_sticker_users_embed, least_activity_embeds, scan_errors,
        scan_data, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_least_link_posters_embed, least_activity_embeds, scan_errors,
        user_link_counts, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_least_image_posters_embed, least_activity_embeds, scan_errors,
        user_image_counts, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_least_distinct_channel_users_embed, least_activity_embeds, scan_errors,
        scan_data, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_least_activity_span_users_embed, least_activity_embeds, scan_errors,
        user_activity, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_least_thread_creators_embed, least_activity_embeds, scan_errors,
        user_thread_creation_counts, guild=server, bot=bot
    )
    # --- Nhóm 3: Hoạt Động Nhiều Nhất ---
    log.info(f"--- {e('info')} Nhóm 3: Hoạt Động Nhiều Nhất ---")
    await _try_create_and_add_embed(
        embeds_analysis.create_top_content_emoji_embed, most_activity_embeds, scan_errors,
        overall_custom_emoji_content_counts, bot=bot, guild=server
    )
    await _try_create_and_add_embed(
        embeds_guild.create_golden_hour_embed, most_activity_embeds, scan_errors,
        server_hourly_activity=server_hourly_activity, channel_hourly_activity=channel_hourly_activity,
        thread_hourly_activity=thread_hourly_activity, guild=server, bot=bot
    )
    if config.ENABLE_REACTION_SCAN:
        await _try_create_and_add_embed(
            embeds_analysis.create_filtered_reaction_embed, most_activity_embeds, scan_errors,
            filtered_reaction_counts, bot=bot
        )
    await _try_create_and_add_embed(
        embeds_items.create_top_sticker_usage_embed, most_activity_embeds, scan_errors,
        sticker_usage_counts, bot=bot, guild=server, scan_data=scan_data
    )
    await _try_create_and_add_embed(
        embeds_guild.create_channel_activity_embed, most_activity_embeds, scan_errors,
        guild=server, bot=bot, channel_details=channel_details
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_active_users_embed, most_activity_embeds, scan_errors,
        user_activity, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_repliers_embed, most_activity_embeds, scan_errors,
        user_reply_counts, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_mentioned_users_embed, most_activity_embeds, scan_errors,
        user_mention_received_counts, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_mentioning_users_embed, most_activity_embeds, scan_errors,
        user_mention_given_counts, guild=server, bot=bot
    )
    if config.ENABLE_REACTION_SCAN:
        await _try_create_and_add_embed(
            embeds_analysis.create_top_reaction_givers_embed, most_activity_embeds, scan_errors,
            user_reaction_given_counts, user_reaction_emoji_given_counts, scan_data, server, bot
        )
        await _try_create_and_add_embed(
            embeds_user.create_top_reaction_received_users_embed, most_activity_embeds, scan_errors,
            user_reaction_received_counts, guild=server, bot=bot,
            user_emoji_received_counts=user_emoji_received_counts, scan_data=scan_data
        )
    await _try_create_and_add_embed(
        embeds_user.create_top_custom_emoji_users_embed, most_activity_embeds, scan_errors,
        scan_data, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_sticker_users_embed, most_activity_embeds, scan_errors,
        scan_data, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_link_posters_embed, most_activity_embeds, scan_errors,
        user_link_counts, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_image_posters_embed, most_activity_embeds, scan_errors,
        user_image_counts, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_distinct_channel_users_embed, most_activity_embeds, scan_errors,
        scan_data, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_activity_span_users_embed, most_activity_embeds, scan_errors,
        user_activity, guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_thread_creators_embed, most_activity_embeds, scan_errors,
        user_thread_creation_counts, guild=server, bot=bot
    )
    # --- Nhóm 4: BXH Đặc Biệt & Danh Hiệu ---
    log.info(f"--- {e('info')} Nhóm 4: BXH Đặc Biệt & Danh Hiệu ---")
    await _try_create_and_add_embed(
        embeds_items.create_top_inviters_embed, special_embeds, scan_errors,
        scan_data.get("invite_usage_counts", Counter()), guild=server, bot=bot
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_booster_embed, special_embeds, scan_errors,
        boosters, bot, scan_data['scan_end_time']
    )
    await _try_create_and_add_embed(
        embeds_user.create_top_oldest_members_embed, special_embeds, scan_errors,
        oldest_members_data, scan_data=scan_data, guild=server, bot=bot
    )
    await _try_create_and_add_embed( # Hàm này trả về list
        embeds_analysis.create_tracked_role_grant_leaderboards, special_embeds, scan_errors,
        tracked_role_grant_counts, server, bot
    )
    # --- Nhóm 5: Báo cáo Lỗi ---
    log.info(f"--- {e('warning')} Nhóm 5: Báo cáo Lỗi ---")
    await _try_create_and_add_embed(
        embeds_analysis.create_error_embed, error_embeds, scan_errors,
        scan_errors, bot=bot # Truyền scan_errors vào args
    )


    # === GỬI EMBEDS THEO THỨ TỰ VÀO KÊNH ĐÍCH ===
    log.info(f"--- {e('loading')} Đang gửi các embeds vào kênh {report_channel.mention} ---")
    # --- Gửi Nhóm 1 ---
    if summary_embeds: await _send_report_embeds(scan_data, summary_embeds, "Nhóm 1: Tổng Quan Server", report_channel)
    if analysis_embeds: await _send_report_embeds(scan_data, analysis_embeds, "Nhóm 1: Phân Tích Chung", report_channel)

    # --- Gửi tin nhắn và Sticker B (Trước nhóm "ít nhất") ---
    sticker_b = await utils.fetch_sticker_object(config.LEAST_STICKER_ID, bot, server)
    kwargs_least: Dict[str, Any] = {"content": """
**<==============================>**                                   
# Đầu tiên là về những thứ ít nhất Server:                                 
**<==============================>**"""}
    if sticker_b: kwargs_least["stickers"] = [sticker_b]
    try:
        if least_activity_embeds: # Chỉ gửi nếu có embed để gửi sau đó
            await report_channel.send(**kwargs_least)
            report_messages_sent += 1
            await asyncio.sleep(1.0) # Delay nhỏ
    except Exception as send_err:
        log.error(f"Lỗi gửi tin nhắn/sticker B vào kênh báo cáo: {send_err}")
        scan_errors.append(f"Lỗi gửi sticker B: {send_err}")
    scan_data["report_messages_sent"] = report_messages_sent # Cập nhật lại

    # --- Gửi Nhóm 2 ---
    if least_activity_embeds: await _send_report_embeds(scan_data, least_activity_embeds, "Nhóm 2: Hoạt Động Ít Nhất", report_channel)

    # --- Gửi tin nhắn và Sticker C (Trước nhóm "nhiều nhất") ---
    sticker_c = await utils.fetch_sticker_object(config.MOST_STICKER_ID, bot, server)
    kwargs_most: Dict[str, Any] = {"content": """
**<==============================>**                                   
# Tiếp theo là về những thứ nhiều nhất Server:                                 
**<==============================>**"""}
    if sticker_c: kwargs_most["stickers"] = [sticker_c]
    try:
        if most_activity_embeds: # Chỉ gửi nếu có embed để gửi sau đó
            await report_channel.send(**kwargs_most)
            report_messages_sent += 1
            await asyncio.sleep(1.0) # Delay nhỏ
    except Exception as send_err:
        log.error(f"Lỗi gửi tin nhắn/sticker C vào kênh báo cáo: {send_err}")
        scan_errors.append(f"Lỗi gửi sticker C: {send_err}")
    scan_data["report_messages_sent"] = report_messages_sent # Cập nhật lại

    # --- Gửi Nhóm 3 ---
    if most_activity_embeds: await _send_report_embeds(scan_data, most_activity_embeds, "Nhóm 3: Hoạt Động Nhiều Nhất", report_channel)

    # --- Gửi Nhóm 4 ---
    if special_embeds: await _send_report_embeds(scan_data, special_embeds, "Nhóm 4: BXH Đặc Biệt & Danh Hiệu", report_channel)

    # --- Gửi Nhóm 5 (Lỗi) ---
    if error_embeds: await _send_report_embeds(scan_data, error_embeds, "Nhóm 5: Tóm tắt Lỗi", report_channel)
    elif scan_errors: log.error(f"Có {len(scan_errors)} lỗi nhưng không thể tạo embed báo cáo lỗi.")
    else: log.info("Không có lỗi nào được ghi nhận trong quá trình quét.")

    # --- Gửi tin nhắn kết quả lệnh cuối cùng vào kênh BÁO CÁO (B) ---
    # Tính toán thời gian tổng từ scan_data
    total_cmd_duration_td: datetime.timedelta = scan_data.get("overall_duration", datetime.timedelta(0))

    final_result_lines = [
        f"{e('success')} **Đã Hoàn Thành Toàn Bộ Lệnh!**",
        f"{e('clock')} Tổng thời gian lệnh: **{utils.format_timedelta(total_cmd_duration_td, high_precision=True)}**",
        f"{e('stats')} Đã gửi **{report_messages_sent}** tin nhắn báo cáo vào kênh này.",
    ]
    if log_thread:
        # Đề cập đến kênh gốc A để xem log
        final_result_lines.append(f"{e('info')} Xem log chi tiết tại: {log_thread.mention} (trong kênh {ctx.channel.mention})")
    else:
        final_result_lines.append(f"{e('info')} Log chi tiết chỉ có trên Console.")

    # Thêm thông tin về file xuất (nếu có)
    if files_to_send:
        file_tags = []; csv_found = any(f.filename.endswith('.csv') for f in files_to_send); json_found = any(f.filename.endswith('.json') for f in files_to_send)
        if csv_found: file_tags.append(f"{e('csv_file')} CSV")
        if json_found: file_tags.append(f"{e('json_file')} JSON")
        file_tags_str = " / ".join(file_tags) or "file"
        final_result_lines.append(f"📎 Đính kèm **{len(files_to_send)}** {file_tags_str}.")
    elif scan_data["export_csv"] or scan_data["export_json"]:
        final_result_lines.append(f"{e('error')} Yêu cầu xuất file nhưng không thể tạo/gửi (kiểm tra log/lỗi).")

    if scan_errors:
        final_result_lines.append(f"{e('warning')} Lưu ý: Có **{len(scan_errors)}** lỗi/cảnh báo (xem báo cáo lỗi hoặc log).")

    final_command_sticker = await utils.fetch_sticker_object(config.FINAL_STICKER_ID, bot, server)
    final_result_message = "\n".join(final_result_lines)

    try:
        kwargs_final_report: Dict[str, Any] = {
            "content": final_result_message,
            "allowed_mentions": discord.AllowedMentions.none() # Không ping ai
        }
        if files_to_send: kwargs_final_report["files"] = files_to_send
        if final_command_sticker: kwargs_final_report["stickers"] = [final_command_sticker]

        await report_channel.send(**kwargs_final_report)
        log.info(f"{e('success')} Đã gửi tin nhắn kết quả lệnh cuối cùng vào kênh báo cáo #{report_channel.name}.")

    except discord.Forbidden:
        log.error(f"{e('error')} Lỗi gửi tin nhắn/file kết quả lệnh vào kênh báo cáo #{report_channel.name}: Thiếu quyền.")
        # Thử gửi vào kênh gốc nếu kênh báo cáo lỗi
        try:
            await ctx.send(f"{final_result_message}\n\n{e('error')} Lỗi: Không thể gửi vào kênh báo cáo {report_channel_mention}. Gửi tạm vào đây.", files=files_to_send if files_to_send else [], stickers=[final_command_sticker] if final_command_sticker else [], allowed_mentions=discord.AllowedMentions.none())
            log.info(f"Đã gửi tin nhắn kết quả lệnh dự phòng vào kênh gốc #{ctx.channel.name}.")
        except Exception as fallback_err:
             log.error(f"Lỗi gửi tin nhắn kết quả lệnh dự phòng vào kênh gốc: {fallback_err}")
    except discord.HTTPException as e_final:
        log.error(f"{e('error')} Lỗi gửi tin nhắn/file kết quả lệnh vào kênh báo cáo (HTTP {e_final.status}): {e_final.text}", exc_info=True)
        # Thử gửi lại text vào kênh gốc
        try: await ctx.send(f"{final_result_message}\n\n{e('error')} **Lỗi:** Không thể gửi file đính kèm hoặc sticker vào kênh báo cáo.")
        except Exception: log.error("Không thể gửi lại tin nhắn kết quả lệnh sau lỗi HTTP.")
    except Exception as e_final_unkn:
        log.error(f"{e('error')} Lỗi không xác định gửi tin nhắn/file kết quả lệnh: {e_final_unkn}", exc_info=True)
        # Thử gửi lại text vào kênh gốc
        try: await ctx.send(f"{final_result_message}\n\n{e('error')} **Lỗi không xác định khi gửi báo cáo cuối cùng.**")
        except Exception: log.error("Không thể gửi lại tin nhắn kết quả lệnh sau lỗi không xác định.")
    finally:
        # Đóng file handles (QUAN TRỌNG)
        if files_to_send:
            log.debug(f"Đóng {len(files_to_send)} file handles...")
            for f in files_to_send:
                try: f.close()
                except Exception as close_err: log.warning(f"Lỗi đóng file '{f.filename}': {close_err}")
            log.debug("Đóng file handles hoàn tất.")

    # --- Kết thúc ---
    end_time_reports = time.monotonic()
    log.info(f"✅ Hoàn thành tạo và gửi báo cáo embeds công khai vào {report_channel.mention} trong {end_time_reports - start_time_reports:.2f}s.")
    log.info(f"✅✅✅ Hoàn thành toàn bộ lệnh trong {utils.format_timedelta(total_cmd_duration_td, high_precision=True)} ✅✅✅")


# --- END OF FILE cogs/deep_scan_helpers/report_generation.py ---