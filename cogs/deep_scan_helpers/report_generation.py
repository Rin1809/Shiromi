# --- START OF FILE cogs/deep_scan_helpers/report_generation.py ---
import discord
from discord.ext import commands
import logging
import asyncio
from typing import Dict, Any, List, Optional
import time
from collections import Counter, defaultdict

import config
import utils
import database

# Import các module tạo embeds
from reporting import embeds_guild, embeds_user, embeds_items, embeds_analysis, embeds_dm

log = logging.getLogger(__name__)

# --- Hàm Gửi Embed Helper ---
async def _send_report_embeds(scan_data: Dict[str, Any], embed_list: List[discord.Embed], type_name: str):
    """Gửi một danh sách embeds vào kênh context, xử lý rate limit."""
    ctx: commands.Context = scan_data["ctx"]
    scan_errors: List[str] = scan_data["scan_errors"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    report_messages_sent = scan_data.setdefault("report_messages_sent", 0)

    if not embed_list:
        log.info(f"{e('info')} Không có dữ liệu để tạo báo cáo '{type_name}', bỏ qua.")
        return

    log.info(f"{e('loading')} Đang gửi {len(embed_list)} embed(s) cho báo cáo '{type_name}'...")
    for i, embed in enumerate(embed_list):
        if not isinstance(embed, discord.Embed):
             log.warning(f"Bỏ qua gửi mục không phải Embed trong '{type_name}': {type(embed)}")
             continue
        try:
            await ctx.send(embed=embed)
            report_messages_sent += 1
            log.debug(f"  Đã gửi embed {i+1}/{len(embed_list)} cho '{type_name}'.")
            await asyncio.sleep(1.6) # Delay nhẹ giữa các tin nhắn
        except discord.HTTPException as send_err:
            error_msg = f"Lỗi gửi '{type_name}' (Embed {i+1}, HTTP {send_err.status}): {send_err.text}"
            log.error(f"{e('error')} {error_msg}")
            scan_errors.append(error_msg) # Ghi lỗi vào list cục bộ
            if send_err.status == 429:
                retry_after = send_err.retry_after or 5.0
                wait_time = retry_after + config.LOG_RETRY_AFTER_BUFFER
                log.warning(f"    Bị rate limit, chờ {wait_time:.2f}s...")
                await asyncio.sleep(wait_time)
            elif send_err.status >= 500:
                log.warning(f"    Lỗi server Discord ({send_err.status}), chờ 5s...")
                await asyncio.sleep(5.0)
            else: # Các lỗi client khác
                log.warning(f"    Lỗi client Discord ({send_err.status}), chờ 3s...")
                await asyncio.sleep(3.0)
        except Exception as send_e:
            error_msg = f"Lỗi không xác định gửi '{type_name}' (Embed {i+1}): {send_e}"
            log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg); await asyncio.sleep(2.0)

    scan_data["report_messages_sent"] = report_messages_sent

# --- Hàm Chính Tạo và Gửi Báo cáo CÔNG KHAI ---
async def generate_and_send_reports(scan_data: Dict[str, Any]):
    """Tạo và gửi các báo cáo embeds CÔNG KHAI."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    scan_errors: List[str] = scan_data["scan_errors"] # scan_errors vẫn ở đây
    ctx: commands.Context = scan_data["ctx"]

    log.info(f"\n--- [bold green]{e('loading')} Đang Tạo Báo Cáo Embeds Công Khai[/bold green] ---")
    start_time_reports = time.monotonic()

    # --- Chuẩn bị dữ liệu cần thiết (lấy từ scan_data) ---
    user_activity = scan_data["user_activity"]
    user_link_counts = scan_data.get("user_link_counts", Counter())
    user_image_counts = scan_data.get("user_image_counts", Counter())
    user_total_custom_emoji_content_counts = scan_data.get("user_total_custom_emoji_content_counts", Counter())
    user_sticker_counts = scan_data.get("user_sticker_counts", Counter())
    user_mention_received_counts = scan_data.get("user_mention_received_counts", Counter())
    user_mention_given_counts = scan_data.get("user_mention_given_counts", Counter())
    user_reply_counts = scan_data.get("user_reply_counts", Counter())
    user_reaction_received_counts = scan_data.get("user_reaction_received_counts", Counter())
    user_reaction_given_counts = scan_data.get("user_reaction_given_counts", Counter()) # Thêm mới
    user_reaction_emoji_given_counts = scan_data.get("user_reaction_emoji_given_counts", defaultdict(Counter)) # Thêm mới
    filtered_reaction_counts = scan_data.get("filtered_reaction_emoji_counts", Counter())
    sticker_usage_counts = scan_data.get("sticker_usage_counts", Counter())
    user_thread_creation_counts = scan_data.get("user_thread_creation_counts", Counter())
    oldest_members_data = scan_data.get("oldest_members_data", [])
    boosters = scan_data.get("boosters", [])
    tracked_role_grant_counts = scan_data.get("tracked_role_grant_counts", Counter())
    channel_details = scan_data.get("channel_details", [])
    voice_channel_static_data = scan_data.get("voice_channel_static_data", [])
    user_distinct_channel_counts = scan_data.get("user_distinct_channel_counts", Counter())
    server_hourly_activity = scan_data.get("server_hourly_activity", Counter())
    channel_hourly_activity = scan_data.get("channel_hourly_activity", defaultdict(Counter))
    thread_hourly_activity = scan_data.get("thread_hourly_activity", defaultdict(Counter))
    overall_total_reaction_count = scan_data.get("overall_total_reaction_count", 0) # Tổng thô
    overall_filtered_reaction_count = scan_data.get("overall_total_filtered_reaction_count", 0) # Tổng lọc


    # === KHỐI TẠO VÀ GỬI EMBEDS (ĐÃ SẮP XẾP LẠI) ===

    # --- Nhóm 1: Tổng quan Server & Kênh & Giờ Vàng ---
    log.info(f"--- {e('stats')} Nhóm 1: Tổng quan Server & Kênh & Giờ vàng ---")
    group1_embeds = []
    async def _try_add_embed_group1(func, *args, **kwargs):
        nonlocal group1_embeds
        try:
            embed = await func(*args, **kwargs)
            if embed: group1_embeds.append(embed)
            elif func.__name__ != 'create_error_embed': log.debug(f"Hàm '{func.__name__}' (Nhóm 1) không tạo ra embed.")
        except Exception as ex:
            error_msg = f"Lỗi embed {func.__name__} (Nhóm 1): {ex}"
            log.error(f"{e('error')} {error_msg}", exc_info=True)
            # Ghi lỗi vào scan_errors (truy cập qua closure)
            scan_errors.append(error_msg)

    await _try_add_embed_group1(embeds_guild.create_summary_embed, server, bot, scan_data["processed_channels_count"], scan_data["processed_threads_count"], scan_data["skipped_channels_count"], scan_data["skipped_threads_count"], scan_data["overall_total_message_count"], len(user_activity), scan_data["overall_duration"], scan_data["initial_member_status_counts"], scan_data["channel_counts"], len(scan_data["all_roles_list"]), scan_data["overall_start_time"], scan_data, ctx=ctx, overall_total_reaction_count=overall_filtered_reaction_count)
    await _try_add_embed_group1(embeds_guild.create_channel_activity_embed, guild=server, bot=bot, channel_details=channel_details, voice_channel_static_data=voice_channel_static_data)
    await _try_add_embed_group1(embeds_guild.create_golden_hour_embed, server_hourly_activity=server_hourly_activity, channel_hourly_activity=channel_hourly_activity, thread_hourly_activity=thread_hourly_activity, guild=server, bot=bot)


    if group1_embeds:
        await _send_report_embeds(scan_data, group1_embeds, "Nhóm 1: Tổng quan & Kênh & Giờ vàng")


    # --- Nhóm 2: BXH Người dùng - Hoạt động Cốt lõi ---
    log.info(f"--- {e('members')} Nhóm 2: BXH Hoạt động Cốt lõi ---")
    group2_embeds = []
    # SỬA: Xóa scan_errors_ref khỏi kwargs
    kwargs_basic_g2 = {"guild": server, "bot": bot}

    async def _try_add_embed_group2(func, *args, **kwargs): # Helper riêng cho nhóm này
        nonlocal group2_embeds
        try:
            # SỬA: Không cần pop scan_errors_ref nữa
            embed = await func(*args, **kwargs)
            if embed: group2_embeds.append(embed)
            elif func.__name__ != 'create_error_embed': log.debug(f"Hàm '{func.__name__}' (Nhóm 2) không tạo ra embed.")
        except Exception as ex:
            error_msg = f"Lỗi embed {func.__name__} (Nhóm 2): {ex}"
            log.error(f"{e('error')} {error_msg}", exc_info=True)
            # Ghi lỗi vào scan_errors (truy cập qua closure)
            scan_errors.append(error_msg)

    # Thứ tự ưu tiên: Tin nhắn -> Trả lời -> Reaction (Nhận/Gửi) -> Mention (Nhận/Gửi) -> Đa kênh
    await _try_add_embed_group2(embeds_user.create_top_active_users_embed, user_activity, **kwargs_basic_g2)
    await _try_add_embed_group2(embeds_user.create_top_repliers_embed, user_reply_counts, **kwargs_basic_g2)
    await _try_add_embed_group2(embeds_user.create_top_reaction_received_users_embed, user_reaction_received_counts, **kwargs_basic_g2)
    try: # BXH người thả reaction
        embed_rg = await embeds_analysis.create_top_reaction_givers_embed(user_reaction_given_counts, user_reaction_emoji_given_counts, scan_data, server, bot)
        if embed_rg: group2_embeds.append(embed_rg)
    except Exception as ex_rg: err = f"Lỗi embed create_top_reaction_givers_embed (Nhóm 2): {ex_rg}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)
    await _try_add_embed_group2(embeds_user.create_top_mentioned_users_embed, user_mention_received_counts, **kwargs_basic_g2)
    await _try_add_embed_group2(embeds_user.create_top_mentioning_users_embed, user_mention_given_counts, **kwargs_basic_g2)
    try: # BXH đa kênh
        embed_dc = await embeds_user.create_top_distinct_channel_users_embed(scan_data, guild=server, bot=bot)
        if embed_dc: group2_embeds.append(embed_dc)
    except Exception as ex_dc: err = f"Lỗi embed create_top_distinct_channel_users_embed (Nhóm 2): {ex_dc}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)


    if group2_embeds:
        await _send_report_embeds(scan_data, group2_embeds, "Nhóm 2: BXH Hoạt động Cốt lõi")


    # --- Nhóm 3: BXH Người dùng - Sử dụng Nội dung & Vật phẩm ---
    log.info(f"--- {e('image')} Nhóm 3: BXH Nội dung & Vật phẩm ---")
    group3_embeds = []
    # SỬA: Xóa scan_errors_ref khỏi kwargs
    kwargs_basic_g3 = {"guild": server, "bot": bot}

    async def _try_add_embed_group3(func, *args, **kwargs): # Helper riêng cho nhóm này
        nonlocal group3_embeds
        try:
            # SỬA: Không cần pop scan_errors_ref nữa
            embed = await func(*args, **kwargs)
            if embed: group3_embeds.append(embed)
            elif func.__name__ != 'create_error_embed': log.debug(f"Hàm '{func.__name__}' (Nhóm 3) không tạo ra embed.")
        except Exception as ex:
            error_msg = f"Lỗi embed {func.__name__} (Nhóm 3): {ex}"
            log.error(f"{e('error')} {error_msg}", exc_info=True)
            # Ghi lỗi vào scan_errors (truy cập qua closure)
            scan_errors.append(error_msg)

    # Thứ tự: Link/Ảnh -> Reaction phổ biến -> User dùng Emoji/Sticker -> User tạo thread
    await _try_add_embed_group3(embeds_user.create_top_link_posters_embed, user_link_counts, **kwargs_basic_g3)
    await _try_add_embed_group3(embeds_user.create_top_image_posters_embed, user_image_counts, **kwargs_basic_g3)
    try: # Top Emoji Reactions đã lọc
        embed_fr = await embeds_analysis.create_filtered_reaction_embed(filtered_reaction_counts, bot=bot)
        if embed_fr: group3_embeds.append(embed_fr)
    except Exception as ex_fr: err = f"Lỗi embed create_filtered_reaction_embed (Nhóm 3): {ex_fr}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)
    try: # Top User dùng Custom Emoji (Content)
        embed_ce = await embeds_user.create_top_custom_emoji_users_embed(scan_data, guild=server, bot=bot)
        if embed_ce: group3_embeds.append(embed_ce)
    except Exception as ex_ce: err = f"Lỗi embed create_top_custom_emoji_users_embed (Nhóm 3): {ex_ce}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)
    try: # Top Sticker Usage
        embed_su = await embeds_items.create_top_sticker_usage_embed(sticker_usage_counts, bot=bot, guild=server, scan_data=scan_data)
        if embed_su: group3_embeds.append(embed_su)
    except Exception as ex_su: err = f"Lỗi embed create_top_sticker_usage_embed (Nhóm 3): {ex_su}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)
    try: # Top User gửi Sticker
        embed_ss = await embeds_user.create_top_sticker_users_embed(scan_data, guild=server, bot=bot)
        if embed_ss: group3_embeds.append(embed_ss)
    except Exception as ex_ss: err = f"Lỗi embed create_top_sticker_users_embed (Nhóm 3): {ex_ss}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)
    await _try_add_embed_group3(embeds_user.create_top_thread_creators_embed, user_thread_creation_counts, **kwargs_basic_g3)


    if group3_embeds:
        await _send_report_embeds(scan_data, group3_embeds, "Nhóm 3: BXH Nội dung & Vật phẩm")


    # --- Nhóm 4: BXH Thời gian & Gắn bó ---
    log.info(f"--- {e('calendar')} Nhóm 4: BXH Thời gian & Gắn bó ---")
    group4_embeds = []

    async def _try_add_embed_group4(func, *args, **kwargs): # Helper riêng
        nonlocal group4_embeds
        try:
            embed = await func(*args, **kwargs)
            if embed: group4_embeds.append(embed)
            elif func.__name__ != 'create_error_embed': log.debug(f"Hàm '{func.__name__}' (Nhóm 4) không tạo ra embed.")
        except Exception as ex:
            error_msg = f"Lỗi embed {func.__name__} (Nhóm 4): {ex}"
            log.error(f"{e('error')} {error_msg}", exc_info=True)
            # Ghi lỗi vào scan_errors (truy cập qua closure)
            scan_errors.append(error_msg)

    # Thứ tự: Lâu năm -> Span -> Booster
    try: # Oldest members
        embed_om = await embeds_user.create_top_oldest_members_embed(oldest_members_data, scan_data=scan_data, guild=server, bot=bot)
        if embed_om: group4_embeds.append(embed_om)
    except AttributeError as attr_err: log.error(f"!!! LỖI AttributeError khi gọi hàm embeds_user (Nhóm 4): {attr_err}", exc_info=True); scan_errors.append(f"Lỗi thuộc tính khi gọi embeds_user: {attr_err}")
    except Exception as ex_om: err = f"Lỗi embed create_top_oldest_members_embed (Nhóm 4): {ex_om}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)
    try: # Activity span
        embed_as = await embeds_user.create_top_activity_span_users_embed(user_activity, guild=server, bot=bot)
        if embed_as: group4_embeds.append(embed_as)
    except Exception as ex_as: err = f"Lỗi embed create_top_activity_span_users_embed (Nhóm 4): {ex_as}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)
    try: # Booster
        embed_b = await embeds_user.create_top_booster_embed(boosters, bot=bot, scan_end_time=scan_data['scan_end_time'])
        if embed_b: group4_embeds.append(embed_b)
    except Exception as ex_b: err = f"Lỗi embed create_top_booster_embed (Nhóm 4): {ex_b}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)


    if group4_embeds:
        await _send_report_embeds(scan_data, group4_embeds, "Nhóm 4: BXH Thời gian & Gắn bó")


    # --- Nhóm 5: BXH Danh hiệu Đặc biệt ---
    log.info(f"--- {e('crown')} Nhóm 5: BXH Danh hiệu Đặc biệt ---")
    group5_embeds = []
    try:
         temp_tracked_embeds = await embeds_analysis.create_tracked_role_grant_leaderboards(tracked_role_grant_counts, server, bot)
         if temp_tracked_embeds: group5_embeds.extend(temp_tracked_embeds)
         elif config.TRACKED_ROLE_GRANT_IDS: log.debug("Hàm 'create_tracked_role_grant_leaderboards' (Nhóm 5) không tạo ra embed.")
    except Exception as ex_trg:
        error_msg = f"Lỗi embed BXH danh hiệu (Nhóm 5): {ex_trg}"
        log.error(f"{e('error')} {error_msg}", exc_info=True)
        # Ghi lỗi vào scan_errors (truy cập qua closure)
        scan_errors.append(error_msg)
    if group5_embeds:
        await _send_report_embeds(scan_data, group5_embeds, "Nhóm 5: BXH Danh hiệu Đặc biệt")


    # --- Nhóm 6: Báo cáo Tóm tắt Lỗi ---
    log.info(f"--- {e('warning')} Nhóm 6: Báo cáo Lỗi ---")
    group6_embeds = []
    try:
        embed_err = await embeds_analysis.create_error_embed(scan_errors, bot=bot)
        if embed_err: group6_embeds.append(embed_err)
    except Exception as ex_err:
        error_msg = f"Lỗi embed create_error_embed (Nhóm 6): {ex_err}"
        log.error(f"{e('error')} {error_msg}", exc_info=True)
        # Ghi lỗi vào scan_errors (truy cập qua closure)
        scan_errors.append(error_msg)

    if group6_embeds:
        await _send_report_embeds(scan_data, group6_embeds, "Nhóm 6: Tóm tắt Lỗi")
    elif scan_errors: log.error(f"Có {len(scan_errors)} lỗi nhưng không thể tạo embed báo cáo lỗi.")
    else: log.info("Không có lỗi nào được ghi nhận trong quá trình quét.")

    # --- Kết thúc ---
    end_time_reports = time.monotonic()
    log.info(f"✅ Hoàn thành tạo và gửi báo cáo embeds công khai trong {end_time_reports - start_time_reports:.2f}s.")

# --- END OF FILE cogs/deep_scan_helpers/report_generation.py ---