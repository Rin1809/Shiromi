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
    """Tạo và gửi các báo cáo embeds CÔNG KHAI theo thứ tự mới."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    scan_errors: List[str] = scan_data["scan_errors"]
    ctx: commands.Context = scan_data["ctx"]

    log.info(f"\n--- [bold green]{e('loading')} Đang Tạo Báo Cáo Embeds Công Khai (Thứ Tự Mới)[/bold green] ---")
    start_time_reports = time.monotonic()

    # --- Chuẩn bị dữ liệu cần thiết (lấy từ scan_data) ---
    # (Giữ nguyên phần lấy dữ liệu này)
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
    voice_channel_static_data = scan_data.get("voice_channel_static_data", [])
    user_distinct_channel_counts = scan_data.get("user_distinct_channel_counts", Counter())
    server_hourly_activity = scan_data.get("server_hourly_activity", Counter())
    channel_hourly_activity = scan_data.get("channel_hourly_activity", defaultdict(Counter))
    thread_hourly_activity = scan_data.get("thread_hourly_activity", defaultdict(Counter))
    overall_total_reaction_count = scan_data.get("overall_total_reaction_count", 0)
    overall_filtered_reaction_count = scan_data.get("overall_total_filtered_reaction_count", 0)
    user_emoji_received_counts = scan_data.get("user_emoji_received_counts", defaultdict(Counter)) # <<< LẤY DỮ LIỆU MỚI


    # === KHỐI TẠO VÀ GỬI EMBEDS (THỨ TỰ MỚI) ===

    # --- Helper tạo và thêm embed ---
    all_public_embeds = []
    async def _try_create_and_add_embed(embed_creation_func, target_list, error_list, *args, **kwargs):
        func_name = embed_creation_func.__name__
        try:
            embed = await embed_creation_func(*args, **kwargs)
            if embed:
                target_list.append(embed)
            else:
                log.debug(f"Hàm '{func_name}' không tạo ra embed.")
        except Exception as ex:
            error_msg = f"Lỗi tạo embed '{func_name}': {ex}"
            log.error(f"{e('error')} {error_msg}", exc_info=True)
            error_list.append(error_msg) # Ghi lỗi vào danh sách chung

    log.info(f"--- {e('loading')} Đang tạo embeds công khai theo thứ tự yêu cầu ---")

    # 1. Tổng Quan Server
    await _try_create_and_add_embed(
        embeds_guild.create_summary_embed, all_public_embeds, scan_errors,
        server, bot, scan_data["processed_channels_count"], scan_data["processed_threads_count"],
        scan_data["skipped_channels_count"], scan_data["skipped_threads_count"],
        scan_data["overall_total_message_count"], len(user_activity), scan_data["overall_duration"],
        scan_data["initial_member_status_counts"], scan_data["channel_counts"],
        len(scan_data["all_roles_list"]), scan_data["overall_start_time"],
        scan_data, ctx=ctx, overall_total_reaction_count=overall_filtered_reaction_count
    )

    # 2. Top Booster Bền Bỉ
    await _try_create_and_add_embed(
        embeds_user.create_top_booster_embed, all_public_embeds, scan_errors,
        boosters, bot, scan_data['scan_end_time']
    )

    # 3. Top Emoji Reactions Phổ Biến
    await _try_create_and_add_embed(
        embeds_analysis.create_filtered_reaction_embed, all_public_embeds, scan_errors,
        filtered_reaction_counts, bot=bot
    )

    # 4. Top Stickers Được Dùng
    await _try_create_and_add_embed(
        embeds_items.create_top_sticker_usage_embed, all_public_embeds, scan_errors,
        sticker_usage_counts, bot=bot, guild=server, scan_data=scan_data
    )

    # 5. Hoạt động Kênh
    await _try_create_and_add_embed(
        embeds_guild.create_channel_activity_embed, all_public_embeds, scan_errors,
        guild=server, bot=bot, channel_details=channel_details, voice_channel_static_data=voice_channel_static_data
    )

    # 6. "Giờ Vàng" của Server
    await _try_create_and_add_embed(
        embeds_guild.create_golden_hour_embed, all_public_embeds, scan_errors,
        server_hourly_activity=server_hourly_activity, channel_hourly_activity=channel_hourly_activity,
        thread_hourly_activity=thread_hourly_activity, guild=server, bot=bot
    )

    # 7. Top User Gửi Tin Nhắn
    await _try_create_and_add_embed(
        embeds_user.create_top_active_users_embed, all_public_embeds, scan_errors,
        user_activity, guild=server, bot=bot
    )

    # 8. Trả Lời Tin Nhắn
    await _try_create_and_add_embed(
        embeds_user.create_top_repliers_embed, all_public_embeds, scan_errors,
        user_reply_counts, guild=server, bot=bot
    )

    # 9. Được Nhắc Tên
    await _try_create_and_add_embed(
        embeds_user.create_top_mentioned_users_embed, all_public_embeds, scan_errors,
        user_mention_received_counts, guild=server, bot=bot
    )

    # 10. Hay Nhắc Tên
    await _try_create_and_add_embed(
        embeds_user.create_top_mentioning_users_embed, all_public_embeds, scan_errors,
        user_mention_given_counts, guild=server, bot=bot
    )

    # 11. Top Người Thả Reaction
    await _try_create_and_add_embed(
        embeds_analysis.create_top_reaction_givers_embed, all_public_embeds, scan_errors,
        user_reaction_given_counts, user_reaction_emoji_given_counts, scan_data, server, bot
    )

    # 12. Nhận Reactions
    await _try_create_and_add_embed(
        embeds_user.create_top_reaction_received_users_embed, all_public_embeds, scan_errors,
        user_reaction_received_counts, # Counter tổng reaction nhận
        guild=server,
        bot=bot,
        user_emoji_received_counts=user_emoji_received_counts, # <<< TRUYỀN DỮ LIỆU MỚI
        scan_data=scan_data # <<< TRUYỀN scan_data để lấy emoji cache
    )

    # 13. Top User Dùng Custom Emoji Server
    await _try_create_and_add_embed(
        embeds_user.create_top_custom_emoji_users_embed, all_public_embeds, scan_errors,
        scan_data, guild=server, bot=bot
    )

    # 14. Top User Gửi Sticker
    await _try_create_and_add_embed(
        embeds_user.create_top_sticker_users_embed, all_public_embeds, scan_errors,
        scan_data, guild=server, bot=bot
    )

    # 15. Gửi Link
    await _try_create_and_add_embed(
        embeds_user.create_top_link_posters_embed, all_public_embeds, scan_errors,
        user_link_counts, guild=server, bot=bot
    )

    # 16. Gửi Ảnh (Sửa: Đổi tên param thành user_image_counts)
    await _try_create_and_add_embed(
        embeds_user.create_top_image_posters_embed, all_public_embeds, scan_errors,
        user_image_counts, guild=server, bot=bot
    )

    # 17. Top "Người Đa Năng"
    await _try_create_and_add_embed(
        embeds_user.create_top_distinct_channel_users_embed, all_public_embeds, scan_errors,
        scan_data, guild=server, bot=bot
    )

    # 18. Top Thành Viên Lâu Năm Nhất
    await _try_create_and_add_embed(
        embeds_user.create_top_oldest_members_embed, all_public_embeds, scan_errors,
        oldest_members_data, scan_data=scan_data, guild=server, bot=bot
    )

    # 19. Top User Hoạt Động Lâu Nhất (Span)
    await _try_create_and_add_embed(
        embeds_user.create_top_activity_span_users_embed, all_public_embeds, scan_errors,
        user_activity, guild=server, bot=bot
    )

    # --- Gửi batch embed chính ---
    if all_public_embeds:
        await _send_report_embeds(scan_data, all_public_embeds, "Báo cáo Công khai Tổng hợp")
    else:
        log.warning("Không có embed công khai nào được tạo để gửi.")

    # --- GỬI CÁC EMBED PHỤ (KHÔNG THAY ĐỔI) ---

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
        scan_errors.append(error_msg)

    if group6_embeds:
        await _send_report_embeds(scan_data, group6_embeds, "Nhóm 6: Tóm tắt Lỗi")
    elif scan_errors: log.error(f"Có {len(scan_errors)} lỗi nhưng không thể tạo embed báo cáo lỗi.")
    else: log.info("Không có lỗi nào được ghi nhận trong quá trình quét.")

    # --- Kết thúc ---
    end_time_reports = time.monotonic()
    log.info(f"✅ Hoàn thành tạo và gửi báo cáo embeds công khai trong {end_time_reports - start_time_reports:.2f}s.")

# --- END OF FILE cogs/deep_scan_helpers/report_generation.py ---