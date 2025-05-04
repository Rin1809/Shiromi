# --- START OF FILE cogs/deep_scan_helpers/report_generation.py ---
# (Các import và hàm _send_report_embeds giữ nguyên)
# ...
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
            await asyncio.sleep(1.5) # Delay nhẹ
        except discord.HTTPException as send_err:
            error_msg = f"Lỗi gửi '{type_name}' (Embed {i+1}, HTTP {send_err.status}): {send_err.text}"
            log.error(f"{e('error')} {error_msg}")
            scan_errors.append(error_msg)
            if send_err.status == 429: retry_after = send_err.retry_after or 5.0; log.warning(f"    Bị rate limit, chờ {retry_after + config.LOG_RETRY_AFTER_BUFFER:.2f}s..."); await asyncio.sleep(retry_after + config.LOG_RETRY_AFTER_BUFFER) # Sử dụng config
            elif send_err.status >= 500: log.warning(f"    Lỗi server Discord ({send_err.status}), chờ 5s..."); await asyncio.sleep(5.0)
            else: log.warning(f"    Lỗi client Discord ({send_err.status}), chờ 3s..."); await asyncio.sleep(3.0)
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
    scan_errors: List[str] = scan_data["scan_errors"]
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

    # --- Trình tự Tạo và Gửi Embeds Công Khai ---

    # 1. Tóm tắt Server & Hoạt động Kênh & Giờ vàng
    log.info(f"--- {e('stats')} Báo cáo Tổng quan & Kênh & Giờ vàng ---")
    server_channel_embeds = []
    async def _try_add_embed(func, *args, **kwargs):
        nonlocal server_channel_embeds # Cho phép sửa list bên ngoài
        try:
            embed = await func(*args, **kwargs)
            if embed: server_channel_embeds.append(embed)
            # <<< ADDED: Log nếu embed không được tạo >>>
            elif func.__name__ != 'create_error_embed': # Không log cho embed lỗi
                log.debug(f"Hàm '{func.__name__}' không tạo ra embed (dữ liệu rỗng?).")
            # <<< END ADDED >>>
        except Exception as ex: error_msg = f"Lỗi embed {func.__name__}: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    # Gọi các hàm tạo embed cho nhóm 1
    await _try_add_embed(embeds_guild.create_summary_embed, server, bot, scan_data["processed_channels_count"], scan_data["processed_threads_count"], scan_data["skipped_channels_count"], scan_data["skipped_threads_count"], scan_data["overall_total_message_count"], len(user_activity), scan_data["overall_duration"], scan_data["initial_member_status_counts"], scan_data["channel_counts"], len(scan_data["all_roles_list"]), scan_data["overall_start_time"], scan_data, ctx, scan_data.get('overall_total_reaction_count'))
    await _try_add_embed(embeds_guild.create_channel_activity_embed, guild=server, bot=bot, channel_details=channel_details, voice_channel_static_data=voice_channel_static_data)
    await _try_add_embed(embeds_guild.create_golden_hour_embed, server_hourly_activity=server_hourly_activity, channel_hourly_activity=channel_hourly_activity, thread_hourly_activity=thread_hourly_activity, guild=server, bot=bot)

    if server_channel_embeds:
        await _send_report_embeds(scan_data, server_channel_embeds, "Tóm tắt Server & Kênh & Giờ vàng")

    # 2. Bảng Xếp Hạng Hoạt Động & Tương Tác
    log.info(f"--- {e('members')} BXH Hoạt động & Tương tác ---")
    activity_interaction_embeds = []
    # Hàm helper _try_add_leaderboard để dùng lại cho các BXH khác
    async def _try_add_leaderboard(embed_list, func, counter_or_data, *args, **kwargs):
        try:
            embed = await func(counter_or_data, *args, **kwargs)
            if embed: embed_list.append(embed)
            # <<< ADDED: Log nếu embed không được tạo >>>
            elif func.__name__ != 'create_error_embed': # Không log cho embed lỗi
                log.debug(f"Hàm '{func.__name__}' không tạo ra embed (dữ liệu rỗng?).")
            # <<< END ADDED >>>
        except Exception as ex: err = f"Lỗi embed {func.__name__}: {ex}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)

    # Gọi các hàm tạo embed cho nhóm 2
    await _try_add_leaderboard(activity_interaction_embeds, embeds_user.create_top_active_users_embed, user_activity, guild=server, bot=bot)
    await _try_add_leaderboard(activity_interaction_embeds, embeds_user.create_top_reaction_received_users_embed, user_reaction_received_counts, guild=server, bot=bot)
    await _try_add_leaderboard(activity_interaction_embeds, embeds_user.create_top_repliers_embed, user_reply_counts, guild=server, bot=bot)
    await _try_add_leaderboard(activity_interaction_embeds, embeds_user.create_top_mentioned_users_embed, user_mention_received_counts, guild=server, bot=bot)
    await _try_add_leaderboard(activity_interaction_embeds, embeds_user.create_top_mentioning_users_embed, user_mention_given_counts, guild=server, bot=bot)
    await _try_add_leaderboard(activity_interaction_embeds, embeds_user.create_top_distinct_channel_users_embed, user_distinct_channel_counts, guild=server, bot=bot)
    if activity_interaction_embeds:
        await _send_report_embeds(scan_data, activity_interaction_embeds, "BXH Hoạt động & Tương tác")

    # 3. Bảng Xếp Hạng Sáng Tạo Nội Dung
    log.info(f"--- {e('image')} BXH Sáng tạo Nội dung ---")
    content_creation_embeds = []
    await _try_add_leaderboard(content_creation_embeds, embeds_analysis.create_filtered_reaction_embed, filtered_reaction_counts, bot=bot)
    # <<< FIX: Truyền scan_data vào create_top_sticker_usage_embed >>>
    await _try_add_leaderboard(content_creation_embeds, embeds_items.create_top_sticker_usage_embed, sticker_usage_counts, bot=bot, guild=server, scan_data=scan_data)
    # <<< END FIX >>>
    await _try_add_leaderboard(content_creation_embeds, embeds_user.create_top_custom_emoji_users_embed, scan_data.get("user_total_custom_emoji_content_counts", Counter()), guild=server, bot=bot) # Đã sửa để nhận counter tổng
    await _try_add_leaderboard(content_creation_embeds, embeds_user.create_top_sticker_users_embed, user_sticker_counts, guild=server, bot=bot)
    await _try_add_leaderboard(content_creation_embeds, embeds_user.create_top_link_posters_embed, user_link_counts, guild=server, bot=bot)
    await _try_add_leaderboard(content_creation_embeds, embeds_user.create_top_image_posters_embed, user_image_counts, guild=server, bot=bot)
    await _try_add_leaderboard(content_creation_embeds, embeds_user.create_top_thread_creators_embed, user_thread_creation_counts, guild=server, bot=bot)
    if content_creation_embeds:
        await _send_report_embeds(scan_data, content_creation_embeds, "BXH Sáng tạo Nội dung")

    # 4. Bảng Xếp Hạng Danh Hiệu Đặc Biệt
    log.info(f"--- {e('crown')} BXH Danh hiệu Đặc biệt ---")
    tracked_role_embeds = [] # Khởi tạo list riêng
    # Gọi trực tiếp hàm tạo embed danh hiệu (vì nó trả về list)
    try:
         temp_tracked_embeds = await embeds_analysis.create_tracked_role_grant_leaderboards(tracked_role_grant_counts, server, bot)
         if temp_tracked_embeds:
              tracked_role_embeds.extend(temp_tracked_embeds) # Thêm vào list
         # <<< ADDED: Log nếu embed không được tạo >>>
         elif config.TRACKED_ROLE_GRANT_IDS: # Chỉ log nếu có cấu hình role
              log.debug(f"Hàm 'create_tracked_role_grant_leaderboards' không tạo ra embed (dữ liệu rỗng?).")
         # <<< END ADDED >>>
    except Exception as ex: error_msg = f"Lỗi embed BXH danh hiệu: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)
    # Gửi list embed danh hiệu nếu có
    if tracked_role_embeds:
        await _send_report_embeds(scan_data, tracked_role_embeds, "BXH Danh hiệu Đặc biệt")

    # 5. Bảng Xếp Hạng Thời Gian & Tham Gia
    log.info(f"--- {e('calendar')} BXH Thời gian & Tham gia ---")
    time_join_embeds = []
    await _try_add_leaderboard(time_join_embeds, embeds_user.create_top_oldest_members_embed, oldest_members_data, bot=bot)
    await _try_add_leaderboard(time_join_embeds, embeds_user.create_top_activity_span_users_embed, user_activity, guild=server, bot=bot)
    await _try_add_leaderboard(time_join_embeds, embeds_user.create_top_booster_embed, boosters, bot=bot, scan_end_time=scan_data['scan_end_time'])
    if time_join_embeds:
        await _send_report_embeds(scan_data, time_join_embeds, "BXH Thời gian & Tham gia")

    # 6. Báo cáo Tóm tắt Lỗi
    log.info(f"--- {e('warning')} Báo cáo Lỗi ---")
    error_embed_list = [] # Tạo list riêng
    await _try_add_embed(embeds_analysis.create_error_embed, scan_errors, bot) # Gọi helper để thêm vào error_embed_list
    if error_embed_list: # Chỉ gửi nếu embed lỗi được tạo
        await _send_report_embeds(scan_data, error_embed_list, "Tóm tắt Lỗi")
    elif scan_errors: # Log nếu có lỗi nhưng embed không tạo được
         log.error(f"Có {len(scan_errors)} lỗi nhưng không thể tạo embed báo cáo lỗi.")
         # Cân nhắc gửi lỗi thô nếu cần
    else:
        log.info("Không có lỗi nào được ghi nhận trong quá trình quét.")


    end_time_reports = time.monotonic()
    log.info(f"✅ Hoàn thành tạo và gửi báo cáo embeds công khai trong {end_time_reports - start_time_reports:.2f}s.")

# --- END OF FILE cogs/deep_scan_helpers/report_generation.py ---