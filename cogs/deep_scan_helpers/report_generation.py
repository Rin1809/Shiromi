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

# --- Hàm Helper Tạo Embed Leaderboard Chung (Dùng cho các hàm Counter đơn giản) ---
# <<< SỬA HÀM NÀY >>>
async def _try_add_generic_leaderboard(embed_list, func, counter_data, **kwargs):
    """Helper để gọi hàm tạo embed leaderboard CHUNG, bắt lỗi và thêm vào list."""
    bot_ref = kwargs.get('bot')
    e = lambda name: utils.get_emoji(name, bot_ref) if bot_ref else '❓'
    # Lấy scan_errors_ref ra khỏi kwargs trước khi truyền xuống func
    scan_errors_ref = kwargs.pop('scan_errors_ref', None)
    try:
        # Truyền counter_data và các kwargs còn lại vào hàm gốc
        embed = await func(counter_data, **kwargs) # kwargs ở đây đã không còn scan_errors_ref
        if embed: embed_list.append(embed)
        elif func.__name__ != 'create_error_embed':
            log.debug(f"Hàm '{func.__name__}' không tạo ra embed (dữ liệu rỗng?).")
    except Exception as ex:
        err = f"Lỗi embed {func.__name__}: {ex}"
        log.error(f"{e('error')} {err}", exc_info=True)
        # Ghi lỗi vào scan_errors nếu tham chiếu được truyền vào
        if scan_errors_ref is not None and isinstance(scan_errors_ref, list):
            scan_errors_ref.append(err)
# <<< KẾT THÚC SỬA >>>

# --- Hàm Chính Tạo và Gửi Báo cáo CÔNG KHAI ---
async def generate_and_send_reports(scan_data: Dict[str, Any]):
    """Tạo và gửi các báo cáo embeds CÔNG KHAI."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    # Lấy list scan_errors từ scan_data để cập nhật trực tiếp
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
    # Helper để gọi hàm tạo embed, bắt lỗi và thêm vào list
    async def _try_add_embed(func, *args, **kwargs):
        nonlocal server_channel_embeds
        try:
            embed = await func(*args, **kwargs)
            if embed: server_channel_embeds.append(embed)
            elif func.__name__ != 'create_error_embed':
                log.debug(f"Hàm '{func.__name__}' không tạo ra embed (dữ liệu rỗng?).")
        except Exception as ex: error_msg = f"Lỗi embed {func.__name__}: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    # Gọi các hàm tạo embed cho phần này
    await _try_add_embed(embeds_guild.create_summary_embed, server, bot, scan_data["processed_channels_count"], scan_data["processed_threads_count"], scan_data["skipped_channels_count"], scan_data["skipped_threads_count"], scan_data["overall_total_message_count"], len(user_activity), scan_data["overall_duration"], scan_data["initial_member_status_counts"], scan_data["channel_counts"], len(scan_data["all_roles_list"]), scan_data["overall_start_time"], scan_data, ctx=ctx, overall_total_reaction_count=scan_data.get('overall_total_reaction_count'))
    await _try_add_embed(embeds_guild.create_channel_activity_embed, guild=server, bot=bot, channel_details=channel_details, voice_channel_static_data=voice_channel_static_data)
    await _try_add_embed(embeds_guild.create_golden_hour_embed, server_hourly_activity=server_hourly_activity, channel_hourly_activity=channel_hourly_activity, thread_hourly_activity=thread_hourly_activity, guild=server, bot=bot)

    if server_channel_embeds:
        await _send_report_embeds(scan_data, server_channel_embeds, "Tóm tắt Server & Kênh & Giờ vàng")

    # 2. Bảng Xếp Hạng Hoạt Động & Tương Tác
    log.info(f"--- {e('members')} BXH Hoạt động & Tương tác ---")
    activity_interaction_embeds = []
    kwargs_basic = {"guild": server, "bot": bot, "scan_errors_ref": scan_errors} # Thêm scan_errors_ref

    # Sử dụng helper generic cho các hàm counter đơn giản
    # Helper _try_add_generic_leaderboard sẽ tự động xử lý scan_errors_ref
    await _try_add_generic_leaderboard(activity_interaction_embeds, embeds_user.create_top_active_users_embed, user_activity, **kwargs_basic)
    await _try_add_generic_leaderboard(activity_interaction_embeds, embeds_user.create_top_reaction_received_users_embed, user_reaction_received_counts, **kwargs_basic)
    await _try_add_generic_leaderboard(activity_interaction_embeds, embeds_user.create_top_repliers_embed, user_reply_counts, **kwargs_basic)
    await _try_add_generic_leaderboard(activity_interaction_embeds, embeds_user.create_top_mentioned_users_embed, user_mention_received_counts, **kwargs_basic)
    await _try_add_generic_leaderboard(activity_interaction_embeds, embeds_user.create_top_mentioning_users_embed, user_mention_given_counts, **kwargs_basic)

    # Gọi hàm distinct channel trực tiếp (hàm này không dùng helper generic)
    try:
        embed = await embeds_user.create_top_distinct_channel_users_embed(scan_data, guild=server, bot=bot)
        if embed: activity_interaction_embeds.append(embed)
    except Exception as ex: err = f"Lỗi embed create_top_distinct_channel_users_embed: {ex}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)

    if activity_interaction_embeds:
        await _send_report_embeds(scan_data, activity_interaction_embeds, "BXH Hoạt động & Tương tác")

    # 3. Bảng Xếp Hạng Sáng Tạo Nội Dung
    log.info(f"--- {e('image')} BXH Sáng tạo Nội dung ---")
    content_creation_embeds = []

    # Gọi trực tiếp các hàm đặc biệt (không dùng helper generic)
    try: # reaction embed
        embed = await embeds_analysis.create_filtered_reaction_embed(filtered_reaction_counts, bot=bot)
        if embed: content_creation_embeds.append(embed)
    except Exception as ex: err = f"Lỗi embed create_filtered_reaction_embed: {ex}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)

    try: # sticker usage embed
        embed = await embeds_items.create_top_sticker_usage_embed(sticker_usage_counts, bot=bot, guild=server, scan_data=scan_data)
        if embed: content_creation_embeds.append(embed)
    except Exception as ex: err = f"Lỗi embed create_top_sticker_usage_embed: {ex}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)

    try: # custom emoji users embed
        embed = await embeds_user.create_top_custom_emoji_users_embed(scan_data, guild=server, bot=bot)
        if embed: content_creation_embeds.append(embed)
    except Exception as ex: err = f"Lỗi embed create_top_custom_emoji_users_embed: {ex}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)

    try: # sticker users embed
        embed = await embeds_user.create_top_sticker_users_embed(scan_data, guild=server, bot=bot)
        if embed: content_creation_embeds.append(embed)
    except Exception as ex: err = f"Lỗi embed create_top_sticker_users_embed: {ex}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)

    # Dùng helper generic cho các hàm còn lại
    await _try_add_generic_leaderboard(content_creation_embeds, embeds_user.create_top_link_posters_embed, user_link_counts, **kwargs_basic)
    await _try_add_generic_leaderboard(content_creation_embeds, embeds_user.create_top_image_posters_embed, user_image_counts, **kwargs_basic)
    await _try_add_generic_leaderboard(content_creation_embeds, embeds_user.create_top_thread_creators_embed, user_thread_creation_counts, **kwargs_basic)

    if content_creation_embeds:
        await _send_report_embeds(scan_data, content_creation_embeds, "BXH Sáng tạo Nội dung")

    # 4. Bảng Xếp Hạng Danh Hiệu Đặc Biệt (Gọi trực tiếp)
    log.info(f"--- {e('crown')} BXH Danh hiệu Đặc biệt ---")
    tracked_role_embeds = []
    try:
         temp_tracked_embeds = await embeds_analysis.create_tracked_role_grant_leaderboards(tracked_role_grant_counts, server, bot)
         if temp_tracked_embeds:
              tracked_role_embeds.extend(temp_tracked_embeds)
         elif config.TRACKED_ROLE_GRANT_IDS:
              log.debug(f"Hàm 'create_tracked_role_grant_leaderboards' không tạo ra embed (dữ liệu rỗng?).")
    except Exception as ex: error_msg = f"Lỗi embed BXH danh hiệu: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)
    if tracked_role_embeds:
        await _send_report_embeds(scan_data, tracked_role_embeds, "BXH Danh hiệu Đặc biệt")

    # 5. Bảng Xếp Hạng Thời Gian & Tham Gia
    log.info(f"--- {e('calendar')} BXH Thời gian & Tham gia ---")
    time_join_embeds = []

    # Gọi trực tiếp các hàm trong nhóm này (không dùng helper generic)
    try: # oldest members
        embed = await embeds_user.create_top_oldest_members_embed(
            oldest_members_data,
            scan_data=scan_data,
            guild=server,
            bot=bot
        )
        if embed: time_join_embeds.append(embed)
    except AttributeError as attr_err:
        log.error(f"!!! LỖI AttributeError khi gọi hàm trong embeds_user: {attr_err}", exc_info=True)
        scan_errors.append(f"Lỗi thuộc tính khi gọi embeds_user: {attr_err}")
    except Exception as ex:
        err = f"Lỗi embed create_top_oldest_members_embed: {ex}"
        log.error(f"{e('error')} {err}", exc_info=True)
        scan_errors.append(err)

    try: # activity span
        embed = await embeds_user.create_top_activity_span_users_embed(user_activity, guild=server, bot=bot)
        if embed: time_join_embeds.append(embed)
    except Exception as ex: err = f"Lỗi embed create_top_activity_span_users_embed: {ex}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)

    try: # booster
        embed = await embeds_user.create_top_booster_embed(boosters, bot=bot, scan_end_time=scan_data['scan_end_time'])
        if embed: time_join_embeds.append(embed)
    except Exception as ex: err = f"Lỗi embed create_top_booster_embed: {ex}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)

    if time_join_embeds:
        await _send_report_embeds(scan_data, time_join_embeds, "BXH Thời gian & Tham gia")

    # 6. Báo cáo Tóm tắt Lỗi (Gọi trực tiếp)
    log.info(f"--- {e('warning')} Báo cáo Lỗi ---")
    error_embed_list = []
    try:
        embed = await embeds_analysis.create_error_embed(scan_errors, bot=bot)
        if embed: error_embed_list.append(embed)
    except Exception as ex: error_msg = f"Lỗi embed create_error_embed: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg) # Ghi vào scan_errors

    if error_embed_list:
        await _send_report_embeds(scan_data, error_embed_list, "Tóm tắt Lỗi")
    elif scan_errors:
         log.error(f"Có {len(scan_errors)} lỗi nhưng không thể tạo embed báo cáo lỗi.")
    else:
        log.info("Không có lỗi nào được ghi nhận trong quá trình quét.")

    end_time_reports = time.monotonic()
    log.info(f"✅ Hoàn thành tạo và gửi báo cáo embeds công khai trong {end_time_reports - start_time_reports:.2f}s.")

# --- END OF FILE cogs/deep_scan_helpers/report_generation.py ---