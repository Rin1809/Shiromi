# --- START OF FILE cogs/deep_scan_helpers/report_generation.py ---
import discord
from discord.ext import commands
import logging
import asyncio
from typing import Dict, Any, List, Optional
import time
import config
import utils
import database # Cần DB để lấy audit log cho báo cáo

# Import các module tạo embeds
from reporting import embeds_guild, embeds_user, embeds_items, embeds_analysis

log = logging.getLogger(__name__)

# --- Hàm Gửi Embed Helper ---
async def _send_report_embeds(scan_data: Dict[str, Any], embed_list: List[discord.Embed], type_name: str):
    """Gửi một danh sách embeds vào kênh context, xử lý rate limit."""
    ctx: commands.Context = scan_data["ctx"]
    scan_errors: List[str] = scan_data["scan_errors"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    report_messages_sent = scan_data.setdefault("report_messages_sent", 0) # Khởi tạo nếu chưa có

    if not embed_list:
        log.info(f"{e('info')} Không có dữ liệu để tạo báo cáo '{type_name}', bỏ qua.")
        return

    log.info(f"{e('loading')} Đang gửi {len(embed_list)} embed(s) cho báo cáo '{type_name}'...")
    for i, embed in enumerate(embed_list):
        try:
            await ctx.send(embed=embed)
            report_messages_sent += 1
            log.debug(f"  Đã gửi embed {i+1}/{len(embed_list)} cho '{type_name}'.")
            await asyncio.sleep(1.5) # Delay nhẹ giữa các embed để tránh rate limit và flood
        except discord.HTTPException as send_err:
            error_msg = f"Lỗi gửi '{type_name}' (Embed {i+1}, HTTP {send_err.status}): {send_err.text}"
            log.error(f"{e('error')} {error_msg}")
            scan_errors.append(error_msg)
            # Xử lý rate limit
            if send_err.status == 429:
                retry_after = send_err.retry_after or 5.0
                log.warning(f"    Bị rate limit, chờ {retry_after:.2f}s...")
                await asyncio.sleep(retry_after + 0.5)
            # Xử lý lỗi server Discord
            elif send_err.status >= 500:
                 log.warning(f"    Lỗi server Discord ({send_err.status}), chờ 5s...")
                 await asyncio.sleep(5.0)
            # Các lỗi client khác (vd: 403 Forbidden)
            else:
                log.warning(f"    Lỗi client Discord ({send_err.status}), chờ 3s...")
                await asyncio.sleep(3.0)
        except Exception as send_e:
            error_msg = f"Lỗi không xác định gửi '{type_name}' (Embed {i+1}): {send_e}"
            log.error(f"{e('error')} {error_msg}", exc_info=True)
            scan_errors.append(error_msg)
            await asyncio.sleep(2.0) # Chờ nếu có lỗi lạ

    scan_data["report_messages_sent"] = report_messages_sent # Cập nhật lại số lượng


# --- Hàm Chính Tạo và Gửi Báo cáo ---
async def generate_and_send_reports(scan_data: Dict[str, Any]):
    """Tạo và gửi tất cả các báo cáo embeds."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    scan_errors: List[str] = scan_data["scan_errors"]

    log.info(f"\n--- [bold green]{e('loading')} Đang Tạo Tất Cả Báo Cáo Embeds[/bold green] ---")
    start_time_reports = time.monotonic()

    # --- Chuẩn bị dữ liệu cần thiết cho embeds ---
    # (Lấy dữ liệu đã được xử lý từ scan_data)
    server_info_for_report = {
        'member_count_real': len([m for m in scan_data["current_members_list"] if not m.bot]),
        'bot_count': len([m for m in scan_data["current_members_list"] if m.bot]),
        'text_channel_count': scan_data["channel_counts"].get(discord.ChannelType.text, 0),
        'voice_channel_count': scan_data["channel_counts"].get(discord.ChannelType.voice, 0),
        'category_count': scan_data["channel_counts"].get(discord.ChannelType.category, 0),
        'stage_count': scan_data["channel_counts"].get(discord.ChannelType.stage_voice, 0),
        'forum_count': scan_data["channel_counts"].get(discord.ChannelType.forum, 0),
        'reaction_count_overall': scan_data.get('overall_total_reaction_count') if scan_data.get("can_scan_reactions") else None
    }
    channel_details = scan_data["channel_details"]
    voice_channel_static_data = scan_data["voice_channel_static_data"]
    all_roles_list = scan_data["all_roles_list"]
    boosters = scan_data["boosters"]
    user_activity = scan_data["user_activity"]
    oldest_members_data = scan_data["oldest_members_data"]
    # Counters
    user_link_counts = scan_data["user_link_counts"]
    user_image_counts = scan_data["user_image_counts"]
    user_emoji_counts = scan_data["user_emoji_counts"]
    user_sticker_counts = scan_data["user_sticker_counts"]
    user_mention_received_counts = scan_data["user_mention_received_counts"]
    user_mention_given_counts = scan_data["user_mention_given_counts"]
    user_reply_counts = scan_data["user_reply_counts"]
    user_reaction_received_counts = scan_data["user_reaction_received_counts"]
    reaction_emoji_counts = scan_data["reaction_emoji_counts"]
    sticker_usage_counts = scan_data["sticker_usage_counts"]
    user_thread_creation_counts = scan_data["user_thread_creation_counts"]
    # Invites/Webhooks/Integrations
    invites_data = scan_data["invites_data"]
    invite_usage_counts = scan_data["invite_usage_counts"]
    webhooks_data = scan_data["webhooks_data"]
    integrations_data = scan_data["integrations_data"]
    # Moderation/Admin data
    permission_audit_results = scan_data["permission_audit_results"]
    role_change_stats = scan_data["role_change_stats"]
    user_role_changes = scan_data["user_role_changes"]
    # Keywords
    target_keywords = scan_data["target_keywords"]
    keyword_counts = scan_data["keyword_counts"]
    channel_keyword_counts = scan_data["channel_keyword_counts"]
    thread_keyword_counts = scan_data["thread_keyword_counts"]
    user_keyword_counts = scan_data["user_keyword_counts"]
    # Audit Log (lấy từ DB cho báo cáo)
    audit_logs_for_report_cached = []
    if scan_data.get("can_scan_audit_log"):
        try:
            log.debug("Fetching audit logs từ DB cho báo cáo (limit 150)...")
            audit_logs_for_report_cached = await database.get_audit_logs_for_report(server.id, limit=150)
            log.debug(f"Fetched {len(audit_logs_for_report_cached)} audit logs.")
        except Exception as ex:
            error_msg = f"Lỗi fetch audit logs cho báo cáo: {ex}"
            log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    # --- Trình tự Tạo và Gửi Embeds ---

    # 1. Tóm tắt Server & Chi tiết Kênh (Text/Voice/Static)
    log.info(f"--- {e('stats')} Báo cáo Tổng quan & Kênh ---")
    try:
        summary_embed = await embeds_guild.create_summary_embed(
            server, bot, scan_data["processed_channels_count"], scan_data["processed_threads_count"],
            len(channel_details) - scan_data["processed_channels_count"], # Tính lại skipped
            scan_data["skipped_threads_count"], scan_data["overall_total_message_count"],
            len(user_activity), scan_data["overall_duration"],
            scan_data["initial_member_status_counts"], scan_data["channel_counts"],
            len(all_roles_list), scan_data["overall_start_time"], scan_data["ctx"],
            server_info_for_report['reaction_count_overall']
        )
        await _send_report_embeds(scan_data, [summary_embed], "Tóm tắt Server")
    except Exception as ex: error_msg = f"Lỗi embed tóm tắt: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    # Chi tiết Kênh Text/Voice đã quét
    channel_embeds = []
    processed_channel_details = [d for d in channel_details if d.get("processed")]
    log.info(f"Đang tạo embeds cho {len(processed_channel_details)} chi tiết kênh text/voice...")
    for detail in processed_channel_details:
         try:
             embed = await embeds_guild.create_text_channel_embed(detail, bot)
             channel_embeds.append(embed)
         except Exception as ex:
             error_msg = f"Lỗi embed kênh #{detail.get('name', 'N/A')} ({detail.get('id')}): {ex}"
             log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)
             # Tạo embed lỗi thay thế
             error_embed = discord.Embed(
                 title=f"{e('error')} Lỗi báo cáo kênh #{detail.get('name', 'N/A')}",
                 description=f"```\n{utils.escape_markdown(error_msg)}\n```",
                 color=discord.Color.dark_red()
             ).set_footer(text=f"Channel ID: {detail.get('id')}")
             channel_embeds.append(error_embed)
    if channel_embeds: await _send_report_embeds(scan_data, channel_embeds, "Kênh Text/Voice (Chi tiết)")

    # Thông tin Kênh Voice/Stage Tĩnh
    if voice_channel_static_data:
        try:
            voice_info_embeds = await embeds_guild.create_voice_channel_embeds(voice_channel_static_data, bot)
            await _send_report_embeds(scan_data, voice_info_embeds, "Kênh Voice/Stage Info")
        except Exception as ex: error_msg = f"Lỗi embed voice info: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    # 2. Roles & Boosters
    log.info(f"--- {e('role')} Báo cáo Roles & Boosters ---")
    if all_roles_list:
        try:
            role_embeds = await embeds_guild.create_role_embeds(all_roles_list, bot)
            await _send_report_embeds(scan_data, role_embeds, "Role")
        except Exception as ex: error_msg = f"Lỗi embed role: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)
    if boosters:
        try:
            booster_embeds = await embeds_guild.create_booster_embeds(boosters, bot, scan_data["scan_end_time"]) # Cần scan_end_time
            await _send_report_embeds(scan_data, booster_embeds, "Booster")
        except Exception as ex: error_msg = f"Lỗi embed booster: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    # 3. Hoạt động User & Leaderboards Cơ Bản
    log.info(f"--- {e('members')} Báo cáo Hoạt động User & Leaderboards Cơ Bản ---")
    try:
        # Cần user_role_changes cho embed này (dù có thể không dùng?)
        top_active_embed = await embeds_user.create_top_active_users_embed(user_activity, server, bot, user_role_changes)
        await _send_report_embeds(scan_data, [top_active_embed] if top_active_embed else [], "Top User Hoạt Động (Tin nhắn)")
    except Exception as ex: error_msg = f"Lỗi embed top active: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)
    try:
        oldest_embed = await embeds_user.create_top_oldest_members_embed(oldest_members_data, bot)
        await _send_report_embeds(scan_data, [oldest_embed] if oldest_embed else [], "Top Thành viên Lâu năm")
    except Exception as ex: error_msg = f"Lỗi embed top lâu năm: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    # Chi tiết Hoạt động User
    if user_activity:
        try:
            user_activity_embeds = await embeds_user.create_user_activity_embeds(
                user_activity, server, bot, config.MIN_MESSAGE_COUNT_FOR_REPORT, scan_data["overall_start_time"]
            )
            await _send_report_embeds(scan_data, user_activity_embeds, "Hoạt động User (Chi tiết)")
        except Exception as ex: error_msg = f"Lỗi embed user activity: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    # Leaderboards (Nội dung)
    async def _try_send_leaderboard(func, counter, name):
        try:
            embed = await func(counter, server, bot)
            await _send_report_embeds(scan_data, [embed] if embed else [], name)
        except Exception as ex:
            err = f"Lỗi embed {name}: {ex}"; log.error(f"{e('error')} {err}", exc_info=True); scan_errors.append(err)

    await _try_send_leaderboard(embeds_user.create_top_link_posters_embed, user_link_counts, "Top User Gửi Link")
    await _try_send_leaderboard(embeds_user.create_top_image_posters_embed, user_image_counts, "Top User Gửi Ảnh")
    await _try_send_leaderboard(embeds_user.create_top_emoji_users_embed, user_emoji_counts, "Top User Dùng Emoji (Content)")
    await _try_send_leaderboard(embeds_user.create_top_sticker_users_embed, user_sticker_counts, "Top User Gửi Sticker")

    # 4. Leaderboards (Tương tác & Sử dụng Item)
    log.info(f"--- {e('members')} Báo cáo Tương tác User & Sử dụng Item ---")
    await _try_send_leaderboard(embeds_user.create_top_mentioned_users_embed, user_mention_received_counts, "Top User Được Nhắc Tên")
    await _try_send_leaderboard(embeds_user.create_top_mentioning_users_embed, user_mention_given_counts, "Top User Hay Nhắc Tên")
    await _try_send_leaderboard(embeds_user.create_top_repliers_embed, user_reply_counts, "Top User Trả Lời")
    if scan_data.get("can_scan_reactions"):
        await _try_send_leaderboard(embeds_user.create_top_reaction_received_users_embed, user_reaction_received_counts, "Top User Nhận Reactions")
        await _try_send_leaderboard(embeds_analysis.create_top_emoji_usage_embed, reaction_emoji_counts, "Top Emoji Reactions Được Dùng") # Từ analysis
    try:
        top_sticker_usage_embed = await embeds_items.create_top_sticker_usage_embed(sticker_usage_counts, bot) # Chỉ cần counter và bot
        await _send_report_embeds(scan_data, [top_sticker_usage_embed] if top_sticker_usage_embed else [], "Top Sticker Được Dùng")
    except Exception as ex:
        error_msg = f"Lỗi embed top sticker usage: {ex}"
        log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)
        
    try: # Activity span dùng user_activity, không phải counter
        top_activity_span_embed = await embeds_user.create_top_activity_span_users_embed(user_activity, server, bot)
        await _send_report_embeds(scan_data, [top_activity_span_embed] if top_activity_span_embed else [], "Top User Hoạt Động Lâu Nhất (Span)")
    except Exception as ex: error_msg = f"Lỗi embed top activity span: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)
    if discord.AuditLogAction.thread_create in config.AUDIT_LOG_ACTIONS_TO_TRACK and user_thread_creation_counts:
        await _try_send_leaderboard(embeds_user.create_top_thread_creators_embed, user_thread_creation_counts, "Top User Tạo Thread")

    # 5. Invites, Webhooks, Integrations
    log.info(f"--- {e('invite')} Báo cáo Invites, Webhooks, Integrations ---")
    try:
        if invites_data:
            invite_embeds = await embeds_items.create_invite_embeds(invites_data, bot)
            await _send_report_embeds(scan_data, invite_embeds, "Invite")
        if invite_usage_counts:
            top_inviter_embed = await embeds_items.create_top_inviters_embed(invite_usage_counts, server, bot)
            await _send_report_embeds(scan_data, [top_inviter_embed] if top_inviter_embed else [], "Top Người Mời (Lượt sử dụng)")
        if webhooks_data or integrations_data:
            webhook_integration_embeds = await embeds_items.create_webhook_integration_embeds(webhooks_data, integrations_data, bot)
            await _send_report_embeds(scan_data, webhook_integration_embeds, "Webhook/Integration")
    except Exception as ex: error_msg = f"Lỗi embed inv/wh/int/inviter: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    # 6. Báo cáo Moderation & Admin
    log.info(f"--- {e('shield')} Báo cáo Moderation & Admin ---")
    try:
        perm_audit_embeds = await embeds_analysis.create_permission_audit_embeds(permission_audit_results, bot)
        await _send_report_embeds(scan_data, perm_audit_embeds, "Phân tích Quyền")
    except Exception as ex: error_msg = f"Lỗi embed phân tích quyền: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    if scan_data.get("can_scan_audit_log"):
        try:
            audit_log_embeds = await embeds_analysis.create_audit_log_summary_embeds(audit_logs_for_report_cached, server, bot)
            await _send_report_embeds(scan_data, audit_log_embeds, "Tóm tắt Audit Log (Gần đây)")
        except Exception as ex: error_msg = f"Lỗi embed Audit Log: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)
    try:
        role_stat_embeds = await embeds_analysis.create_role_change_stats_embeds(role_change_stats, server, bot)
        await _send_report_embeds(scan_data, role_stat_embeds, "Thống kê Cấp/Hủy Role (Bởi Mod)")
    except Exception as ex: error_msg = f"Lỗi embed thống kê role (mod): {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)
    try:
        user_role_stat_embeds = await embeds_analysis.create_user_role_change_embeds(user_role_changes, server, bot)
        await _send_report_embeds(scan_data, user_role_stat_embeds, "Thống kê Cấp/Hủy Role (Cho User)")
    except Exception as ex: error_msg = f"Lỗi embed thống kê role (user): {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)
    try:
        top_roles_embed = await embeds_analysis.create_top_roles_granted_embed(role_change_stats, server, bot)
        await _send_report_embeds(scan_data, [top_roles_embed] if top_roles_embed else [], "Top Roles Được Cấp")
    except Exception as ex: error_msg = f"Lỗi embed top roles granted: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    # 7. Báo cáo Phân tích (Keywords, Reactions)
    log.info(f"--- {e('stats')} Báo cáo Phân tích ---")
    if target_keywords and keyword_counts:
         try:
             keyword_embeds = await embeds_analysis.create_keyword_analysis_embeds(
                 keyword_counts, channel_keyword_counts, thread_keyword_counts,
                 user_keyword_counts, server, bot, target_keywords
             )
             await _send_report_embeds(scan_data, keyword_embeds, "Phân tích Từ khóa")
         except Exception as ex: error_msg = f"Lỗi embed phân tích từ khóa: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    if scan_data.get("can_scan_reactions") and reaction_emoji_counts:
         try:
             # Phân tích reaction tổng hợp dùng lại embed top usage
             reaction_analysis_embed = await embeds_analysis.create_top_emoji_usage_embed(reaction_emoji_counts, bot)
             await _send_report_embeds(scan_data, [reaction_analysis_embed] if reaction_analysis_embed else [], "Phân tích Biểu cảm (Sử dụng)")
         except Exception as ex: error_msg = f"Lỗi embed phân tích biểu cảm (usage): {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg)

    # 8. Báo cáo Tóm tắt Lỗi
    log.info(f"--- {e('warning')} Báo cáo Lỗi ---")
    try:
        if scan_errors:
            error_summary_embed = await embeds_analysis.create_error_embed(scan_errors, bot)
            await _send_report_embeds(scan_data, [error_summary_embed], "Tóm tắt Lỗi")
        else:
            log.info("Không có lỗi nào được ghi nhận trong quá trình quét.")
    except Exception as ex:
        log.error(f"Lỗi tạo embed tóm tắt lỗi: {ex}", exc_info=True)
        # Thử gửi lỗi thô nếu tạo embed lỗi
        try:
            error_chunks = ["**LỖI TẠO BÁO CÁO LỖI!**\n**Danh sách lỗi thô:**"]
            current_chunk = error_chunks[0]
            for err_line in scan_errors:
                 line_to_add = f"\n- {err_line}"
                 if len(current_chunk) + len(line_to_add) < 1900:
                     current_chunk += line_to_add
                 else:
                     error_chunks.append(line_to_add.strip()) # Bắt đầu chunk mới
                     current_chunk = error_chunks[-1]
            for chunk in error_chunks:
                 await scan_data["ctx"].send(chunk)
                 await asyncio.sleep(1)
        except Exception as raw_err_send_ex:
            log.error(f"Không thể gửi danh sách lỗi thô: {raw_err_send_ex}")

    end_time_reports = time.monotonic()
    log.info(f"✅ Hoàn thành tạo và gửi báo cáo embeds trong {end_time_reports - start_time_reports:.2f}s.")

# --- END OF FILE cogs/deep_scan_helpers/report_generation.py ---