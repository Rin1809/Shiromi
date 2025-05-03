# --- START OF FILE cogs/deep_scan_helpers/init_scan.py ---
import discord
from discord.ext import commands
import logging
import asyncio
import time
from typing import Dict, Any, List, Union, Optional
from collections import Counter, defaultdict

import config
import utils
import database
import discord_logging
from reporting import embeds_guild # Cần cho hằng số

log = logging.getLogger(__name__)

async def initialize_scan(scan_data: Dict[str, Any]) -> bool:
    """
    Thực hiện các bước khởi tạo và kiểm tra ban đầu cho quá trình quét.
    Bao gồm: gửi status ban đầu, tạo thread log, kiểm tra DB, xử lý keywords,
    kiểm tra quyền bot, fetch dữ liệu cache ban đầu, lọc kênh có thể truy cập.

    Trả về True nếu khởi tạo thành công, False nếu có lỗi nghiêm trọng.
    Cập nhật scan_data với các thông tin khởi tạo.
    """
    ctx: commands.Context = scan_data["ctx"]
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    scan_errors: List[str] = scan_data["scan_errors"]

    # --- Gửi tin nhắn trạng thái ban đầu ---
    try:
        initial_status_msg = await ctx.send(f"{e('loading')} Đang khởi tạo quét...")
        scan_data["initial_status_msg"] = initial_status_msg
    except discord.HTTPException as e_init:
        log.error(f"Không thể gửi tin nhắn khởi tạo: {e_init}")
        try:
            await ctx.author.send(f"Không thể bắt đầu quét server `{server.name}` do không thể gửi tin nhắn vào kênh `{ctx.channel.name}`.")
        except Exception:
            pass
        return False # Lỗi nghiêm trọng, không thể tiếp tục

    # --- Tạo Log Thread và đặt làm target ---
    try:
        thread_name = f"{e('stats')} S{server.id}-U{ctx.author.id}-{scan_data['overall_start_time'].strftime('%y%m%d-%H%M')}"
        if ctx.channel.permissions_for(server.me).create_public_threads:
            log_thread = await ctx.channel.create_thread(
                name=thread_name,
                type=discord.ChannelType.public_thread,
                auto_archive_duration=10080 # 1 tuần
            )
            discord_logging.set_log_target_thread(log_thread) # Đặt target mới
            scan_data["log_thread"] = log_thread
            log.info(f"{e('success')} Đã tạo và đặt luồng log đích: {log_thread.mention}")
        else:
            log.error(f"{e('error')} Bot thiếu quyền 'Create Public Threads'. Sẽ không tạo/gửi log vào luồng.")
            scan_errors.append("Thiếu quyền tạo luồng log.")
            discord_logging.set_log_target_thread(None) # Đảm bảo target là None
    except Exception as thread_err:
        log.error(f"{e('error')} Lỗi khi tạo luồng log: {thread_err}", exc_info=True)
        scan_errors.append(f"Lỗi tạo luồng log: {thread_err}")
        discord_logging.set_log_target_thread(None)

    # --- Kiểm tra Database ---
    db_pool = await database.connect_db()
    if not db_pool:
        error_msg = f"{e('error')} Không thể kết nối đến database. Quét sâu không thể tiếp tục."
        await _update_initial_status(scan_data, content=error_msg, embed=None)
        if scan_data["log_thread"]:
             try: await scan_data["log_thread"].send(f"{e('error')} Lỗi nghiêm trọng: Không thể kết nối database.")
             except Exception: pass
        raise ConnectionError("Không thể kết nối database.") # Raise để dừng hẳn ở cog
    log.info(f"{e('success')} Kết nối database đã được xác nhận.")

    # --- Xử lý Keywords ---
    keywords_str = scan_data.get("keywords_str")
    target_keywords = []
    if keywords_str:
        try:
            target_keywords = [kw.strip().lower() for kw in keywords_str.split(',') if kw.strip()]
            if target_keywords:
                log.info(f"{e('hashtag')} Sẽ tìm kiếm: [blue]{', '.join(target_keywords)}[/]")
            else:
                log.warning("Keywords trống hoặc không hợp lệ sau khi xử lý.")
                scan_errors.append("Keywords trống/không hợp lệ.")
        except Exception as kw_err:
            log.error(f"Lỗi xử lý keywords: {kw_err}")
            scan_errors.append(f"Lỗi keywords: {kw_err}")
            target_keywords = [] # Reset nếu lỗi
    scan_data["target_keywords"] = target_keywords

    # --- Kiểm tra quyền Bot ---
    if not await _check_bot_permissions(scan_data):
        # Lỗi thiếu quyền cơ bản đã được xử lý và báo cáo trong _check_bot_permissions
        return False # Dừng nếu thiếu quyền cơ bản

    # --- Lấy dữ liệu cache ban đầu ---
    if not await _fetch_initial_cache(scan_data):
        # Lỗi fetch cache đã được xử lý và báo cáo trong _fetch_initial_cache
        return False # Dừng nếu không fetch được cache ban đầu

    # --- Lọc kênh và kiểm tra quyền truy cập kênh ---
    await _filter_accessible_channels(scan_data)
    total_accessible_channels = len(scan_data["accessible_channels"])
    log.info(f"Tìm thấy [green]{total_accessible_channels}[/] kênh có thể quét, [yellow]{scan_data['skipped_channels_count']}[/] bị bỏ qua.")

    # --- Cập nhật Embed trạng thái ban đầu ---
    start_embed = _create_start_embed(scan_data)
    await _update_initial_status(scan_data, content=None, embed=start_embed)

    # --- Xử lý trường hợp không có kênh nào để quét ---
    if total_accessible_channels == 0:
        final_message = f"{e('error')} Không có kênh text/voice nào để quét (kiểm tra quyền của bot)."
        log.error(final_message)
        await _update_initial_status(scan_data, content=final_message, embed=None)
        if scan_data["log_thread"]:
            try: await scan_data["log_thread"].send(final_message)
            except Exception: pass
        # Không raise lỗi ở đây, để cog xử lý và reset cooldown
        return False # Báo hiệu dừng quét

    # Đánh dấu giai đoạn khởi tạo thành công
    scan_data["scan_started"] = True # Đánh dấu quét đã bắt đầu (quan trọng cho việc reset cooldown)
    log.info("Hoàn thành giai đoạn khởi tạo và kiểm tra.")
    return True


async def _update_initial_status(scan_data: Dict[str, Any], content: Optional[str], embed: Optional[discord.Embed]):
    """Cập nhật tin nhắn trạng thái ban đầu một cách an toàn."""
    initial_msg = scan_data.get("initial_status_msg")
    ctx = scan_data["ctx"]
    if not initial_msg:
        log.warning("Không tìm thấy initial_status_msg để cập nhật.")
        return

    try:
        await initial_msg.edit(content=content, embed=embed)
    except (discord.NotFound, discord.HTTPException) as edit_err:
        log.warning(f"Không thể sửa/tìm thấy tin nhắn trạng thái ban đầu: {edit_err}")
        # Thử gửi lại nếu bị xóa hoặc lỗi
        try:
            scan_data["initial_status_msg"] = await ctx.send(content=content, embed=embed)
        except discord.HTTPException as send_err:
            log.error(f"Không thể gửi lại tin nhắn trạng thái ban đầu: {send_err}")
            # Đây là lỗi nghiêm trọng, có thể ảnh hưởng đến các cập nhật sau
    except Exception as e:
         log.error(f"Lỗi không xác định khi cập nhật tin nhắn trạng thái: {e}", exc_info=True)


async def _check_bot_permissions(scan_data: Dict[str, Any]) -> bool:
    """Kiểm tra các quyền cơ bản và tùy chọn của bot."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    e = lambda name: utils.get_emoji(name, bot)

    # Quyền cơ bản tuyệt đối cần thiết
    required_perms_base = [
        "view_channel",
        "read_message_history",
        "embed_links", # Để gửi báo cáo
        "attach_files" # Để gửi file export
    ]
    bot_perms = server.me.guild_permissions
    missing_perms: List[str] = [p for p in required_perms_base if not getattr(bot_perms, p, False)]

    if missing_perms:
        perms_str = ', '.join(f"`{p}`" for p in missing_perms)
        log.error(f"{e('error')} Bot thiếu quyền cơ bản: {perms_str}")
        error_msg = f"{e('error')} Bot thiếu quyền cơ bản: {perms_str}. Không thể tiếp tục quét."
        await _update_initial_status(scan_data, content=error_msg, embed=None)
        if scan_data["log_thread"]:
            try: await scan_data["log_thread"].send(f"{e('error')} **Dừng quét:** Bot thiếu quyền `{perms_str}`.")
            except Exception: pass
        # Raise lỗi để cog bắt và reset cooldown
        raise commands.BotMissingPermissions(missing_perms)
        # return False # Chỉ cần raise là đủ

    log.info(f"{e('success')} Quyền cơ bản OK.")

    # Kiểm tra quyền cho các tính năng tùy chọn
    scan_data["can_scan_invites"] = bot_perms.manage_guild
    scan_data["can_scan_webhooks"] = bot_perms.manage_webhooks
    scan_data["can_scan_integrations"] = bot_perms.manage_guild
    scan_data["can_scan_audit_log"] = bot_perms.view_audit_log
    # Quyền quét reactions phụ thuộc vào cấu hình và quyền đọc lịch sử
    scan_data["can_scan_reactions"] = config.ENABLE_REACTION_SCAN and bot_perms.read_message_history
    # Quyền xem thread lưu trữ (cần 1 trong 2)
    scan_data["can_scan_archived_threads"] = bot_perms.read_message_history or bot_perms.manage_threads

    # Log cảnh báo nếu thiếu quyền cho tính năng tùy chọn
    if not scan_data["can_scan_invites"]:
        scan_errors.append("Thiếu quyền 'Manage Server', bỏ qua invites.")
        log.warning(f"{e('warning')} Bỏ qua invites (thiếu Manage Server)")
    if not scan_data["can_scan_webhooks"]:
        scan_errors.append("Thiếu quyền 'Manage Webhooks', bỏ qua webhooks.")
        log.warning(f"{e('warning')} Bỏ qua webhooks (thiếu Manage Webhooks)")
    if not scan_data["can_scan_integrations"]:
        scan_errors.append("Thiếu quyền 'Manage Server', bỏ qua integrations.")
        log.warning(f"{e('warning')} Bỏ qua integrations (thiếu Manage Server)")
    if not scan_data["can_scan_audit_log"]:
        scan_errors.append("Thiếu quyền 'View Audit Log', bỏ qua Audit Log.")
        log.warning(f"{e('warning')} Bỏ qua Audit Log (thiếu View Audit Log)")
    if config.ENABLE_REACTION_SCAN and not bot_perms.read_message_history:
        scan_errors.append("Bỏ qua quét Biểu cảm: Thiếu quyền 'Read Message History'.")
        log.warning(f"{e('warning')} Bỏ qua quét Biểu cảm (thiếu Read History)")
    if not scan_data["can_scan_archived_threads"]:
        scan_errors.append("Thiếu quyền xem luồng lưu trữ.")
        log.warning(f"{e('warning')} Bỏ qua luồng lưu trữ (thiếu quyền).")

    return True


async def _fetch_initial_cache(scan_data: Dict[str, Any]) -> bool:
    """Fetch dữ liệu cache ban đầu (members, roles, channels)."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    e = lambda name: utils.get_emoji(name, bot)
    log.info(f"{e('loading')} Đang fetch dữ liệu cache ban đầu...")

    try:
        current_members_list = [] # Khởi tạo list rỗng
        # Cố gắng chunk hoặc fetch members để đảm bảo cache cập nhật
        if bot.intents.members:
            log.debug("Intents Members đang bật, đang fetch members...")
            # --- SỬA Ở ĐÂY ---
            # Dùng list comprehension với async for để lấy hết members
            try:
                current_members_list = [member async for member in server.fetch_members(limit=None)]
                log.debug(f"Fetch {len(current_members_list)} members hoàn tất.")
            except discord.Forbidden:
                 log.error(f"{e('error')} Lỗi quyền khi fetch members. Bot có thiếu Members Intent không?")
                 # Có thể raise lỗi hoặc trả về False tùy theo cách xử lý mong muốn
                 raise # Re-raise lỗi để dừng quét
            except discord.HTTPException as fetch_err:
                 log.error(f"{e('error')} Lỗi HTTP khi fetch members: {fetch_err.status} {fetch_err.text}")
                 raise # Re-raise lỗi
            # --- KẾT THÚC SỬA ---
        else:
            log.warning("Members Intent đang tắt, dữ liệu member có thể không đầy đủ. Sử dụng cache hiện tại.")
            current_members_list = list(server.members) # Lấy từ cache nếu không fetch được

        # Lưu danh sách member vào scan_data
        scan_data["current_members_list"] = current_members_list
        log.info(f"Lấy được {len(current_members_list)} thành viên.")

        # Đếm trạng thái member ban đầu
        scan_data["initial_member_status_counts"] = Counter(str(m.status) for m in current_members_list)

        # Đếm loại kênh
        scan_data["channel_counts"] = Counter(c.type for c in server.channels)

        # Lấy danh sách roles (không tính @everyone), sắp xếp theo vị trí
        scan_data["all_roles_list"] = sorted(
            [r for r in server.roles if not r.is_default()],
            key=lambda r: r.position,
            reverse=True
        )
        log.info(f"Tổng cộng {len(scan_data['all_roles_list'])} roles (không tính @everyone).")
        return True

    except discord.Forbidden:
         log.error(f"{e('error')} Lỗi quyền khi fetch dữ liệu cache (thiếu Members Intent?).")
         error_msg = f"{e('error')} Lỗi quyền khi lấy dữ liệu ban đầu (thiếu Members Intent?). Không thể tiếp tục."
         await _update_initial_status(scan_data, content=error_msg, embed=None)
         if scan_data["log_thread"]:
              try: await scan_data["log_thread"].send(f"{e('error')} **Dừng quét:** Lỗi fetch dữ liệu cache (quyền).")
              except Exception: pass
         # Không raise ở đây, trả về False để cog xử lý
         return False
    except Exception as cache_err:
        log.error(f"{e('error')} Lỗi fetch dữ liệu cache ban đầu: {cache_err}", exc_info=True)
        error_msg = f"{e('error')} Lỗi fetch dữ liệu cache ban đầu: {cache_err}"
        await _update_initial_status(scan_data, content=error_msg, embed=None)
        if scan_data["log_thread"]:
             try: await scan_data["log_thread"].send(f"{e('error')} **Dừng quét:** Lỗi fetch dữ liệu cache.")
             except Exception: pass
        # Raise lỗi để cog bắt và reset cooldown
        raise RuntimeError(f"Failed to fetch initial guild data: {cache_err}") from cache_err
        # return False


async def _filter_accessible_channels(scan_data: Dict[str, Any]):
    """Lọc các kênh Text/Voice mà bot có thể đọc lịch sử."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    scan_errors: List[str] = scan_data["scan_errors"]
    channel_details: List[Dict[str, Any]] = scan_data["channel_details"]
    accessible_channels: List[Union[discord.TextChannel, discord.VoiceChannel]] = []
    skipped_channels_count = 0

    log.info(f"{utils.get_emoji('info', bot)} Đang lọc kênh text & voice...")
    # Chỉ lấy TextChannel và VoiceChannel từ cache của server
    channels_to_scan = server.text_channels + server.voice_channels

    for channel in channels_to_scan:
        channel_type_emoji = utils.get_channel_type_emoji(channel, bot)
        try:
            # Kiểm tra quyền cụ thể trên kênh này
            perms = channel.permissions_for(server.me)
            can_view = perms.view_channel
            can_read_history = perms.read_message_history

            if can_view and can_read_history:
                accessible_channels.append(channel)
                log.debug(f"Kênh {channel_type_emoji} '{channel.name}' ({channel.id}) có thể truy cập.")
                # Thêm entry cơ bản vào channel_details ngay cả khi truy cập được
                # Các trường khác sẽ được cập nhật sau khi quét xong kênh
                channel_details.append({
                    "type": str(channel.type),
                    "name": channel.name,
                    "id": channel.id,
                    "created_at": channel.created_at,
                    "category": getattr(channel.category, 'name', "N/A"),
                    "category_id": getattr(channel.category, 'id', None),
                    "error": None, # Chưa có lỗi
                    "processed": False, # Chưa xử lý
                    "message_count": 0,
                    "reaction_count": 0,
                    "threads_data": [] # Khởi tạo list cho threads
                })
            else:
                skipped_channels_count += 1
                reason = "Thiếu View Channel" if not can_view else "Thiếu Read History"
                scan_errors.append(f"Kênh {channel_type_emoji} #{channel.name}: Bỏ qua ({reason}).")
                log.warning(f"Bỏ qua kênh {channel_type_emoji} [yellow]#{channel.name}[/]: {reason}")
                # Thêm entry vào channel_details với trạng thái lỗi
                channel_details.append({
                    "type": str(channel.type),
                    "name": channel.name,
                    "id": channel.id,
                    "created_at": channel.created_at,
                    "category": getattr(channel.category, 'name', "N/A"),
                    "category_id": getattr(channel.category, 'id', None),
                    "error": f"Bỏ qua do {reason}",
                    "processed": False, # Đánh dấu không xử lý
                    "message_count": 0,
                    "reaction_count": 0,
                     "threads_data": []
                })
        except Exception as perm_check_err:
            skipped_channels_count += 1
            error_msg = f"Lỗi kiểm tra quyền kênh #{channel.name}: {perm_check_err}"
            log.error(f"{utils.get_emoji('error', bot)} {error_msg}", exc_info=True)
            scan_errors.append(error_msg)
            # Thêm entry lỗi
            channel_details.append({
                "type": str(channel.type),
                "name": channel.name,
                "id": channel.id,
                 "created_at": channel.created_at, # Vẫn cố lấy nếu có
                 "category": getattr(channel.category, 'name', "N/A"),
                 "category_id": getattr(channel.category, 'id', None),
                "error": f"Lỗi kiểm tra quyền: {perm_check_err}",
                "processed": False,
                "message_count": 0,
                "reaction_count": 0,
                 "threads_data": []
            })

    scan_data["accessible_channels"] = accessible_channels
    scan_data["skipped_channels_count"] = skipped_channels_count


def _create_start_embed(scan_data: Dict[str, Any]) -> discord.Embed:
    """Tạo embed thông báo bắt đầu quét."""
    ctx: commands.Context = scan_data["ctx"]
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)

    total_accessible_channels = len(scan_data["accessible_channels"])
    skipped_channels_count = scan_data["skipped_channels_count"]
    initiator_display_mention = f"{ctx.author.mention} (`{ctx.author.display_name}`)"
    log_thread = scan_data.get("log_thread")
    target_keywords = scan_data.get("target_keywords", []) 

    start_embed = discord.Embed(
        title=f"{e('loading')} Chuẩn Bị Quét Sâu Server: {server.name}",
        description=f"Yêu cầu bởi: {initiator_display_mention}",
        color=discord.Color.blue(),
        timestamp=scan_data["overall_start_time"]
    )
    if ctx.author.display_avatar:
        start_embed.set_author(name=ctx.author.display_name, icon_url=ctx.author.display_avatar.url)
    else:
        start_embed.set_author(name=ctx.author.display_name)

    start_embed.add_field(
        name="Cấu hình Xuất",
        value=f"CSV: `{scan_data['export_csv']}`, JSON: `{scan_data['export_json']}`",
        inline=True
    )

    start_embed.add_field(
        name="Keywords",
        value=f"`{', '.join(target_keywords) if target_keywords else 'Không tìm kiếm'}`",
        inline=True
    )


    start_embed.add_field(
        name="Mục tiêu Quét",
        value=f"Quét **{total_accessible_channels}** kênh text/voice ({skipped_channels_count} bỏ qua).",
        inline=False
    )

    if log_thread:
        start_embed.add_field(name="Log Chi Tiết", value=f"Xem tại: {log_thread.mention}", inline=False)
    else:
        start_embed.add_field(name="Log Chi Tiết", value="Chỉ hiển thị trên Console (Lỗi tạo/quyền thread).", inline=False)

    # Thông báo về Reaction Scan
    can_scan_reactions = scan_data.get("can_scan_reactions", False)
    reaction_scan_status = "Tắt (Cấu hình)"
    if config.ENABLE_REACTION_SCAN:
         reaction_scan_status = f"**{'Bật' if can_scan_reactions else 'Tắt (Thiếu quyền)'}**"
         if can_scan_reactions: reaction_scan_status += " (Có thể chậm)"

    start_embed.add_field(
        name=f"{e('reaction')} Reaction Scan",
        value=reaction_scan_status,
        inline=True
    )

    start_embed.set_footer(text="Đang bắt đầu quét tin nhắn...")
    return start_embed

# --- END OF FILE cogs/deep_scan_helpers/init_scan.py ---