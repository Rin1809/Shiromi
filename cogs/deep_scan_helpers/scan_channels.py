# --- START OF FILE cogs/deep_scan_helpers/scan_channels.py ---
import discord
from discord.ext import commands
import logging
import asyncio
import time
import datetime
import re
from typing import Dict, Any, List, Union, Optional
from collections import Counter, defaultdict

import config
import utils
import discord_logging
from reporting import embeds_guild # Cần cho hằng số

log = logging.getLogger(__name__)

# Biểu thức chính quy để tối ưu việc tìm kiếm
URL_REGEX = re.compile(r'https?://\S+')
EMOJI_REGEX = re.compile(r'<a?:[a-zA-Z0-9_]+:[0-9]+>|[\U00010000-\U0010ffff]') # Tìm cả emoji custom và unicode

async def scan_all_channels_and_threads(scan_data: Dict[str, Any]):
    """Quét tất cả các kênh và luồng có thể truy cập."""
    accessible_channels: List[Union[discord.TextChannel, discord.VoiceChannel]] = scan_data["accessible_channels"]
    total_accessible_channels = len(accessible_channels)
    bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)

    log.info(f"Bắt đầu quét {total_accessible_channels} kênh...")

    last_status_update_time = scan_data["overall_start_time"]
    update_interval_seconds = 12 # Tần suất cập nhật trạng thái (giây)
    status_message = scan_data["initial_status_msg"] # Lấy tin nhắn status ban đầu

    for current_channel_index, channel in enumerate(accessible_channels, 1):
        # Khởi tạo biến đếm/dữ liệu cho kênh hiện tại
        channel_message_count = 0
        channel_reaction_count = 0
        channel_scan_start_time = discord.utils.utcnow()
        channel_processed_flag = False # Đánh dấu kênh đã được xử lý ít nhất 1 tin nhắn
        channel_error: Optional[str] = None
        author_counter_channel: Counter = Counter() # Đếm tin nhắn user (không phải bot) trong kênh này
        channel_threads_data: List[Dict[str, Any]] = []
        current_channel_thread_count = 0 # Số luồng quét được trong kênh này
        current_channel_skipped_threads = 0 # Số luồng bị bỏ qua trong kênh này

        channel_type_emoji = utils.get_channel_type_emoji(channel, bot)
        channel_type_name = "Voice" if isinstance(channel, discord.VoiceChannel) else "Text"

        log.info(f"[bold]({current_channel_index}/{total_accessible_channels})[/bold] Đang quét kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/] ({channel.id})")

        # --- Cập nhật trạng thái quét kênh ---
        channel_status_embed = discord.Embed(
            title=f"{e('loading')} Bắt đầu quét: {channel_type_emoji} #{channel.name}",
            description=f"Kênh {current_channel_index}/{total_accessible_channels}\nĐang đọc lịch sử...",
            color=discord.Color.yellow()
        )
        status_message = await _update_status_message(scan_data["ctx"], status_message, channel_status_embed)
        scan_data["status_message"] = status_message # Lưu lại msg object mới nếu có

        # --- Bắt đầu quét kênh ---
        try:
            message_iterator = channel.history(limit=None) # Lấy toàn bộ lịch sử

            async for message in message_iterator:
                # --- Xử lý từng tin nhắn ---
                timestamp = message.created_at
                if not message.author: continue # Bỏ qua tin nhắn hệ thống không có tác giả

                author_id = message.author.id
                is_bot = message.author.bot

                channel_message_count += 1
                scan_data["overall_total_message_count"] += 1
                channel_processed_flag = True # Đánh dấu đã xử lý

                # Cập nhật user_activity
                user_data = scan_data["user_activity"][author_id]
                user_data['message_count'] += 1
                if user_data['first_seen'] is None or timestamp < user_data['first_seen']:
                    user_data['first_seen'] = timestamp
                if user_data['last_seen'] is None or timestamp > user_data['last_seen']:
                    user_data['last_seen'] = timestamp
                if is_bot:
                    user_data['is_bot'] = True
                else:
                    # Chỉ đếm tin nhắn user trong kênh này cho top chatter
                    author_counter_channel[author_id] += 1

                # --- Phân tích nội dung tin nhắn (chỉ cho user không phải bot) ---
                if not is_bot:
                    msg_content = message.content or ""

                    # Đếm link
                    link_count = len(URL_REGEX.findall(msg_content))
                    scan_data["user_link_counts"][author_id] += link_count
                    user_data['link_count'] += link_count

                    # Đếm ảnh đính kèm
                    image_count = sum(1 for att in message.attachments if att.content_type and att.content_type.startswith('image/'))
                    scan_data["user_image_counts"][author_id] += image_count
                    user_data['image_count'] += image_count

                    # Đếm emoji trong nội dung
                    emoji_count = len(EMOJI_REGEX.findall(msg_content))
                    scan_data["user_emoji_counts"][author_id] += emoji_count
                    user_data['emoji_count'] += emoji_count

                    # Đếm sticker
                    sticker_count = len(message.stickers)
                    scan_data["user_sticker_counts"][author_id] += sticker_count
                    user_data['sticker_count'] += sticker_count
                    for sticker_item in message.stickers:
                        scan_data["sticker_usage_counts"][str(sticker_item.id)] += 1

                    # Đếm mention (chỉ user, không tính bot)
                    non_bot_mentions = [m for m in message.mentions if not m.bot]
                    if non_bot_mentions:
                        mention_given_count = len(non_bot_mentions)
                        scan_data["user_mention_given_counts"][author_id] += mention_given_count
                        user_data['mention_given_count'] += mention_given_count
                        # Đếm số lần được nhắc tên cho người bị nhắc
                        for mentioned_user in non_bot_mentions:
                            scan_data["user_mention_received_counts"][mentioned_user.id] += 1
                            scan_data["user_activity"][mentioned_user.id]['mention_received_count'] += 1

                    # Đếm reply
                    if message.reference and message.reference.message_id:
                        scan_data["user_reply_counts"][author_id] += 1
                        user_data['reply_count'] += 1

                # --- Đếm keywords (nếu có) ---
                target_keywords = scan_data["target_keywords"]
                if target_keywords and message.content:
                    message_content_lower = message.content.lower()
                    for keyword in target_keywords:
                        count_in_msg = message_content_lower.count(keyword)
                        if count_in_msg > 0:
                            scan_data["keyword_counts"][keyword] += count_in_msg
                            scan_data["channel_keyword_counts"][channel.id][keyword] += count_in_msg
                            if not is_bot:
                                scan_data["user_keyword_counts"][author_id][keyword] += count_in_msg

                # --- Đếm reactions (nếu bật và có quyền) ---
                can_scan_reactions = scan_data.get("can_scan_reactions", False)
                if can_scan_reactions and message.reactions:
                    try:
                        msg_react_count = 0
                        for reaction in message.reactions:
                            count = reaction.count
                            msg_react_count += count
                            # Key là string representation của emoji (custom hoặc unicode)
                            scan_data["reaction_emoji_counts"][str(reaction.emoji)] += count
                        channel_reaction_count += msg_react_count
                        scan_data["overall_total_reaction_count"] += msg_react_count
                        # Đếm reaction nhận được (chỉ cho user)
                        if not is_bot:
                            scan_data["user_reaction_received_counts"][author_id] += msg_react_count
                            user_data['reaction_received_count'] += msg_react_count
                    except Exception as react_err:
                        log.warning(f"Lỗi xử lý reaction msg {message.id} kênh {channel.id}: {react_err}")

                # --- Cập nhật trạng thái định kỳ ---
                now = discord.utils.utcnow()
                if (now - last_status_update_time).total_seconds() > update_interval_seconds:
                    status_embed = _create_progress_embed(scan_data, channel, current_channel_index, channel_message_count, channel_scan_start_time, now)
                    status_message = await _update_status_message(scan_data["ctx"], status_message, status_embed)
                    scan_data["status_message"] = status_message # Lưu lại msg object mới
                    last_status_update_time = now

            # --- Kết thúc quét tin nhắn kênh ---
            channel_scan_duration = discord.utils.utcnow() - channel_scan_start_time
            log.info(f"  {e('success')} Hoàn thành kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/]: {channel_message_count:,} tin nhắn trong [magenta]{utils.format_timedelta(channel_scan_duration)}[/].")

            # --- Thu thập chi tiết kênh sau quét ---
            log.info(f"  {e('info')} Đang thu thập chi tiết kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/]...")
            channel_detail_entry = await _gather_channel_details(scan_data, channel, author_counter_channel, channel_message_count, channel_reaction_count, channel_scan_duration, channel_error)

            # --- Quét luồng (nếu là kênh text) ---
            if isinstance(channel, discord.TextChannel):
                 await _scan_threads_in_channel(scan_data, channel, channel_detail_entry) # Truyền channel_detail_entry để cập nhật threads_data
            else:
                 log.info(f"  {e('thread')} Bỏ qua quét luồng cho kênh voice {channel_type_emoji} [cyan]#{channel.name}[/].")


            scan_data["processed_channels_count"] += 1
            log.info(f"  {e('success')} Hoàn thành xử lý kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/].")
            await asyncio.sleep(0.1) # Nghỉ nhẹ giữa các kênh

        # --- Xử lý lỗi quét kênh ---
        except Exception as e_channel:
            channel_error_msg = f"Lỗi kênh {channel_type_name} #{channel.name}: {e_channel}"
            log.error(f"{e('error')} {channel_error_msg}", exc_info=not isinstance(e_channel, discord.Forbidden))
            scan_data["scan_errors"].append(channel_error_msg)

            # Cập nhật channel_details với lỗi
            detail_entry = next((item for item in scan_data["channel_details"] if item.get('id') == channel.id), None)
            error_prefix = "FATAL SCAN ERROR: " if not channel_processed_flag else "PARTIAL SCAN ERROR: "
            full_error = f"{error_prefix}{channel_error_msg}"

            if detail_entry:
                detail_entry["error"] = (detail_entry.get("error", "") + f"\n{full_error}").strip()
                detail_entry["processed"] = channel_processed_flag # Kênh có thể đã xử lý được một phần
            else:
                # Thêm entry mới nếu chưa có (nên có từ init_scan)
                scan_data["channel_details"].append({
                    "type": str(channel.type), "name": channel.name, "id": channel.id,
                    "created_at": channel.created_at,
                    "category": getattr(channel.category, 'name', "N/A"),
                    "category_id": getattr(channel.category, 'id', None),
                    "error": full_error,
                    "message_count": channel_message_count, # Ghi lại số msg đã xử lý trước lỗi
                    "reaction_count": channel_reaction_count, # Ghi lại số reaction đã xử lý
                    "processed": channel_processed_flag,
                    "threads_data": []
                })

            # Tăng số kênh bị bỏ qua nếu lỗi xảy ra trước khi xử lý bất kỳ tin nhắn nào
            if not channel_processed_flag:
                 # Đảm bảo không đếm trùng kênh đã skip ở init_scan
                 if not any(d['id'] == channel.id and not d.get('processed', True) for d in scan_data["channel_details"]):
                      scan_data["skipped_channels_count"] += 1

            # Thông báo lỗi tạm thời cho người dùng
            try:
                await scan_data["ctx"].send(f"{e('error')} {channel_error_msg}. Dữ liệu kênh #{channel.name} có thể không đầy đủ.", delete_after=30)
            except Exception:
                pass
            await asyncio.sleep(2) # Chờ một chút trước khi tiếp tục kênh khác

    log.info("Hoàn thành quét tất cả các kênh và luồng.")

async def _scan_threads_in_channel(scan_data: Dict[str, Any], parent_channel: discord.TextChannel, channel_detail_entry: Dict[str, Any]):
    """Quét tất cả các luồng (active và archived) trong một kênh text."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    can_scan_archived = scan_data.get("can_scan_archived_threads", False)
    scan_errors: List[str] = scan_data["scan_errors"]

    log.info(f"  {e('thread')} Đang kiểm tra luồng trong kênh text {e('text_channel')} [cyan]#{parent_channel.name}[/]...")

    threads_to_scan: List[discord.Thread] = []
    try:
        # Lấy active threads từ cache
        threads_to_scan.extend(parent_channel.threads)

        # Fetch archived threads nếu có quyền
        if can_scan_archived:
            log.info(f"    Đang fetch luồng lưu trữ...")
            async for thread in parent_channel.archived_threads(limit=None): # Lấy hết
                threads_to_scan.append(thread)
            log.info(f"    Fetch xong luồng lưu trữ.")
        else:
            log.info("    Bỏ qua luồng lưu trữ do thiếu quyền.")

        # Loại bỏ trùng lặp (dùng dict theo ID) và giữ lại thứ tự tương đối
        unique_threads_to_scan = list({t.id: t for t in threads_to_scan}.values())
        total_threads_found = len(unique_threads_to_scan)

        if total_threads_found == 0:
            log.info(f"  {e('info')} Không tìm thấy luồng trong kênh text [cyan]#{parent_channel.name}[/].")
            channel_detail_entry["threads_data"] = [] # Đảm bảo list trống
            return

        log.info(f"  Tìm thấy {total_threads_found} luồng. Bắt đầu quét...")
        thread_index = 0
        current_channel_thread_count = 0
        current_channel_skipped_threads = 0
        threads_data_list = []

        for thread in unique_threads_to_scan:
            thread_index += 1
            thread_message_count = 0
            thread_reaction_count = 0
            thread_scan_start_time = discord.utils.utcnow()
            thread_processed_flag = False
            error_in_thread: Optional[str] = None
            thread_data_entry: Dict[str, Any] = { # Khởi tạo entry cho thread này
                 "id": thread.id, "name": thread.name, "archived": thread.archived,
                 "locked": thread.locked, "created_at": thread.created_at.isoformat() if thread.created_at else None,
                 "owner_id": thread.owner_id, "owner_mention": "N/A", "owner_name": "N/A",
                 "message_count": 0, "reaction_count": None, "scan_duration_seconds": 0, "error": None
            }

            # --- Kiểm tra quyền luồng ---
            try:
                thread_perms = thread.permissions_for(server.me)
                if not thread_perms.view_channel or not thread_perms.read_message_history:
                    reason = "Thiếu View" if not thread_perms.view_channel else "Thiếu Read History"
                    log.warning(f"    Bỏ qua luồng '{thread.name}' ({thread.id}): {reason}.")
                    scan_errors.append(f"Luồng '{thread.name}' ({thread.id}): Bỏ qua ({reason}).")
                    scan_data["skipped_threads_count"] += 1
                    current_channel_skipped_threads += 1
                    thread_data_entry["error"] = f"Bỏ qua do {reason}"
                    threads_data_list.append(thread_data_entry)
                    continue # Bỏ qua luồng này
            except Exception as thread_perm_err:
                log.error(f"    {e('error')} Lỗi kiểm tra quyền luồng '{thread.name}': {thread_perm_err}", exc_info=True)
                scan_errors.append(f"Luồng '{thread.name}': Lỗi kiểm tra quyền.")
                scan_data["skipped_threads_count"] += 1
                current_channel_skipped_threads += 1
                thread_data_entry["error"] = f"Lỗi kiểm tra quyền: {thread_perm_err}"
                threads_data_list.append(thread_data_entry)
                continue # Bỏ qua luồng này

            log.info(f"    [bold]({thread_index}/{total_threads_found})[/bold] Đang quét luồng [magenta]'{thread.name}'[/] ({thread.id})...")

            # --- Try/Except quét luồng ---
            try:
                # --- Quét tin nhắn luồng ---
                thread_message_iterator = thread.history(limit=None)
                async for message in thread_message_iterator:
                    # --- Xử lý tin nhắn luồng (tương tự kênh cha) ---
                    timestamp = message.created_at
                    if not message.author: continue

                    author_id = message.author.id
                    is_bot = message.author.bot

                    thread_message_count += 1
                    scan_data["overall_total_message_count"] += 1
                    thread_processed_flag = True

                    # Cập nhật user_activity
                    user_data = scan_data["user_activity"][author_id]
                    user_data['message_count'] += 1
                    if user_data['first_seen'] is None or timestamp < user_data['first_seen']:
                        user_data['first_seen'] = timestamp
                    if user_data['last_seen'] is None or timestamp > user_data['last_seen']:
                        user_data['last_seen'] = timestamp
                    if is_bot:
                        user_data['is_bot'] = True

                    # Phân tích nội dung (chỉ cho user)
                    if not is_bot:
                        msg_content = message.content or ""
                        link_count = len(URL_REGEX.findall(msg_content)); scan_data["user_link_counts"][author_id] += link_count; user_data['link_count'] += link_count
                        image_count = sum(1 for att in message.attachments if att.content_type and att.content_type.startswith('image/')); scan_data["user_image_counts"][author_id] += image_count; user_data['image_count'] += image_count
                        emoji_count = len(EMOJI_REGEX.findall(msg_content)); scan_data["user_emoji_counts"][author_id] += emoji_count; user_data['emoji_count'] += emoji_count
                        sticker_count = len(message.stickers); scan_data["user_sticker_counts"][author_id] += sticker_count; user_data['sticker_count'] += sticker_count
                        for sticker_item in message.stickers: scan_data["sticker_usage_counts"][str(sticker_item.id)] += 1
                        non_bot_mentions = [m for m in message.mentions if not m.bot]
                        if non_bot_mentions:
                             mention_given_count = len(non_bot_mentions); scan_data["user_mention_given_counts"][author_id] += mention_given_count; user_data['mention_given_count'] += mention_given_count
                             for mentioned_user in non_bot_mentions: scan_data["user_mention_received_counts"][mentioned_user.id] += 1; scan_data["user_activity"][mentioned_user.id]['mention_received_count'] += 1
                        if message.reference and message.reference.message_id: scan_data["user_reply_counts"][author_id] += 1; user_data['reply_count'] += 1

                    # Đếm keywords
                    target_keywords = scan_data["target_keywords"]
                    if target_keywords and message.content:
                        message_content_lower = message.content.lower()
                        for keyword in target_keywords:
                            count_in_msg = message_content_lower.count(keyword)
                            if count_in_msg > 0:
                                scan_data["keyword_counts"][keyword] += count_in_msg
                                scan_data["thread_keyword_counts"][thread.id][keyword] += count_in_msg # Lưu theo thread ID
                                if not is_bot:
                                    scan_data["user_keyword_counts"][author_id][keyword] += count_in_msg

                    # Đếm reactions
                    can_scan_reactions = scan_data.get("can_scan_reactions", False)
                    if can_scan_reactions and message.reactions:
                        try:
                            msg_react_count = 0
                            for reaction in message.reactions:
                                count = reaction.count
                                msg_react_count += count
                                scan_data["reaction_emoji_counts"][str(reaction.emoji)] += count
                            thread_reaction_count += msg_react_count
                            scan_data["overall_total_reaction_count"] += msg_react_count
                            if not is_bot:
                                scan_data["user_reaction_received_counts"][author_id] += msg_react_count
                                user_data['reaction_received_count'] += msg_react_count
                        except Exception as react_err_thread:
                            log.warning(f"Lỗi xử lý reaction msg {message.id} luồng {thread.id}: {react_err_thread}")

                # --- Kết thúc quét tin nhắn luồng ---
                thread_scan_duration = discord.utils.utcnow() - thread_scan_start_time
                log.info(f"      {e('success')} Hoàn thành quét luồng [magenta]'{thread.name}'[/]: {thread_message_count:,} tin nhắn trong [magenta]{utils.format_timedelta(thread_scan_duration)}[/].")
                scan_data["processed_threads_count"] += 1
                current_channel_thread_count += 1
                thread_data_entry["message_count"] = thread_message_count
                thread_data_entry["scan_duration_seconds"] = round(thread_scan_duration.total_seconds(), 2)
                if can_scan_reactions:
                    thread_data_entry["reaction_count"] = thread_reaction_count


            # --- Xử lý lỗi quét luồng ---
            except Exception as e_thread:
                if isinstance(e_thread, discord.Forbidden):
                    error_in_thread = f"Thiếu quyền: {e_thread.text}"
                elif isinstance(e_thread, discord.HTTPException):
                    error_in_thread = f"Lỗi mạng (HTTP {e_thread.status}): {e_thread.text}"
                    await asyncio.sleep(3) # Chờ một chút nếu lỗi mạng
                else:
                    error_in_thread = f"Lỗi không xác định: {e_thread}"

                log.error(f"    {e('error')} {error_in_thread} khi quét luồng '{thread.name}'", exc_info=not isinstance(e_thread, discord.Forbidden))
                scan_errors.append(f"Luồng '{thread.name}' ({thread.id}): {error_in_thread}")
                thread_data_entry["error"] = error_in_thread
                if not thread_processed_flag: # Chỉ tăng skipped nếu chưa xử lý được gì
                    scan_data["skipped_threads_count"] += 1
                    current_channel_skipped_threads += 1

            # --- Lấy thông tin chủ luồng ---
            if thread.owner_id:
                try:
                    owner = await utils.fetch_user_data(server, thread.owner_id, bot_ref=bot)
                    if owner:
                         thread_data_entry["owner_mention"] = owner.mention
                         thread_data_entry["owner_name"] = owner.display_name
                    else:
                         thread_data_entry["owner_mention"] = f"ID: {thread.owner_id}"
                         thread_data_entry["owner_name"] = "(Không tìm thấy)"
                except Exception as owner_err:
                    log.warning(f"Không thể fetch owner luồng {thread.id}: {owner_err}")
                    thread_data_entry["owner_mention"] = f"ID: {thread.owner_id} (Lỗi fetch)"

            # Thêm dữ liệu của thread này vào list
            threads_data_list.append(thread_data_entry)
            await asyncio.sleep(0.1) # Nghỉ nhẹ giữa các luồng

        log.info(f"  {e('success')} Hoàn thành quét {current_channel_thread_count} luồng trong kênh #{parent_channel.name} ({current_channel_skipped_threads} bị bỏ qua).")
        # Cập nhật dữ liệu luồng vào channel_detail_entry
        channel_detail_entry["threads_data"] = threads_data_list

    except Exception as e_outer_thread:
        log.error(f"Lỗi nghiêm trọng khi xử lý luồng cho kênh #{parent_channel.name}: {e_outer_thread}", exc_info=True)
        scan_errors.append(f"Lỗi nghiêm trọng khi xử lý luồng kênh #{parent_channel.name}: {e_outer_thread}")
        # Đảm bảo threads_data là list trống nếu có lỗi lớn
        if "threads_data" not in channel_detail_entry:
            channel_detail_entry["threads_data"] = []


async def _gather_channel_details(
    scan_data: Dict[str, Any],
    channel: Union[discord.TextChannel, discord.VoiceChannel],
    author_counter_channel: Counter,
    channel_message_count: int,
    channel_reaction_count: int,
    channel_scan_duration: datetime.timedelta,
    channel_error: Optional[str]
) -> Dict[str, Any]:
    """Thu thập và cập nhật chi tiết kênh sau khi quét xong tin nhắn."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)

    # Tìm entry chi tiết kênh đã được tạo trong init_scan
    detail_entry = next((d for d in scan_data["channel_details"] if d.get("id") == channel.id), None)
    if not detail_entry:
        # Trường hợp rất hiếm: kênh được quét nhưng không có trong list ban đầu?
        log.error(f"Không tìm thấy detail_entry cho kênh đã quét {channel.id}. Tạo mới.")
        detail_entry = {
            "type": str(channel.type), "name": channel.name, "id": channel.id,
            "created_at": channel.created_at,
            "category": getattr(channel.category, 'name', "N/A"),
            "category_id": getattr(channel.category, 'id', None),
            "threads_data": []
        }
        scan_data["channel_details"].append(detail_entry) # Thêm vào list chính

    # --- Lấy Top Chatter ---
    top_chatter_info = "Không có (hoặc chỉ bot)"
    top_chatter_roles = "N/A"
    if author_counter_channel: # Nếu có user chat trong kênh này
        try:
            top_author_id, top_count = author_counter_channel.most_common(1)[0]
            user = await utils.fetch_user_data(server, top_author_id, bot_ref=bot)
            if user:
                top_chatter_info = f"{user.mention} (`{utils.escape_markdown(user.display_name)}`) - {top_count:,} tin"
                if isinstance(user, discord.Member):
                    # Lấy roles của member (không tính @everyone), sắp xếp
                    member_roles = sorted([r for r in user.roles if not r.is_default()], key=lambda r: r.position, reverse=True)
                    roles_str = ", ".join([r.mention for r in member_roles]) if member_roles else "Không có role"
                    # Giới hạn độ dài roles_str
                    top_chatter_roles = roles_str[:150] + "..." if len(roles_str) > 150 else roles_str
                else:
                    top_chatter_roles = "N/A (Không còn trong server)"
            else:
                top_chatter_info = f"ID: `{top_author_id}` (Không tìm thấy) - {top_count:,} tin"
        except Exception as chatter_err:
            log.error(f"Lỗi lấy top chatter kênh #{channel.name}: {chatter_err}")
            top_chatter_info = f"{e('error')} Lỗi lấy top chatter"

    # --- Lấy Log Tin Nhắn Đầu Tiên ---
    first_messages_log: List[str] = []
    try:
        # Fetch tối đa N+5 tin nhắn đầu tiên để lọc ra N tin nhắn hợp lệ
        async for msg in channel.history(limit=embeds_guild.FIRST_MESSAGES_LIMIT + 5, oldest_first=True):
            author_display = msg.author.display_name if msg.author else "Không rõ"
            timestamp_str = msg.created_at.strftime('%d/%m/%y %H:%M') # Định dạng ngắn gọn
            # Lấy preview nội dung, escape markdown, giới hạn độ dài
            content_preview = (msg.content or "")[:embeds_guild.FIRST_MESSAGES_CONTENT_PREVIEW].replace('`', "'").replace('\n', ' ')
            if len(msg.content or "") > embeds_guild.FIRST_MESSAGES_CONTENT_PREVIEW:
                content_preview += "..."
            elif not content_preview and msg.attachments:
                content_preview = "[File đính kèm]"
            elif not content_preview and msg.embeds:
                content_preview = "[Embed]"
            elif not content_preview and msg.stickers:
                content_preview = "[Sticker]"
            elif not content_preview:
                content_preview = "[Nội dung trống]"

            first_messages_log.append(f"[`{timestamp_str}`] **{utils.escape_markdown(author_display)}**: {utils.escape_markdown(content_preview)}")
            if len(first_messages_log) >= embeds_guild.FIRST_MESSAGES_LIMIT:
                break # Đủ số lượng cần lấy

        if not first_messages_log:
            first_messages_log.append("`[N/A]`" if channel_message_count == 0 else "`[LỖI]` Không thể fetch tin nhắn đầu.")
    except Exception as e_first:
        log.error(f"Lỗi lấy tin nhắn đầu kênh #{channel.name}: {e_first}")
        first_messages_log = [f"`[LỖI]` {e('error')} Lỗi: {e_first}"]
        # Ghi nhận lỗi này vào lỗi chung của kênh
        channel_error = (channel_error + f"\nLỗi lấy tin nhắn đầu: {e_first}").strip() if channel_error else f"Lỗi lấy tin nhắn đầu: {e_first}"


    # --- Lấy các thông tin khác của kênh ---
    channel_topic = "N/A"
    channel_nsfw_str = "N/A"
    channel_slowmode_str = "N/A"

    if isinstance(channel, discord.TextChannel):
        channel_topic = (channel.topic or "Không có")[:150] + ("..." if channel.topic and len(channel.topic) > 150 else "")
        channel_nsfw_str = f"{e('success')} Có" if channel.is_nsfw() else f"{e('error')} Không"
        channel_slowmode_str = f"{channel.slowmode_delay} giây" if channel.slowmode_delay > 0 else "Không"
    elif isinstance(channel, discord.VoiceChannel):
        # Kênh voice cũng có thể NSFW
        channel_nsfw_str = f"{e('success')} Có" if channel.is_nsfw() else f"{e('error')} Không"
        channel_topic = "N/A (Kênh Voice)"
        channel_slowmode_str = "N/A (Kênh Voice)"

    # --- Cập nhật dữ liệu vào detail_entry ---
    update_data = {
        "processed": True,
        "message_count": channel_message_count,
        "duration": channel_scan_duration,
        "reaction_count": channel_reaction_count if scan_data.get("can_scan_reactions", False) else None,
        "topic": channel_topic,
        "nsfw": channel_nsfw_str,
        "slowmode": channel_slowmode_str,
        "top_chatter": top_chatter_info,
        "top_chatter_roles": top_chatter_roles,
        "first_messages_log": first_messages_log,
        "error": channel_error # Cập nhật lỗi (nếu có)
    }
    detail_entry.update(update_data)

    return detail_entry


async def _update_status_message(
    ctx: commands.Context,
    current_status_message: Optional[discord.Message],
    embed: discord.Embed
) -> Optional[discord.Message]:
    """Gửi hoặc sửa tin nhắn trạng thái, trả về message object mới nếu cần."""
    try:
        if current_status_message:
            await current_status_message.edit(content=None, embed=embed)
            return current_status_message # Trả về msg cũ nếu sửa thành công
        else:
            # Nếu chưa có msg hoặc msg cũ bị lỗi/xóa, gửi msg mới
            new_msg = await ctx.send(embed=embed)
            return new_msg
    except (discord.NotFound, discord.HTTPException) as http_err:
        log.warning(f"Cập nhật trạng thái thất bại ({http_err.status}), thử gửi lại.")
        try:
            # Thử gửi lại msg mới
            new_msg = await ctx.send(embed=embed)
            return new_msg
        except Exception as send_new_err:
            log.error(f"Không thể gửi lại tin nhắn trạng thái: {send_new_err}")
            return None # Không thể gửi/sửa, trả về None
    except Exception as e_stat:
        log.error(f"Lỗi không xác định khi cập nhật trạng thái: {e_stat}", exc_info=True)
        return None # Trả về None nếu có lỗi lạ


def _create_progress_embed(
    scan_data: Dict[str, Any],
    current_channel: Union[discord.TextChannel, discord.VoiceChannel],
    current_channel_index: int,
    channel_message_count: int,
    channel_scan_start_time: datetime.datetime,
    now: datetime.datetime
) -> discord.Embed:
    """Tạo embed hiển thị tiến trình quét."""
    bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    total_accessible_channels = len(scan_data["accessible_channels"])

    # Tính toán tiến độ và thời gian ước tính
    progress_percent = ((current_channel_index - 1) / total_accessible_channels) * 100 if total_accessible_channels > 0 else 0
    progress_bar = utils.create_progress_bar(progress_percent)

    channel_elapsed_sec = (now - channel_scan_start_time).total_seconds()
    messages_per_second = (channel_message_count / channel_elapsed_sec) if channel_elapsed_sec > 0.1 else 0

    time_so_far_sec = (now - scan_data["overall_start_time"]).total_seconds()
    # Ước tính thời gian dựa trên thời gian trung bình mỗi kênh đã quét
    avg_time_per_channel = (time_so_far_sec / (current_channel_index - 1)) if current_channel_index > 1 else 60.0 # Giả định 60s nếu là kênh đầu
    estimated_remaining_sec = max(0.0, (total_accessible_channels - (current_channel_index - 1)) * avg_time_per_channel)
    estimated_completion_time = now + datetime.timedelta(seconds=estimated_remaining_sec)

    channel_type_emoji = utils.get_channel_type_emoji(current_channel, bot)
    status_embed = discord.Embed(
        title=f"{e('loading')} Đang Quét: {channel_type_emoji} #{current_channel.name}",
        description=progress_bar,
        color=discord.Color.orange(),
        timestamp=now
    )

    status_embed.add_field(name="Tiến độ Kênh", value=f"{current_channel_index}/{total_accessible_channels}", inline=True)
    status_embed.add_field(name="Tin nhắn (Kênh)", value=f"{channel_message_count:,}", inline=True)
    status_embed.add_field(name="Tốc độ", value=f"~{messages_per_second:.1f} msg/s", inline=True)

    status_embed.add_field(name="Tổng Tin Nhắn", value=f"{scan_data['overall_total_message_count']:,}", inline=True)
    status_embed.add_field(name="Users Phát Hiện", value=f"{len(scan_data['user_activity']):,}", inline=True)
    status_embed.add_field(name="TG Kênh", value=utils.format_timedelta(datetime.timedelta(seconds=channel_elapsed_sec)), inline=True)

    status_embed.add_field(name="TG Ước Tính", value=utils.format_timedelta(datetime.timedelta(seconds=estimated_remaining_sec)), inline=True)
    status_embed.add_field(name="Dự Kiến Xong", value=utils.format_discord_time(estimated_completion_time, 'R'), inline=True)

    if scan_data.get("can_scan_reactions", False):
        status_embed.add_field(name=f"{e('reaction')} Reactions", value=f"{scan_data['overall_total_reaction_count']:,}", inline=True)
    else:
        # Thêm field trống để giữ layout 3 cột
        status_embed.add_field(name="\u200b", value="\u200b", inline=True)

    footer_text = f"Quét toàn bộ | ID Kênh: {current_channel.id}"
    if discord_logging.get_log_target_thread():
        footer_text += " | Log chi tiết trong thread"
    status_embed.set_footer(text=footer_text)

    return status_embed

# --- END OF FILE cogs/deep_scan_helpers/scan_channels.py ---