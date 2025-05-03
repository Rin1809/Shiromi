# --- START OF FILE cogs/deep_scan_helpers/scan_channels.py ---
import discord
from discord.ext import commands
import logging
import asyncio
import time
import datetime
import re
from typing import Dict, Any, List, Union, Optional, Set
from collections import Counter, defaultdict
from typing import Dict, Any, List, Union, Optional, Set, Tuple #

import config
import utils
import discord_logging

log = logging.getLogger(__name__)

# Biểu thức chính quy để tối ưu việc tìm kiếm
URL_REGEX = re.compile(r'https?://\S+')
EMOJI_REGEX = re.compile(r'<a?:([a-zA-Z0-9_]+):([0-9]+)>|([\U00010000-\U0010ffff])')

# --- Hàm quét tin nhắn (được dùng cho cả kênh và luồng) ---
async def _process_message(message: discord.Message, scan_data: Dict[str, Any], location_id: int):
    """Xử lý một tin nhắn, cập nhật scan_data."""
    target_keywords = scan_data["target_keywords"]
    can_scan_reactions = scan_data.get("can_scan_reactions", False)
    server_emojis = scan_data.get("server_emojis_cache", {}) # Lấy từ cache
    server_sticker_ids = scan_data.get("server_sticker_ids_cache", set()) # Lấy từ cache

    timestamp = message.created_at
    if not message.author or message.is_system(): # Bỏ qua tin nhắn hệ thống và webhook (message.author sẽ là None cho webhook?)
        return

    author_id = message.author.id
    is_bot = message.author.bot

    # Cập nhật tổng tin nhắn toàn server
    scan_data["overall_total_message_count"] += 1

    # Cập nhật user_activity
    user_data = scan_data["user_activity"][author_id]
    user_data['message_count'] += 1
    scan_data["user_activity_message_counts"][author_id] = user_data['message_count'] # Cập nhật counter riêng

    if user_data['first_seen'] is None or timestamp < user_data['first_seen']:
        user_data['first_seen'] = timestamp
    if user_data['last_seen'] is None or timestamp > user_data['last_seen']:
        user_data['last_seen'] = timestamp
    if is_bot:
        user_data['is_bot'] = True

    # Lưu kênh/luồng user đã nhắn (dùng cho tính distinct)
    user_data.setdefault('channels_messaged_in', set()).add(location_id)

    # Sử dụng cấu trúc đã định nghĩa: {user_id: {location_id: count}}
    # Đảm bảo rằng defaultdict cấp 2 được tạo nếu chưa có
    scan_data["user_channel_message_counts"][author_id][location_id] += 1
    # *******************************************************

    # --- Phân tích nội dung tin nhắn (chỉ cho user không phải bot) ---
    msg_content = message.content or "" # Đảm bảo có string
    msg_content_lower = msg_content.lower()

    if not is_bot:
        # Đếm link
        link_count = len(URL_REGEX.findall(msg_content))
        scan_data["user_link_counts"][author_id] += link_count
        user_data['link_count'] = user_data.get('link_count', 0) + link_count # +=

        # Đếm ảnh và file khác
        image_count = sum(1 for att in message.attachments if att.content_type and att.content_type.startswith('image/'))
        other_file_count = len(message.attachments) - image_count
        scan_data["user_image_counts"][author_id] += image_count
        user_data['image_count'] = user_data.get('image_count', 0) + image_count # +=
        scan_data.setdefault("user_other_file_counts", Counter())[author_id] += other_file_count
        user_data['other_file_count'] = user_data.get('other_file_count', 0) + other_file_count # +=

        # Đếm emoji trong nội dung
        emoji_matches = EMOJI_REGEX.finditer(msg_content) # Dùng finditer để lấy match object
        emoji_count = 0
        custom_emoji_content_counter_user = scan_data.setdefault("user_custom_emoji_content_counts", defaultdict(Counter))[author_id]
        overall_custom_emoji_counter = scan_data.setdefault("overall_custom_emoji_content_counts", Counter())

        for match in emoji_matches:
            emoji_count += 1
            custom_name, custom_id_str, unicode_emoji = match.groups()
            if custom_id_str: # Là emoji custom
                try:
                    emoji_id = int(custom_id_str)
                    if emoji_id in server_emojis: # Chỉ đếm nếu là emoji của server này
                        custom_emoji_content_counter_user[emoji_id] += 1
                        overall_custom_emoji_counter[emoji_id] += 1
                except ValueError:
                    pass # Bỏ qua ID không hợp lệ

        scan_data["user_emoji_counts"][author_id] += emoji_count
        user_data['emoji_count'] = user_data.get('emoji_count', 0) + emoji_count # +=
        # Cập nhật tổng custom emoji count cho user
        scan_data["user_total_custom_emoji_content_counts"][author_id] = sum(custom_emoji_content_counter_user.values())


        # Đếm sticker
        sticker_count = len(message.stickers)
        scan_data["user_sticker_counts"][author_id] += sticker_count
        user_data['sticker_count'] = user_data.get('sticker_count', 0) + sticker_count # +=
        overall_custom_sticker_counter = scan_data.setdefault("overall_custom_sticker_counts", Counter())
        sticker_usage_counter = scan_data.setdefault("sticker_usage_counts", Counter()) # Lấy hoặc tạo counter

        for sticker_item in message.stickers:
             sticker_id_str = str(sticker_item.id)
             sticker_usage_counter[sticker_id_str] += 1
             # Chỉ đếm custom sticker của server này
             if sticker_item.id in server_sticker_ids:
                  overall_custom_sticker_counter[sticker_item.id] += 1


        # Đếm mention (chỉ user, không bot)
        non_bot_mentions = [m for m in message.mentions if not m.bot]
        if non_bot_mentions:
            mention_given_count = len(non_bot_mentions)
            scan_data["user_mention_given_counts"][author_id] += mention_given_count
            user_data['mention_given_count'] = user_data.get('mention_given_count', 0) + mention_given_count # +=

            # Đếm distinct mentions GIVEN by this user
            distinct_mentioned_ids_in_msg = {m.id for m in non_bot_mentions}
            user_data.setdefault('distinct_mentions_set', set()).update(distinct_mentioned_ids_in_msg)

            # Đếm mentions RECEIVED by mentioned users
            user_mention_received_counter = scan_data.setdefault("user_mention_received_counts", Counter())
            for mentioned_user in non_bot_mentions:
                mentioned_user_id = mentioned_user.id
                user_mention_received_counter[mentioned_user_id] += 1
                scan_data["user_activity"][mentioned_user_id]['mention_received_count'] = user_mention_received_counter[mentioned_user_id]


        # Đếm reply
        if message.reference and message.reference.message_id:
            scan_data["user_reply_counts"][author_id] += 1
            user_data['reply_count'] = user_data.get('reply_count', 0) + 1 # +=

    # --- Đếm keywords (nếu có) ---
    if target_keywords and msg_content_lower: # Đã lấy msg_content_lower ở trên
        keyword_counter = scan_data.setdefault("keyword_counts", Counter())
        channel_kw_counter = scan_data.setdefault("channel_keyword_counts", defaultdict(Counter))
        thread_kw_counter = scan_data.setdefault("thread_keyword_counts", defaultdict(Counter))
        user_kw_counter = scan_data.setdefault("user_keyword_counts", defaultdict(Counter))
        for keyword in target_keywords:
            count_in_msg = msg_content_lower.count(keyword)
            if count_in_msg > 0:
                keyword_counter[keyword] += count_in_msg
                if isinstance(message.channel, discord.Thread):
                    thread_kw_counter[location_id][keyword] += count_in_msg
                else:
                    channel_kw_counter[location_id][keyword] += count_in_msg
                if not is_bot: # Chỉ đếm keyword cho user
                    user_kw_counter[author_id][keyword] += count_in_msg

    # --- Đếm reactions (nếu bật và có quyền) ---
    if can_scan_reactions and message.reactions:
        try:
            msg_react_count = 0
            filtered_reaction_counter = scan_data.setdefault("filtered_reaction_emoji_counts", Counter())
            reaction_total_counter = scan_data.setdefault("reaction_emoji_counts", Counter()) # Counter tổng

            for reaction in message.reactions:
                count = reaction.count
                if count <= 0: continue # Bỏ qua nếu count không hợp lệ
                msg_react_count += count

                # Lọc emoji reaction
                is_custom_server_emoji = False
                emoji_key: Union[int, str] = str(reaction.emoji) # Mặc định key là unicode string

                if reaction.custom_emoji and reaction.emoji.id in server_emojis: # Chỉ cần check ID trong cache
                    is_custom_server_emoji = True
                    emoji_key = reaction.emoji.id # Dùng ID làm key cho custom emoji server

                is_allowed_unicode = not reaction.custom_emoji and str(reaction.emoji) in config.REACTION_UNICODE_EXCEPTIONS

                if is_custom_server_emoji or is_allowed_unicode:
                    filtered_reaction_counter[emoji_key] += count # Key là ID (int) hoặc Unicode (str)

                # Đếm tổng reaction không lọc
                reaction_total_counter[str(reaction.emoji)] += count

            scan_data["overall_total_reaction_count"] = scan_data.get("overall_total_reaction_count", 0) + msg_react_count
            # Đếm reaction nhận được (chỉ cho user)
            if not is_bot:
                user_react_received_counter = scan_data.setdefault("user_reaction_received_counts", Counter())
                user_react_received_counter[author_id] += msg_react_count
                user_data['reaction_received_count'] = user_react_received_counter[author_id]
        except Exception as react_err:
            log.warning(f"Lỗi xử lý reaction msg {message.id} location {location_id}: {react_err}")


# --- Các hàm quét kênh/luồng ---

async def scan_all_channels_and_threads(scan_data: Dict[str, Any]):
    """Quét tất cả các kênh và luồng có thể truy cập."""
    accessible_channels: List[Union[discord.TextChannel, discord.VoiceChannel]] = scan_data["accessible_channels"]
    total_accessible_channels = len(accessible_channels)
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)

    log.info(f"Bắt đầu quét {total_accessible_channels} kênh...")

    last_status_update_time = scan_data["overall_start_time"]
    update_interval_seconds = 12
    status_message = scan_data["initial_status_msg"]

    processed_channels_count = 0
    processed_threads_count = 0

    for current_channel_index, channel in enumerate(accessible_channels, 1):
        channel_message_count = 0
        channel_scan_start_time = discord.utils.utcnow()
        channel_processed_flag = False
        channel_error: Optional[str] = None
        author_counter_channel: Counter = Counter()
        channel_threads_data: List[Dict[str, Any]] = []

        channel_type_emoji = utils.get_channel_type_emoji(channel, bot)
        channel_type_name = "Voice" if isinstance(channel, discord.VoiceChannel) else "Text"

        log.info(f"[bold]({current_channel_index}/{total_accessible_channels})[/bold] Đang quét kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/] ({channel.id})")

        channel_status_embed = discord.Embed(
            title=f"{e('loading')} Bắt đầu quét: {channel_type_emoji} #{channel.name}",
            description=f"Kênh {current_channel_index}/{total_accessible_channels}\nĐang đọc lịch sử...",
            color=discord.Color.yellow()
        )
        status_message = await _update_status_message(scan_data["ctx"], status_message, channel_status_embed)
        scan_data["status_message"] = status_message

        try:
            message_iterator = channel.history(limit=None) # Giới hạn None để lấy hết
            async for message in message_iterator:
                await _process_message(message, scan_data, channel.id)
                channel_message_count += 1
                channel_processed_flag = True # Đánh dấu đã xử lý ít nhất 1 tin
                if message.author and not message.author.bot:
                    author_counter_channel[message.author.id] += 1

                now = discord.utils.utcnow()
                if (now - last_status_update_time).total_seconds() > update_interval_seconds:
                    status_embed = _create_progress_embed(scan_data, channel, current_channel_index, channel_message_count, channel_scan_start_time, now)
                    status_message = await _update_status_message(scan_data["ctx"], status_message, status_embed)
                    scan_data["status_message"] = status_message
                    last_status_update_time = now

            channel_scan_duration = discord.utils.utcnow() - channel_scan_start_time
            log.info(f"  {e('success')} Hoàn thành kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/]: {channel_message_count:,} tin nhắn trong [magenta]{utils.format_timedelta(channel_scan_duration)}[/].")

            # --- Thu thập chi tiết kênh SAU KHI QUÉT XONG ---
            # Tìm detail_entry tương ứng đã được tạo trong init_scan
            detail_entry = next((item for item in scan_data["channel_details"] if item.get('id') == channel.id), None)

            if detail_entry:
                # Cập nhật thông tin vào entry đã có
                log.info(f"  {e('info')} Đang cập nhật chi tiết kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/]...")
                await _update_channel_details_after_scan(
                    scan_data, channel, detail_entry, author_counter_channel,
                    channel_message_count, channel_scan_duration, channel_error
                )
                # Đánh dấu đã xử lý thành công
                detail_entry["processed"] = True
                processed_channels_count += 1 # Chỉ tăng nếu xử lý thành công
            else:
                 log.error(f"  {e('error')} Không tìm thấy detail_entry cho kênh {channel.id} để cập nhật chi tiết.")
                 # Vẫn cần xử lý thread nếu là kênh text
                 detail_entry = {} # Tạo dict rỗng để truyền vào scan thread nếu cần


            # --- Quét luồng ---
            if isinstance(channel, discord.TextChannel):
                 # Truyền detail_entry vào để hàm scan thread cập nhật trực tiếp
                 threads_scanned_count, threads_skipped_count = await _scan_threads_in_channel(scan_data, channel, detail_entry)
                 processed_threads_count += threads_scanned_count # Cộng số thread đã xử lý thành công
            else:
                 log.info(f"  {e('thread')} Bỏ qua quét luồng cho kênh voice {channel_type_emoji} [cyan]#{channel.name}[/].")


            log.info(f"  {e('success')} Hoàn thành xử lý kênh và luồng (nếu có) cho {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/].")
            await asyncio.sleep(0.1) # Delay nhẹ giữa các kênh

        except Exception as e_channel:
             channel_error_msg = f"Lỗi kênh {channel_type_name} #{channel.name}: {e_channel}"
             log.error(f"{utils.get_emoji('error', bot)} {channel_error_msg}", exc_info=not isinstance(e_channel, discord.Forbidden))
             scan_data["scan_errors"].append(channel_error_msg)

             # Cập nhật lỗi vào detail_entry nếu tìm thấy
             detail_entry = next((item for item in scan_data["channel_details"] if item.get('id') == channel.id), None)
             if detail_entry:
                 existing_error = str(detail_entry.get("error") or "")
                 error_prefix = "FATAL SCAN ERROR: " if not channel_processed_flag else "PARTIAL SCAN ERROR: "
                 full_error = f"{error_prefix}{channel_error_msg}"
                 detail_entry["error"] = (existing_error + f"\n{full_error}").strip()
                 detail_entry["processed"] = channel_processed_flag # Đánh dấu xử lý lỗi
                 # KHÔNG tăng processed_channels_count nếu lỗi
             else:
                  log.error(f"  {e('error')} Không tìm thấy detail_entry cho kênh lỗi {channel.id} để ghi lỗi.")

             # Thử thông báo lỗi vào kênh context
             try:
                 await scan_data["ctx"].send(f"{utils.get_emoji('error', bot)} {channel_error_msg}. Dữ liệu kênh #{channel.name} có thể không đầy đủ.", delete_after=30)
             except Exception: pass
             await asyncio.sleep(2) # Delay sau lỗi kênh

    # Cập nhật số lượng tổng cuối cùng vào scan_data
    scan_data["processed_channels_count"] = processed_channels_count
    scan_data["processed_threads_count"] = processed_threads_count
    # skipped_channels_count đã được tính trong init_scan và cập nhật nếu có lỗi thread không thể quét

    log.info("Hoàn thành quét tất cả các kênh và luồng.")


async def _scan_threads_in_channel(
    scan_data: Dict[str, Any],
    parent_channel: discord.TextChannel,
    channel_detail_entry: Dict[str, Any] # Nhận detail_entry để cập nhật
) -> Tuple[int, int]: # Trả về số thread quét thành công và số thread bỏ qua
    """Quét tất cả các luồng (active và archived) trong một kênh text."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    can_scan_archived = scan_data.get("can_scan_archived_threads", False)
    scan_errors: List[str] = scan_data["scan_errors"]

    threads_scanned_ok = 0
    threads_skipped = 0

    log.info(f"  {e('thread')} Đang kiểm tra luồng trong kênh text {e('text_channel')} [cyan]#{parent_channel.name}[/]...")

    threads_to_scan: List[discord.Thread] = []
    try:
        # Lấy active threads
        threads_to_scan.extend(parent_channel.threads)

        # Lấy archived threads nếu có quyền
        if can_scan_archived:
            log.info(f"    Đang fetch luồng lưu trữ...")
            try:
                async for thread in parent_channel.archived_threads(limit=None):
                    threads_to_scan.append(thread)
                log.info(f"    Fetch xong luồng lưu trữ.")
            except discord.Forbidden:
                 log.warning(f"    Thiếu quyền fetch luồng lưu trữ cho kênh #{parent_channel.name}.")
                 scan_errors.append(f"Thiếu quyền fetch luồng lưu trữ kênh #{parent_channel.name}.")
            except discord.HTTPException as arch_http_err:
                 log.error(f"    Lỗi HTTP khi fetch luồng lưu trữ kênh #{parent_channel.name}: {arch_http_err.status}")
                 scan_errors.append(f"Lỗi HTTP fetch luồng lưu trữ kênh #{parent_channel.name}.")
            except Exception as arch_err:
                 log.error(f"    Lỗi không xác định khi fetch luồng lưu trữ kênh #{parent_channel.name}: {arch_err}", exc_info=True)
                 scan_errors.append(f"Lỗi fetch luồng lưu trữ kênh #{parent_channel.name}.")
        else:
            log.info("    Bỏ qua luồng lưu trữ do thiếu quyền.")

        # Loại bỏ trùng lặp (nếu có) và tạo list duy nhất
        unique_threads_map: Dict[int, discord.Thread] = {t.id: t for t in threads_to_scan}
        unique_threads_to_scan = list(unique_threads_map.values())
        total_threads_found = len(unique_threads_to_scan)

        if total_threads_found == 0:
            log.info(f"  {e('info')} Không tìm thấy luồng trong kênh text [cyan]#{parent_channel.name}[/].")
            channel_detail_entry["threads_data"] = [] # Đảm bảo list rỗng
            return 0, 0 # Trả về 0, 0

        log.info(f"  Tìm thấy {total_threads_found} luồng. Bắt đầu quét...")
        threads_data_list = [] # List để lưu dữ liệu các thread của kênh này

        for thread_index, thread in enumerate(unique_threads_to_scan, 1):
            thread_message_count = 0
            thread_scan_start_time = discord.utils.utcnow()
            thread_processed_flag = False
            error_in_thread: Optional[str] = None
            thread_data_entry: Dict[str, Any] = {
                 "id": thread.id, "name": thread.name, "archived": thread.archived,
                 "locked": thread.locked,
                 "created_at": thread.created_at.isoformat() if thread.created_at else None,
                 "owner_id": thread.owner_id, "owner_mention": "N/A", "owner_name": "N/A",
                 "message_count": 0, "reaction_count": None, # Bỏ reaction count riêng
                 "scan_duration_seconds": 0, "error": None
            }

            # Kiểm tra quyền trước khi quét
            try:
                thread_perms = thread.permissions_for(server.me)
                if not thread_perms.view_channel or not thread_perms.read_message_history:
                    reason = "Thiếu View" if not thread_perms.view_channel else "Thiếu Read History"
                    log.warning(f"    Bỏ qua luồng '{thread.name}' ({thread.id}): {reason}.")
                    scan_errors.append(f"Luồng '{thread.name}' ({thread.id}): Bỏ qua ({reason}).")
                    scan_data["skipped_threads_count"] = scan_data.get("skipped_threads_count", 0) + 1
                    threads_skipped += 1
                    thread_data_entry["error"] = f"Bỏ qua do {reason}"
                    threads_data_list.append(thread_data_entry) # Vẫn thêm entry lỗi
                    continue # Sang thread tiếp theo
            except Exception as thread_perm_err:
                log.error(f"    {e('error')} Lỗi kiểm tra quyền luồng '{thread.name}': {thread_perm_err}", exc_info=True)
                scan_errors.append(f"Luồng '{thread.name}': Lỗi kiểm tra quyền.")
                scan_data["skipped_threads_count"] = scan_data.get("skipped_threads_count", 0) + 1
                threads_skipped += 1
                thread_data_entry["error"] = f"Lỗi kiểm tra quyền: {thread_perm_err}"
                threads_data_list.append(thread_data_entry)
                continue

            log.info(f"    [bold]({thread_index}/{total_threads_found})[/bold] Đang quét luồng [magenta]'{thread.name}'[/] ({thread.id})...")

            # Quét tin nhắn trong luồng
            try:
                thread_message_iterator = thread.history(limit=None)
                async for message in thread_message_iterator:
                    await _process_message(message, scan_data, thread.id)
                    thread_message_count += 1
                    thread_processed_flag = True

                thread_scan_duration = discord.utils.utcnow() - thread_scan_start_time
                log.info(f"      {e('success')} Hoàn thành quét luồng [magenta]'{thread.name}'[/]: {thread_message_count:,} tin nhắn trong [magenta]{utils.format_timedelta(thread_scan_duration)}[/].")
                # scan_data["processed_threads_count"] += 1 # Đã chuyển ra ngoài vòng lặp kênh
                threads_scanned_ok += 1
                thread_data_entry["message_count"] = thread_message_count
                thread_data_entry["scan_duration_seconds"] = round(thread_scan_duration.total_seconds(), 2)

            except discord.Forbidden as e_forbidden:
                 error_in_thread = f"Thiếu quyền: {e_forbidden.text}"
            except discord.HTTPException as e_http:
                 error_in_thread = f"Lỗi mạng (HTTP {e_http.status}): {e_http.text}"; await asyncio.sleep(3)
            except Exception as e_thread:
                 error_in_thread = f"Lỗi không xác định: {e_thread}"

            if error_in_thread:
                 log.error(f"    {e('error')} {error_in_thread} khi quét luồng '{thread.name}'", exc_info=not isinstance(error_in_thread, (discord.Forbidden, discord.HTTPException)))
                 scan_errors.append(f"Luồng '{thread.name}' ({thread.id}): {error_in_thread}")
                 thread_data_entry["error"] = error_in_thread
                 if not thread_processed_flag:
                     scan_data["skipped_threads_count"] = scan_data.get("skipped_threads_count", 0) + 1
                     threads_skipped += 1

            # Fetch thông tin owner (giữ nguyên)
            if thread.owner_id:
                try:
                    # Dùng cache từ utils.fetch_user_data
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

            threads_data_list.append(thread_data_entry) # Thêm dữ liệu thread vào list của kênh này
            await asyncio.sleep(0.05) # Delay nhẹ giữa các thread

        log.info(f"  {e('success')} Hoàn thành quét {threads_scanned_ok} luồng trong kênh #{parent_channel.name} ({threads_skipped} bị bỏ qua).")
        # Cập nhật list dữ liệu thread vào entry của kênh cha
        channel_detail_entry["threads_data"] = threads_data_list

    except Exception as e_outer_thread:
        log.error(f"Lỗi nghiêm trọng khi xử lý luồng cho kênh #{parent_channel.name}: {e_outer_thread}", exc_info=True)
        scan_errors.append(f"Lỗi nghiêm trọng khi xử lý luồng kênh #{parent_channel.name}: {e_outer_thread}")
        # Đảm bảo threads_data là list rỗng nếu có lỗi ngoài cùng
        if "threads_data" not in channel_detail_entry:
            channel_detail_entry["threads_data"] = []

    return threads_scanned_ok, threads_skipped


# --- ĐỔI TÊN hàm _gather_channel_details thành _update_channel_details_after_scan ---
async def _update_channel_details_after_scan(
    scan_data: Dict[str, Any],
    channel: Union[discord.TextChannel, discord.VoiceChannel],
    detail_entry: Dict[str, Any], # Nhận entry đã tồn tại
    author_counter_channel: Counter,
    channel_message_count: int,
    channel_scan_duration: datetime.timedelta,
    channel_error: Optional[str]
):
    """Cập nhật chi tiết kênh vào detail_entry đã có sau khi quét xong tin nhắn."""
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)

    # Lấy top chatter (giữ nguyên logic)
    top_chatter_info = "Không có (hoặc chỉ bot)"
    top_chatter_roles = "N/A"
    if author_counter_channel:
        try:
            top_author_id, top_count = author_counter_channel.most_common(1)[0]
            # Sử dụng cache từ utils.fetch_user_data
            user = await utils.fetch_user_data(server, top_author_id, bot_ref=bot)
            if user:
                top_chatter_info = f"{user.mention} (`{utils.escape_markdown(user.display_name)}`) - {top_count:,} tin"
                if isinstance(user, discord.Member):
                    member_roles = sorted([r for r in user.roles if not r.is_default()], key=lambda r: r.position, reverse=True)
                    roles_str = ", ".join([r.mention for r in member_roles]) if member_roles else "Không có role"
                    top_chatter_roles = roles_str[:150] + "..." if len(roles_str) > 150 else roles_str
                else:
                    top_chatter_roles = "N/A (Không còn trong server)"
            else:
                top_chatter_info = f"ID: `{top_author_id}` (Không tìm thấy) - {top_count:,} tin"
        except Exception as chatter_err:
            log.error(f"Lỗi lấy top chatter kênh #{channel.name}: {chatter_err}")
            top_chatter_info = f"{e('error')} Lỗi lấy top chatter"

    # Lấy tin nhắn đầu (giữ nguyên logic)
    first_messages_log: List[str] = []
    first_messages_limit = 5 # Giảm số lượng lấy cho hiệu quả hơn
    first_messages_preview = 80
    try:
        # Dùng oldest_first=True để lấy tin nhắn đầu tiên hiệu quả hơn
        async for msg in channel.history(limit=first_messages_limit, oldest_first=True):
            author_display = msg.author.display_name if msg.author else "Không rõ"
            timestamp_str = msg.created_at.strftime('%d/%m/%y %H:%M')
            content_preview = (msg.content or "")[:first_messages_preview].replace('`', "'").replace('\n', ' ')
            if len(msg.content or "") > first_messages_preview: content_preview += "..."
            elif not content_preview and msg.attachments: content_preview = "[File đính kèm]"
            elif not content_preview and msg.embeds: content_preview = "[Embed]"
            elif not content_preview and msg.stickers: content_preview = "[Sticker]"
            elif not content_preview: content_preview = "[Nội dung trống]"
            first_messages_log.append(f"[`{timestamp_str}`] **{utils.escape_markdown(author_display)}**: {utils.escape_markdown(content_preview)}")
            # Không cần break vì limit đã giới hạn

        if not first_messages_log and channel_message_count == 0:
             first_messages_log.append("`[Không có tin nhắn]`")
        elif not first_messages_log and channel_message_count > 0:
             first_messages_log.append("`[LỖI]` Không thể fetch tin nhắn đầu.")
    except Exception as e_first:
        log.error(f"Lỗi lấy tin nhắn đầu kênh #{channel.name}: {e_first}")
        first_messages_log = [f"`[LỖI]` {e('error')} Lỗi: {e_first}"]
        # Cập nhật lỗi vào channel_error nếu có
        channel_error = (channel_error + f"\nLỗi lấy tin nhắn đầu: {e_first}").strip() if channel_error else f"Lỗi lấy tin nhắn đầu: {e_first}"

    # Lấy thông tin kênh (topic, nsfw, slowmode)
    channel_topic = "N/A"
    channel_nsfw_str = "N/A"
    channel_slowmode_str = "N/A"
    if isinstance(channel, discord.TextChannel):
        channel_topic = (channel.topic or "Không có")[:150] + ("..." if channel.topic and len(channel.topic) > 150 else "")
        channel_nsfw_str = f"{e('success')} Có" if channel.is_nsfw() else f"{e('error')} Không"
        channel_slowmode_str = f"{channel.slowmode_delay} giây" if channel.slowmode_delay > 0 else "Không"
    elif isinstance(channel, discord.VoiceChannel):
        channel_nsfw_str = f"{e('success')} Có" if channel.is_nsfw() else f"{e('error')} Không"
        channel_topic = "N/A (Kênh Voice)"
        channel_slowmode_str = "N/A (Kênh Voice)"

    # Tính toán reaction count (tổng thể, không theo kênh)
    filtered_channel_reaction_count = None # Không thể tính chính xác reaction cho kênh này
    if config.ENABLE_REACTION_SCAN:
        # Lấy tổng filtered reaction count từ scan_data làm giá trị tham khảo
        filtered_channel_reaction_count = sum(scan_data.get("filtered_reaction_emoji_counts", Counter()).values())


    # --- Cập nhật detail_entry đã có ---
    update_data = {
        # "processed": True, # Đã đánh dấu ở nơi gọi
        "message_count": channel_message_count,
        "reaction_count": filtered_channel_reaction_count, # Lưu số reaction đã lọc (tổng thể)
        "duration_seconds": round(channel_scan_duration.total_seconds(), 2),
        "topic": channel_topic,
        "nsfw": channel_nsfw_str,
        "slowmode": channel_slowmode_str,
        "top_chatter": top_chatter_info,
        "top_chatter_roles": top_chatter_roles,
        "first_messages_log": first_messages_log,
        "error": channel_error # Cập nhật lỗi nếu có
    }
    # detail_entry.pop('duration', None) # Xóa key cũ nếu có
    detail_entry.update(update_data)


# --- Hàm cập nhật và tạo embed tiến trình (Giữ nguyên logic) ---
async def _update_status_message(
    ctx: commands.Context,
    current_status_message: Optional[discord.Message],
    embed: discord.Embed
) -> Optional[discord.Message]:
    """Gửi hoặc sửa tin nhắn trạng thái, trả về message object mới nếu cần."""
    try:
        if current_status_message:
            await current_status_message.edit(content=None, embed=embed)
            return current_status_message
        else:
            # Nếu không có tin nhắn cũ, thử gửi tin nhắn mới
            new_msg = await ctx.send(embed=embed)
            return new_msg
    except (discord.NotFound, discord.HTTPException) as http_err:
        log.warning(f"Cập nhật trạng thái thất bại ({http_err.status}), thử gửi lại.")
        try:
            # Gửi lại tin nhắn mới nếu sửa thất bại
            new_msg = await ctx.send(embed=embed)
            return new_msg
        except Exception as send_new_err:
            log.error(f"Không thể gửi lại tin nhắn trạng thái: {send_new_err}")
            return None # Trả về None nếu không thể gửi/sửa
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
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    total_accessible_channels = len(scan_data["accessible_channels"])

    progress_percent = ((current_channel_index - 1) / total_accessible_channels) * 100 if total_accessible_channels > 0 else 0
    progress_bar = utils.create_progress_bar(progress_percent)

    channel_elapsed_sec = (now - channel_scan_start_time).total_seconds()
    messages_per_second = (channel_message_count / channel_elapsed_sec) if channel_elapsed_sec > 0.1 else 0

    time_so_far_sec = (now - scan_data["overall_start_time"]).total_seconds()
    # Ước tính thời gian còn lại dựa trên thời gian trung bình mỗi kênh
    # Tránh chia cho 0 nếu là kênh đầu tiên
    avg_time_per_channel = (time_so_far_sec / (current_channel_index - 1)) if current_channel_index > 1 else 60.0 # Ước tính 60s cho kênh đầu
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
    status_embed.add_field(name="Tốc độ (Kênh)", value=f"~{messages_per_second:.1f} msg/s", inline=True)

    # Hiển thị tổng tin nhắn đã quét được từ scan_data
    overall_msgs = scan_data.get('overall_total_message_count', 0)
    status_embed.add_field(name="Tổng Tin Nhắn", value=f"{overall_msgs:,}", inline=True)
    # Hiển thị số users đã phát hiện
    users_detected = len(scan_data['user_activity'])
    status_embed.add_field(name="Users Phát Hiện", value=f"{users_detected:,}", inline=True)
    status_embed.add_field(name="TG Kênh", value=utils.format_timedelta(datetime.timedelta(seconds=channel_elapsed_sec)), inline=True)

    # Thời gian ước tính
    status_embed.add_field(name="TG Ước Tính", value=utils.format_timedelta(datetime.timedelta(seconds=estimated_remaining_sec)), inline=True)
    status_embed.add_field(name="Dự Kiến Xong", value=utils.format_discord_time(estimated_completion_time, 'R'), inline=True)

    # Hiển thị tổng reaction đã lọc nếu bật
    if scan_data.get("can_scan_reactions", False):
        filtered_reaction_count = sum(scan_data.get("filtered_reaction_emoji_counts", Counter()).values())
        status_embed.add_field(name=f"{e('reaction')} React (Lọc)", value=f"{filtered_reaction_count:,}", inline=True)
    else:
        # Thêm field trống để giữ layout 3 cột
        status_embed.add_field(name="\u200b", value="\u200b", inline=True)


    footer_text = f"Quét toàn bộ | ID Kênh: {current_channel.id}"
    if discord_logging.get_log_target_thread():
        footer_text += " | Log chi tiết trong thread"
    status_embed.set_footer(text=footer_text)

    return status_embed

# --- END OF FILE cogs/deep_scan_helpers/scan_channels.py ---