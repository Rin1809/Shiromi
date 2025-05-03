# --- START OF FILE cogs/deep_scan_helpers/scan_channels.py ---
import discord
from discord.ext import commands
import logging
import asyncio
import time
import datetime
import re
from typing import Dict, Any, List, Union, Optional, Set, Tuple
from collections import Counter, defaultdict # Đảm bảo defaultdict được import
# from typing import Dict, Any, List, Union, Optional, Set, Tuple # (Đã có ở trên)

import config
import utils
import discord_logging
# from reporting import embeds_guild # Bỏ import nếu không dùng hằng số từ đây nữa

log = logging.getLogger(__name__)

# Biểu thức chính quy để tối ưu việc tìm kiếm
URL_REGEX = re.compile(r'https?://\S+')
EMOJI_REGEX = re.compile(r'<a?:([a-zA-Z0-9_]+):([0-9]+)>|([\U00010000-\U0010ffff])') # Capture cả group unicode

# --- Hàm quét tin nhắn (được dùng cho cả kênh và luồng) ---
async def _process_message(message: discord.Message, scan_data: Dict[str, Any], location_id: int):
    """Xử lý một tin nhắn, cập nhật scan_data."""
    target_keywords = scan_data["target_keywords"]
    can_scan_reactions = scan_data.get("can_scan_reactions", False)
    server_emojis = scan_data.get("server_emojis_cache", {}) # Lấy từ cache
    server_sticker_ids = scan_data.get("server_sticker_ids_cache", set()) # Lấy từ cache

    timestamp = message.created_at
    if not message.author or message.is_system(): # Bỏ qua tin nhắn hệ thống và webhook
        return

    author_id = message.author.id
    is_bot = message.author.bot

    # Cập nhật tổng tin nhắn toàn server
    # Kiểm tra xem key đã tồn tại chưa trước khi tăng
    if "overall_total_message_count" not in scan_data:
        scan_data["overall_total_message_count"] = 0 # Khởi tạo nếu chưa có
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

    # *** THÊM: Đếm tin nhắn cho user trong kênh/luồng này ***
    scan_data["user_channel_message_counts"][author_id][location_id] += 1

    # --- Phân tích nội dung tin nhắn (chỉ cho user không phải bot) ---
    msg_content = message.content or ""
    msg_content_lower = msg_content.lower()

    if not is_bot:
        # Đếm link
        link_count = len(URL_REGEX.findall(msg_content))
        scan_data["user_link_counts"][author_id] += link_count
        user_data['link_count'] = user_data.get('link_count', 0) + link_count

        # Đếm ảnh và file khác
        image_count = sum(1 for att in message.attachments if att.content_type and att.content_type.startswith('image/'))
        other_file_count = len(message.attachments) - image_count
        scan_data["user_image_counts"][author_id] += image_count
        user_data['image_count'] = user_data.get('image_count', 0) + image_count
        scan_data.setdefault("user_other_file_counts", Counter())[author_id] += other_file_count
        user_data['other_file_count'] = user_data.get('other_file_count', 0) + other_file_count

        # Đếm emoji trong nội dung
        emoji_matches = EMOJI_REGEX.finditer(msg_content)
        emoji_count = 0
        custom_emoji_content_counter_user = scan_data.setdefault("user_custom_emoji_content_counts", defaultdict(Counter))[author_id]
        overall_custom_emoji_counter = scan_data.setdefault("overall_custom_emoji_content_counts", Counter())

        for match in emoji_matches:
            emoji_count += 1
            custom_name, custom_id_str, unicode_emoji = match.groups()
            if custom_id_str:
                try:
                    emoji_id = int(custom_id_str)
                    if emoji_id in server_emojis:
                        custom_emoji_content_counter_user[emoji_id] += 1
                        overall_custom_emoji_counter[emoji_id] += 1
                except ValueError:
                    pass

        scan_data["user_emoji_counts"][author_id] += emoji_count
        user_data['emoji_count'] = user_data.get('emoji_count', 0) + emoji_count
        scan_data["user_total_custom_emoji_content_counts"][author_id] = sum(custom_emoji_content_counter_user.values())

        # Đếm sticker
        sticker_count = len(message.stickers)
        scan_data["user_sticker_counts"][author_id] += sticker_count
        user_data['sticker_count'] = user_data.get('sticker_count', 0) + sticker_count
        overall_custom_sticker_counter = scan_data.setdefault("overall_custom_sticker_counts", Counter())
        sticker_usage_counter = scan_data.setdefault("sticker_usage_counts", Counter())

        for sticker_item in message.stickers:
             sticker_id_str = str(sticker_item.id)
             sticker_usage_counter[sticker_id_str] += 1
             if sticker_item.id in server_sticker_ids:
                  overall_custom_sticker_counter[sticker_item.id] += 1

        # Đếm mention (chỉ user, không bot)
        non_bot_mentions = [m for m in message.mentions if not m.bot]
        if non_bot_mentions:
            mention_given_count = len(non_bot_mentions)
            scan_data["user_mention_given_counts"][author_id] += mention_given_count
            user_data['mention_given_count'] = user_data.get('mention_given_count', 0) + mention_given_count

            distinct_mentioned_ids_in_msg = {m.id for m in non_bot_mentions}
            user_data.setdefault('distinct_mentions_set', set()).update(distinct_mentioned_ids_in_msg)

            user_mention_received_counter = scan_data.setdefault("user_mention_received_counts", Counter())
            for mentioned_user in non_bot_mentions:
                mentioned_user_id = mentioned_user.id
                user_mention_received_counter[mentioned_user_id] += 1
                scan_data["user_activity"][mentioned_user_id]['mention_received_count'] = user_mention_received_counter[mentioned_user_id]

        # Đếm reply
        if message.reference and message.reference.message_id:
            scan_data["user_reply_counts"][author_id] += 1
            user_data['reply_count'] = user_data.get('reply_count', 0) + 1

    # --- Đếm keywords (nếu có) ---
    if target_keywords and msg_content_lower:
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
                if not is_bot:
                    user_kw_counter[author_id][keyword] += count_in_msg

    # --- Đếm reactions (nếu bật và có quyền) ---
    if can_scan_reactions and message.reactions:
        try:
            msg_react_count = 0
            filtered_reaction_counter = scan_data.setdefault("filtered_reaction_emoji_counts", Counter())
            reaction_total_counter = scan_data.setdefault("reaction_emoji_counts", Counter()) # Counter tổng

            for reaction in message.reactions: # Lặp qua từng reaction object
                count = reaction.count
                if count <= 0: continue # Bỏ qua nếu count không hợp lệ
                msg_react_count += count

                emoji = reaction.emoji # Lấy đối tượng emoji/unicode string từ reaction
                emoji_key_for_filtered: Optional[Union[int, str]] = None # Key dùng cho counter đã lọc
                is_custom_server_emoji = False
                is_allowed_unicode = False

                # ----- SỬA LOGIC KIỂM TRA Ở ĐÂY -----
                if isinstance(emoji, discord.Emoji):
                    # Đây là custom emoji mà bot nhận diện đầy đủ
                    if emoji.id in server_emojis: # Kiểm tra xem có phải của server này không
                        is_custom_server_emoji = True
                        emoji_key_for_filtered = emoji.id # Dùng ID làm key
                # elif isinstance(emoji, discord.PartialEmoji): # Có thể là Unicode hoặc custom emoji lạ
                    # Nếu muốn xử lý PartialEmoji riêng, thêm logic ở đây
                    # pass
                else: # Trường hợp còn lại, coi như là Unicode (có thể là str hoặc PartialEmoji)
                    emoji_str = str(emoji) # Lấy dạng string của emoji
                    if emoji_str in config.REACTION_UNICODE_EXCEPTIONS:
                        is_allowed_unicode = True
                        emoji_key_for_filtered = emoji_str # Dùng ký tự unicode làm key
                # ----- KẾT THÚC SỬA LOGIC -----

                # Thêm vào counter đã lọc nếu thỏa mãn điều kiện
                if is_custom_server_emoji or is_allowed_unicode:
                    if emoji_key_for_filtered is not None:
                        filtered_reaction_counter[emoji_key_for_filtered] += count

                # Luôn thêm vào counter tổng (dùng string để nhất quán key)
                reaction_total_counter[str(emoji)] += count

            # Cập nhật tổng reaction count (có thể cần key này ở chỗ khác)
            if "overall_total_reaction_count" not in scan_data:
                scan_data["overall_total_reaction_count"] = 0
            scan_data["overall_total_reaction_count"] += msg_react_count

            # Đếm reaction nhận được (chỉ cho user)
            if not is_bot:
                user_react_received_counter = scan_data.setdefault("user_reaction_received_counts", Counter())
                user_react_received_counter[author_id] += msg_react_count
                user_data['reaction_received_count'] = user_react_received_counter[author_id]

        # ----- THÊM XỬ LÝ AttributeError CỤ THỂ -----
        except AttributeError as attr_err:
            # Log lỗi cụ thể hơn nếu vẫn xảy ra (ít khả năng sau khi sửa)
            log_emoji_info = "N/A"
            if 'reaction' in locals() and hasattr(reaction, 'emoji'):
                log_emoji_info = f"Type: {type(reaction.emoji).__name__}, Value: {repr(reaction.emoji)[:50]}"
            log.warning(f"Lỗi thuộc tính khi xử lý reaction msg {message.id} location {location_id}: {attr_err} (Emoji info: {log_emoji_info})")
        # ----- KẾT THÚC THÊM XỬ LÝ -----
        except Exception as react_err:
            # Log lỗi chung khác
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
    skipped_threads_count = 0 # Khởi tạo biến đếm thread bị bỏ qua

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
            detail_entry = next((item for item in scan_data["channel_details"] if item.get('id') == channel.id), None)

            if detail_entry:
                log.info(f"  {e('info')} Đang cập nhật chi tiết kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/]...")
                await _update_channel_details_after_scan(
                    scan_data, channel, detail_entry, author_counter_channel,
                    channel_message_count, channel_scan_duration, channel_error
                )
                detail_entry["processed"] = True
                processed_channels_count += 1
            else:
                 log.error(f"  {e('error')} Không tìm thấy detail_entry cho kênh {channel.id} để cập nhật chi tiết.")
                 detail_entry = {} # Tạo dict rỗng để truyền vào scan thread nếu cần

            # --- Quét luồng ---
            if isinstance(channel, discord.TextChannel):
                 threads_scanned_count, threads_skipped_in_channel = await _scan_threads_in_channel(scan_data, channel, detail_entry)
                 processed_threads_count += threads_scanned_count
                 skipped_threads_count += threads_skipped_in_channel # Cộng dồn số thread bỏ qua
            else:
                 log.info(f"  {e('thread')} Bỏ qua quét luồng cho kênh voice {channel_type_emoji} [cyan]#{channel.name}[/].")

            log.info(f"  {e('success')} Hoàn thành xử lý kênh và luồng (nếu có) cho {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/].")
            await asyncio.sleep(0.1) # Delay nhẹ giữa các kênh

        except Exception as e_channel:
             channel_error_msg = f"Lỗi kênh {channel_type_name} #{channel.name}: {e_channel}"
             log.error(f"{utils.get_emoji('error', bot)} {channel_error_msg}", exc_info=not isinstance(e_channel, discord.Forbidden))
             scan_data["scan_errors"].append(channel_error_msg)

             detail_entry = next((item for item in scan_data["channel_details"] if item.get('id') == channel.id), None)
             if detail_entry:
                 existing_error = str(detail_entry.get("error") or "")
                 error_prefix = "FATAL SCAN ERROR: " if not channel_processed_flag else "PARTIAL SCAN ERROR: "
                 full_error = f"{error_prefix}{channel_error_msg}"
                 detail_entry["error"] = (existing_error + f"\n{full_error}").strip()
                 detail_entry["processed"] = channel_processed_flag # Đánh dấu xử lý lỗi
             else:
                  log.error(f"  {e('error')} Không tìm thấy detail_entry cho kênh lỗi {channel.id} để ghi lỗi.")

             try:
                 await scan_data["ctx"].send(f"{utils.get_emoji('error', bot)} {channel_error_msg}. Dữ liệu kênh #{channel.name} có thể không đầy đủ.", delete_after=30)
             except Exception: pass
             await asyncio.sleep(2) # Delay sau lỗi kênh

    # Cập nhật số lượng tổng cuối cùng vào scan_data
    scan_data["processed_channels_count"] = processed_channels_count
    scan_data["processed_threads_count"] = processed_threads_count
    scan_data["skipped_threads_count"] = skipped_threads_count # Cập nhật số thread đã bỏ qua

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
        threads_to_scan.extend(parent_channel.threads)

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

        unique_threads_map: Dict[int, discord.Thread] = {t.id: t for t in threads_to_scan}
        unique_threads_to_scan = list(unique_threads_map.values())
        total_threads_found = len(unique_threads_to_scan)

        if total_threads_found == 0:
            log.info(f"  {e('info')} Không tìm thấy luồng trong kênh text [cyan]#{parent_channel.name}[/].")
            channel_detail_entry["threads_data"] = []
            return 0, 0

        log.info(f"  Tìm thấy {total_threads_found} luồng. Bắt đầu quét...")
        threads_data_list = []

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
                 "message_count": 0, "reaction_count": None,
                 "scan_duration_seconds": 0, "error": None
            }

            try:
                thread_perms = thread.permissions_for(server.me)
                if not thread_perms.view_channel or not thread_perms.read_message_history:
                    reason = "Thiếu View" if not thread_perms.view_channel else "Thiếu Read History"
                    log.warning(f"    Bỏ qua luồng '{thread.name}' ({thread.id}): {reason}.")
                    scan_errors.append(f"Luồng '{thread.name}' ({thread.id}): Bỏ qua ({reason}).")
                    # scan_data["skipped_threads_count"] = scan_data.get("skipped_threads_count", 0) + 1 # Đã chuyển ra ngoài
                    threads_skipped += 1
                    thread_data_entry["error"] = f"Bỏ qua do {reason}"
                    threads_data_list.append(thread_data_entry)
                    continue
            except Exception as thread_perm_err:
                log.error(f"    {e('error')} Lỗi kiểm tra quyền luồng '{thread.name}': {thread_perm_err}", exc_info=True)
                scan_errors.append(f"Luồng '{thread.name}': Lỗi kiểm tra quyền.")
                # scan_data["skipped_threads_count"] = scan_data.get("skipped_threads_count", 0) + 1 # Đã chuyển ra ngoài
                threads_skipped += 1
                thread_data_entry["error"] = f"Lỗi kiểm tra quyền: {thread_perm_err}"
                threads_data_list.append(thread_data_entry)
                continue

            log.info(f"    [bold]({thread_index}/{total_threads_found})[/bold] Đang quét luồng [magenta]'{thread.name}'[/] ({thread.id})...")

            try:
                thread_message_iterator = thread.history(limit=None)
                async for message in thread_message_iterator:
                    await _process_message(message, scan_data, thread.id)
                    thread_message_count += 1
                    thread_processed_flag = True

                thread_scan_duration = discord.utils.utcnow() - thread_scan_start_time
                log.info(f"      {e('success')} Hoàn thành quét luồng [magenta]'{thread.name}'[/]: {thread_message_count:,} tin nhắn trong [magenta]{utils.format_timedelta(thread_scan_duration)}[/].")
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
                     # scan_data["skipped_threads_count"] = scan_data.get("skipped_threads_count", 0) + 1 # Đã chuyển ra ngoài
                     threads_skipped += 1

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

            threads_data_list.append(thread_data_entry)
            await asyncio.sleep(0.05)

        log.info(f"  {e('success')} Hoàn thành quét {threads_scanned_ok} luồng trong kênh #{parent_channel.name} ({threads_skipped} bị bỏ qua).")
        channel_detail_entry["threads_data"] = threads_data_list

    except Exception as e_outer_thread:
        log.error(f"Lỗi nghiêm trọng khi xử lý luồng cho kênh #{parent_channel.name}: {e_outer_thread}", exc_info=True)
        scan_errors.append(f"Lỗi nghiêm trọng khi xử lý luồng kênh #{parent_channel.name}: {e_outer_thread}")
        if "threads_data" not in channel_detail_entry:
            channel_detail_entry["threads_data"] = []

    return threads_scanned_ok, threads_skipped


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

    # Lấy top chatter
    top_chatter_info = "Không có (hoặc chỉ bot)"
    top_chatter_roles = "N/A"
    if author_counter_channel:
        try:
            top_author_id, top_count = author_counter_channel.most_common(1)[0]
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

    # Lấy tin nhắn đầu
    first_messages_log: List[str] = []
    first_messages_limit = 5
    first_messages_preview = 80
    try:
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

        if not first_messages_log and channel_message_count == 0:
             first_messages_log.append("`[Không có tin nhắn]`")
        elif not first_messages_log and channel_message_count > 0:
             first_messages_log.append("`[LỖI]` Không thể fetch tin nhắn đầu.")
    except Exception as e_first:
        log.error(f"Lỗi lấy tin nhắn đầu kênh #{channel.name}: {e_first}")
        first_messages_log = [f"`[LỖI]` {e('error')} Lỗi: {e_first}"]
        channel_error = (channel_error + f"\nLỗi lấy tin nhắn đầu: {e_first}").strip() if channel_error else f"Lỗi lấy tin nhắn đầu: {e_first}"

    # Lấy thông tin kênh
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

    # Tính toán reaction count (tạm thời lấy tổng đã lọc)
    filtered_channel_reaction_count = None
    if config.ENABLE_REACTION_SCAN:
        filtered_channel_reaction_count = sum(scan_data.get("filtered_reaction_emoji_counts", Counter()).values())

    # Cập nhật detail_entry
    update_data = {
        "message_count": channel_message_count,
        "reaction_count": filtered_channel_reaction_count,
        "duration_seconds": round(channel_scan_duration.total_seconds(), 2),
        "topic": channel_topic,
        "nsfw": channel_nsfw_str,
        "slowmode": channel_slowmode_str,
        "top_chatter": top_chatter_info,
        "top_chatter_roles": top_chatter_roles,
        "first_messages_log": first_messages_log,
        "error": channel_error
    }
    detail_entry.update(update_data)


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
            new_msg = await ctx.send(embed=embed)
            return new_msg
    except (discord.NotFound, discord.HTTPException) as http_err:
        log.warning(f"Cập nhật trạng thái thất bại ({http_err.status}), thử gửi lại.")
        try:
            new_msg = await ctx.send(embed=embed)
            return new_msg
        except Exception as send_new_err:
            log.error(f"Không thể gửi lại tin nhắn trạng thái: {send_new_err}")
            return None
    except Exception as e_stat:
        log.error(f"Lỗi không xác định khi cập nhật trạng thái: {e_stat}", exc_info=True)
        return None


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
    avg_time_per_channel = (time_so_far_sec / (current_channel_index - 1)) if current_channel_index > 1 else 60.0
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

    overall_msgs = scan_data.get('overall_total_message_count', 0)
    status_embed.add_field(name="Tổng Tin Nhắn", value=f"{overall_msgs:,}", inline=True)
    users_detected = len(scan_data['user_activity'])
    status_embed.add_field(name="Users Phát Hiện", value=f"{users_detected:,}", inline=True)
    status_embed.add_field(name="TG Kênh", value=utils.format_timedelta(datetime.timedelta(seconds=channel_elapsed_sec)), inline=True)

    status_embed.add_field(name="TG Ước Tính", value=utils.format_timedelta(datetime.timedelta(seconds=estimated_remaining_sec)), inline=True)
    status_embed.add_field(name="Dự Kiến Xong", value=utils.format_discord_time(estimated_completion_time, 'R'), inline=True)

    if scan_data.get("can_scan_reactions", False):
        filtered_reaction_count = sum(scan_data.get("filtered_reaction_emoji_counts", Counter()).values())
        status_embed.add_field(name=f"{e('reaction')} React (Lọc)", value=f"{filtered_reaction_count:,}", inline=True)
    else:
        status_embed.add_field(name="\u200b", value="\u200b", inline=True)

    footer_text = f"Quét toàn bộ | ID Kênh: {current_channel.id}"
    if discord_logging.get_log_target_thread():
        footer_text += " | Log chi tiết trong thread"
    status_embed.set_footer(text=footer_text)

    return status_embed

# --- END OF FILE cogs/deep_scan_helpers/scan_channels.py ---