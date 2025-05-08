# --- START OF FILE cogs/deep_scan_helpers/scan_channels.py ---
import discord
from discord.ext import commands
import logging
import asyncio
import time
import datetime
import re
from typing import Dict, Any, List, Union, Optional, Set, Tuple
from collections import Counter, defaultdict
import dotenv
import os
import config
import utils
import discord_logging

log = logging.getLogger(__name__)

# Biểu thức chính quy để tối ưu việc tìm kiếm
URL_REGEX = re.compile(r'https?://\S+')
EMOJI_REGEX = re.compile(r'<a?:([a-zA-Z0-9_]+):([0-9]+)>|([\U00010000-\U0010ffff])') # Capture cả group unicode

# --- Hằng số cho quét song song ---
MAX_CONCURRENT_SCANS = config.MAX_CONCURRENT_CHANNEL_SCANS # Số kênh/luồng quét đồng thời tối đa
scan_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SCANS)


# --- Hàm quét tin nhắn (được dùng cho cả kênh và luồng) ---
async def _process_message(message: discord.Message, scan_data: Dict[str, Any], location_id: int):
    """Xử lý một tin nhắn, cập nhật scan_data."""
    target_keywords = scan_data["target_keywords"]
    can_scan_reactions = scan_data.get("can_scan_reactions", False)
    server_emojis_cache: Dict[int, discord.Emoji] = scan_data.get("server_emojis_cache", {})
    server_sticker_ids_cache: Set[int] = scan_data.get("server_sticker_ids_cache", set())

    timestamp = message.created_at
    if not message.author or message.is_system(): # Bỏ qua tin nhắn hệ thống và webhook
        return

    author_id = message.author.id
    is_bot = message.author.bot

    # Cập nhật tổng tin nhắn toàn server
    if "overall_total_message_count" not in scan_data:
        scan_data["overall_total_message_count"] = 0
    scan_data["overall_total_message_count"] += 1

    # Cập nhật user_activity
    user_data = scan_data["user_activity"][author_id]
    user_data['message_count'] += 1
    scan_data["user_activity_message_counts"][author_id] = user_data['message_count']

    if user_data['first_seen'] is None or timestamp < user_data['first_seen']:
        user_data['first_seen'] = timestamp
    if user_data['last_seen'] is None or timestamp > user_data['last_seen']:
        user_data['last_seen'] = timestamp
    if is_bot:
        user_data['is_bot'] = True

    # Lưu kênh/luồng user đã nhắn (dùng cho tính distinct)
    user_data.setdefault('channels_messaged_in', set()).add(location_id)

    # Đếm tin nhắn cho user trong kênh/luồng này
    scan_data["user_channel_message_counts"][author_id][location_id] += 1

    # Thu thập dữ liệu giờ
    hour = timestamp.hour
    scan_data.setdefault("server_hourly_activity", Counter())[hour] += 1
    is_thread = isinstance(message.channel, discord.Thread)
    if is_thread:
        scan_data.setdefault("thread_hourly_activity", defaultdict(Counter))[location_id][hour] += 1
    else:
        scan_data.setdefault("channel_hourly_activity", defaultdict(Counter))[location_id][hour] += 1

    # --- THÊM THU THẬP GIỜ USER ---
    # Thu thập dữ liệu giờ cho user
    scan_data.setdefault("user_hourly_activity", defaultdict(Counter))[author_id][hour] += 1
    # --- KẾT THÚC THÊM ---

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
                    if emoji_id in server_emojis_cache:
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
        user_sticker_id_counter = scan_data.setdefault("user_sticker_id_counts", defaultdict(Counter))[author_id]

        for sticker_item in message.stickers:
             sticker_id_str = str(sticker_item.id)
             sticker_usage_counter[sticker_id_str] += 1
             user_sticker_id_counter[sticker_id_str] += 1
             if sticker_item.id in server_sticker_ids_cache:
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
            msg_react_received_count = 0 # Bao nhiêu reaction nhận được (kể cả bot thả)
            msg_react_filtered_count = 0 # Bao nhiêu reaction nhận được (đã lọc)

            filtered_reaction_emoji_counter = scan_data.setdefault("filtered_reaction_emoji_counts", Counter())
            reaction_total_emoji_counter = scan_data.setdefault("reaction_emoji_counts", Counter())
            user_react_given_counter = scan_data.setdefault("user_reaction_given_counts", Counter())
            user_react_emoji_given_counter = scan_data.setdefault("user_reaction_emoji_given_counts", defaultdict(Counter))

            for reaction in message.reactions:
                react_count = reaction.count # Số lượt thả của reaction này
                if react_count <= 0: continue

                emoji = reaction.emoji
                emoji_key_for_filtered: Optional[Union[int, str]] = None
                is_custom_server_emoji = False
                is_allowed_unicode = False

                if isinstance(emoji, discord.Emoji):
                    if emoji.id in server_emojis_cache:
                        is_custom_server_emoji = True
                        emoji_key_for_filtered = emoji.id
                elif isinstance(emoji, str): # Chỉ xử lý string unicode
                    if emoji in config.REACTION_UNICODE_EXCEPTIONS:
                        is_allowed_unicode = True
                        emoji_key_for_filtered = emoji

                # Thêm vào counter tổng thô (luôn luôn)
                reaction_total_emoji_counter[str(emoji)] += react_count
                msg_react_received_count += react_count

                is_filtered_reaction = is_custom_server_emoji or is_allowed_unicode

                # Nếu là reaction được lọc
                if is_filtered_reaction and emoji_key_for_filtered is not None:
                    msg_react_filtered_count += react_count
                    filtered_reaction_emoji_counter[emoji_key_for_filtered] += react_count

                    # Đếm người thả reaction (chỉ cho reaction đã lọc)
                    try:
                        async for user in reaction.users():
                            if user and not user.bot: # Chỉ đếm user thật
                                user_id = user.id
                                user_react_given_counter[user_id] += 1
                                user_react_emoji_given_counter[user_id][emoji_key_for_filtered] += 1
                                # Cập nhật vào user_activity của người thả
                                scan_data["user_activity"][user_id]['reaction_given_count'] = user_react_given_counter[user_id]

                    except Exception as user_fetch_err:
                        log.warning(f"Lỗi lấy user thả reaction '{emoji}' msg {message.id}: {user_fetch_err}")

            # Cập nhật tổng reaction thô và đã lọc
            scan_data["overall_total_reaction_count"] = scan_data.get("overall_total_reaction_count", 0) + msg_react_received_count
            scan_data["overall_total_filtered_reaction_count"] = scan_data.get("overall_total_filtered_reaction_count", 0) + msg_react_filtered_count

            # Đếm reaction nhận được (chỉ cho user)
            if not is_bot:
                user_react_received_counter = scan_data.setdefault("user_reaction_received_counts", Counter())
                # Chỉ cộng số reaction đã lọc vào đây để BXH nhất quán
                user_react_received_counter[author_id] += msg_react_filtered_count
                user_data['reaction_received_count'] = user_react_received_counter[author_id]

                # <<< THÊM LOGIC ĐẾM EMOJI NHẬN ĐƯỢC CHO USER >>>
                if msg_react_filtered_count > 0: # Chỉ đếm nếu có reaction đã lọc
                    user_emoji_received_counter_for_author = scan_data.setdefault("user_emoji_received_counts", defaultdict(Counter))[author_id]
                    # Cần lặp lại qua các reaction đã lọc để lấy emoji_key
                    for inner_reaction in message.reactions:
                         inner_emoji = inner_reaction.emoji
                         inner_emoji_key: Optional[Union[int, str]] = None
                         is_inner_custom = False
                         is_inner_allowed_unicode = False

                         if isinstance(inner_emoji, discord.Emoji):
                              if inner_emoji.id in server_emojis_cache:
                                   is_inner_custom = True
                                   inner_emoji_key = inner_emoji.id
                         elif isinstance(inner_emoji, str):
                              if inner_emoji in config.REACTION_UNICODE_EXCEPTIONS:
                                   is_inner_allowed_unicode = True
                                   inner_emoji_key = inner_emoji

                         # Nếu là reaction được lọc và có key hợp lệ
                         if (is_inner_custom or is_inner_allowed_unicode) and inner_emoji_key is not None:
                              user_emoji_received_counter_for_author[inner_emoji_key] += inner_reaction.count


        except AttributeError as attr_err:
            log_emoji_info = "N/A"
            if 'reaction' in locals() and hasattr(reaction, 'emoji'):
                log_emoji_info = f"Type: {type(reaction.emoji).__name__}, Value: {repr(reaction.emoji)[:50]}"
            log.warning(f"Lỗi thuộc tính khi xử lý reaction msg {message.id} location {location_id}: {attr_err} (Emoji info: {log_emoji_info})")
        except Exception as react_err:
            log.warning(f"Lỗi xử lý reaction msg {message.id} location {location_id}: {react_err}")


# --- Hàm Helper để cập nhật chi tiết cho một location (kênh hoặc luồng) ---
async def _populate_additional_location_details(
    scan_data: Dict[str, Any],
    location: Union[discord.TextChannel, discord.VoiceChannel, discord.Thread],
    result_dict_to_update: Dict[str, Any] # Dict kết quả của location này
):
    server: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)

    channel_message_count = result_dict_to_update.get("message_count", 0)
    author_counter_location = result_dict_to_update.get("author_counts", Counter())

    top_chatter_info = "Không có (hoặc chỉ bot)"
    top_chatter_roles = "N/A"
    if author_counter_location:
        try:
            top_author_id, top_count = author_counter_location.most_common(1)[0]
            user = await utils.fetch_user_data(server, top_author_id, bot_ref=bot)
            if user:
                top_chatter_info = f"{user.mention} (`{utils.escape_markdown(user.display_name)}`) - {top_count:,} tin"
                if isinstance(user, discord.Member):
                    member_roles = sorted([r for r in user.roles if not r.is_default()], key=lambda r: r.position, reverse=True)
                    roles_str = ", ".join([r.mention for r in member_roles]) if member_roles else "Không có role"
                    top_chatter_roles = roles_str[:150] + "..." if len(roles_str) > 150 else roles_str
                else: top_chatter_roles = "N/A (Không còn trong server)"
            else: top_chatter_info = f"ID: `{top_author_id}` (Không tìm thấy) - {top_count:,} tin"
        except Exception as chatter_err:
            log.error(f"Lỗi lấy top chatter cho {location.name}: {chatter_err}")
            top_chatter_info = f"{e('error')} Lỗi lấy top chatter"
    result_dict_to_update["top_chatter_info"] = top_chatter_info
    result_dict_to_update["top_chatter_roles"] = top_chatter_roles

    first_messages_log_list: List[str] = []
    if isinstance(location, (discord.TextChannel, discord.VoiceChannel)):
        first_messages_limit = 5; first_messages_preview = 80
        try:
            async for msg in location.history(limit=first_messages_limit, oldest_first=True):
                author_display = msg.author.display_name if msg.author else "Không rõ"
                timestamp_str = msg.created_at.strftime('%d/%m/%y %H:%M')
                content_preview = (msg.content or "")[:first_messages_preview].replace('`', "'").replace('\n', ' ')
                if len(msg.content or "") > first_messages_preview: content_preview += "..."
                elif not content_preview and msg.attachments: content_preview = "[File đính kèm]"
                elif not content_preview and msg.embeds: content_preview = "[Embed]"
                elif not content_preview and msg.stickers: content_preview = "[Sticker]"
                elif not content_preview: content_preview = "[Nội dung trống]"
                first_messages_log_list.append(f"[`{timestamp_str}`] **{utils.escape_markdown(author_display)}**: {utils.escape_markdown(content_preview)}")
            if not first_messages_log_list and channel_message_count == 0: first_messages_log_list.append("`[Không có tin nhắn]`")
            elif not first_messages_log_list and channel_message_count > 0: first_messages_log_list.append("`[LỖI]` Không thể fetch tin nhắn đầu.")
        except Exception as e_first:
            log.error(f"Lỗi lấy tin nhắn đầu cho {location.name}: {e_first}")
            first_messages_log_list = [f"`[LỖI]` {e('error')} Lỗi: {e_first}"]
            err_key = "error"; current_err = result_dict_to_update.get(err_key, "")
            result_dict_to_update[err_key] = (current_err + f"\nLỗi lấy tin nhắn đầu: {e_first}").strip()
    result_dict_to_update["first_messages_log"] = first_messages_log_list

    channel_topic = "N/A"; channel_nsfw_str = "N/A"; channel_slowmode_str = "N/A"
    if isinstance(location, discord.TextChannel):
        channel_topic = (location.topic or "Không có")[:150] + ("..." if location.topic and len(location.topic) > 150 else "")
        channel_nsfw_str = f"{e('success')} Có" if location.is_nsfw() else f"{e('error')} Không"
        channel_slowmode_str = f"{location.slowmode_delay} giây" if location.slowmode_delay > 0 else "Không"
    elif isinstance(location, discord.VoiceChannel):
        channel_nsfw_str = f"{e('success')} Có" if location.is_nsfw() else f"{e('error')} Không"
    result_dict_to_update["topic"] = channel_topic
    result_dict_to_update["nsfw_str"] = channel_nsfw_str
    result_dict_to_update["slowmode_str"] = channel_slowmode_str
    result_dict_to_update["reaction_count_filtered"] = None


# --- Hàm Wrapper để quét một location (kênh hoặc luồng) ---
async def _scan_individual_location_wrapper(
    scan_data: Dict[str, Any],
    location: Union[discord.TextChannel, discord.VoiceChannel, discord.Thread],
) -> Dict[str, Any]:
    location_message_count = 0
    location_error: Optional[str] = None
    author_counter_location: Counter = Counter()
    location_scan_start_time = discord.utils.utcnow()
    processed_flag = False

    log_prefix = f"Thread '{location.name}' ({location.id})" if isinstance(location, discord.Thread) else f"Channel '{location.name}' ({location.id})"
    log.info(f"Wrapper: Bắt đầu quét {log_prefix}")

    result = {
        "id": location.id, "name": location.name, "type": str(location.type),
        "processed": False, "message_count": 0, "error": None,
        "scan_duration_seconds": 0.0, "author_counts": Counter(),
        "threads_data": []
    }
    if isinstance(location, (discord.TextChannel, discord.VoiceChannel)):
        result["category"] = getattr(location.category, 'name', "N/A")
        result["category_id"] = getattr(location.category, 'id', None)
        result["created_at"] = location.created_at
    if isinstance(location, discord.Thread):
        result["parent_channel_id"] = location.parent_id
        result["archived"] = location.archived
        result["locked"] = location.locked
        if location.owner_id:
            owner = await utils.fetch_user_data(scan_data["server"], location.owner_id, bot_ref=scan_data["bot"])
            result["owner_id"] = location.owner_id
            result["owner_mention"] = owner.mention if owner else f"ID: {location.owner_id}"
            result["owner_name"] = owner.display_name if owner else "(Không tìm thấy)"

    try:
        if isinstance(location, discord.Thread):
            thread_perms = location.permissions_for(scan_data["server"].me)
            if not thread_perms.view_channel or not thread_perms.read_message_history:
                reason = "Thiếu View" if not thread_perms.view_channel else "Thiếu Read History"
                location_error = f"Bỏ qua luồng '{location.name}' ({location.id}): {reason}."
                result["error"] = location_error; result["processed"] = False
                result["scan_duration_seconds"] = (discord.utils.utcnow() - location_scan_start_time).total_seconds()
                log.warning(location_error)
                scan_data["scan_errors"].append(location_error)
                return result

        message_iterator = location.history(limit=None)
        async for message in message_iterator:
            await _process_message(message, scan_data, location.id)
            location_message_count += 1
            if message.author and not message.author.bot:
                author_counter_location[message.author.id] += 1
        processed_flag = True

    except discord.Forbidden as forbidden_err:
        location_error = f"Lỗi quyền quét {log_prefix}: {forbidden_err.text}"
    except discord.HTTPException as http_err:
        location_error = f"Lỗi HTTP {http_err.status} quét {log_prefix}: {http_err.text}"
        await asyncio.sleep(2)
    except Exception as e_loc:
        location_error = f"Lỗi không xác định quét {log_prefix}: {e_loc}"

    if location_error:
        log.error(location_error, exc_info=True)
        scan_data["scan_errors"].append(location_error)
        result["error"] = location_error
    result["processed"] = processed_flag
    result["message_count"] = location_message_count
    result["author_counts"] = author_counter_location

    await _populate_additional_location_details(scan_data, location, result)

    result["scan_duration_seconds"] = (discord.utils.utcnow() - location_scan_start_time).total_seconds()
    log.info(f"Wrapper: Hoàn thành {log_prefix}. Tin: {result['message_count']}. Thời gian: {result['scan_duration_seconds']:.2f}s. Lỗi: {result['error']}")
    return result


# --- Hàm xử lý một kênh và các luồng con của nó ---
async def _process_single_channel_and_its_threads(
    scan_data: Dict[str, Any],
    channel: Union[discord.TextChannel, discord.VoiceChannel]
) -> Dict[str, Any]:
    async with scan_semaphore:
        log.info(f"Bắt đầu xử lý song song cho kênh: {channel.name} ({channel.id})")
        channel_result = await _scan_individual_location_wrapper(scan_data, channel)

        if isinstance(channel, discord.TextChannel) and channel_result.get("processed"):
            threads_to_scan: List[discord.Thread] = []
            try:
                threads_to_scan.extend(channel.threads)
                if scan_data.get("can_scan_archived_threads", False):
                    log.debug(f"  Fetching archived threads cho kênh {channel.name}...")
                    async for thread_obj in channel.archived_threads(limit=None):
                        threads_to_scan.append(thread_obj)
            except Exception as e_fetch_thread:
                log.error(f"  Lỗi fetch threads cho kênh {channel.name}: {e_fetch_thread}")
                scan_data["scan_errors"].append(f"Lỗi fetch threads kênh {channel.name}: {e_fetch_thread}")

            unique_threads_map: Dict[int, discord.Thread] = {t.id: t for t in threads_to_scan}
            unique_threads_to_scan = list(unique_threads_map.values())

            if unique_threads_to_scan:
                log.info(f"  Kênh {channel.name} có {len(unique_threads_to_scan)} luồng để quét song song.")
                thread_tasks = [
                    asyncio.create_task(_scan_individual_location_wrapper(scan_data, thread_obj))
                    for thread_obj in unique_threads_to_scan
                ]
                thread_scan_results = await asyncio.gather(*thread_tasks, return_exceptions=True)
                channel_result["threads_data"] = [res for res in thread_scan_results if isinstance(res, dict)]
                for res_thread_err in thread_scan_results:
                    if isinstance(res_thread_err, Exception):
                        log.error(f"    Lỗi trong một task quét luồng (kênh {channel.name}): {res_thread_err}")
        else:
            channel_result["threads_data"] = []

        log.info(f"Hoàn thành xử lý song song cho kênh: {channel.name}. Tổng tin kênh: {channel_result.get('message_count',0)}")
        return channel_result


# --- Hàm chính để quét tất cả các kênh và luồng ---
async def scan_all_channels_and_threads(scan_data: Dict[str, Any]):
    accessible_channels: List[Union[discord.TextChannel, discord.VoiceChannel]] = scan_data["accessible_channels"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)

    log.info(f"Bắt đầu quét song song {len(accessible_channels)} kênh (tối đa {MAX_CONCURRENT_SCANS} đồng thời)...")

    last_status_update_time = scan_data["overall_start_time"]
    update_interval_seconds = 12
    status_message = scan_data["initial_status_msg"]

    new_channel_details: List[Dict[str, Any]] = []
    scan_data["processed_channels_count"] = 0
    scan_data["processed_threads_count"] = 0
    scan_data["skipped_threads_count"] = 0

    channel_tasks = [
        _process_single_channel_and_its_threads(scan_data, ch_obj)
        for ch_obj in accessible_channels
    ]

    completed_tasks_count = 0
    total_tasks_initial = len(channel_tasks)

    for coro in asyncio.as_completed(channel_tasks):
        try:
            channel_result_with_threads = await coro
            if channel_result_with_threads: # Kiểm tra None phòng trường hợp lỗi lạ
                new_channel_details.append(channel_result_with_threads)
                if channel_result_with_threads.get("processed"):
                    scan_data["processed_channels_count"] += 1
                for thread_res in channel_result_with_threads.get("threads_data", []):
                    if thread_res.get("processed") and not thread_res.get("error"):
                        scan_data["processed_threads_count"] += 1
                    else:
                        scan_data["skipped_threads_count"] += 1
        except Exception as e_task:
            log.error(f"Lỗi nghiêm trọng trong một tác vụ quét kênh chính: {e_task}", exc_info=True)
            scan_data["scan_errors"].append(f"Lỗi nghiêm trọng task quét kênh: {e_task}")
        finally:
            completed_tasks_count += 1
            now = discord.utils.utcnow()
            if (now - last_status_update_time).total_seconds() > update_interval_seconds or completed_tasks_count == total_tasks_initial:
                status_embed = _create_progress_embed(scan_data, completed_tasks_count, total_tasks_initial, now)
                status_message = await _update_status_message(scan_data["ctx"], status_message, status_embed)
                scan_data["status_message"] = status_message
                last_status_update_time = now

    scan_data["channel_details"] = new_channel_details
    log.info(
        f"Hoàn thành quét song song. "
        f"Kênh xử lý: {scan_data['processed_channels_count']}, "
        f"Luồng xử lý: {scan_data['processed_threads_count']}, "
        f"Luồng bỏ qua: {scan_data['skipped_threads_count']}"
    )


# --- Các hàm helper cho status message và progress embed (cập nhật) ---
async def _update_status_message(
    ctx: commands.Context,
    current_status_message: Optional[discord.Message],
    embed: discord.Embed
) -> Optional[discord.Message]:
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
    completed_locations_count: int,
    total_initial_channels: int, # Tổng số kênh ban đầu (không tính thread)
    now: datetime.datetime
) -> discord.Embed:
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)

    progress_percent = ((completed_locations_count) / total_initial_channels) * 100 if total_initial_channels > 0 else 0
    progress_bar = utils.create_progress_bar(progress_percent)

    time_so_far_sec = (now - scan_data["overall_start_time"]).total_seconds()
    overall_msgs = scan_data.get('overall_total_message_count', 0)
    overall_scan_speed = (overall_msgs / time_so_far_sec) if time_so_far_sec > 0.1 else 0

    avg_time_per_initial_channel = (time_so_far_sec / completed_locations_count) if completed_locations_count > 0 else 60.0
    estimated_remaining_sec = max(0.0, (total_initial_channels - completed_locations_count) * avg_time_per_initial_channel)
    estimated_completion_time = now + datetime.timedelta(seconds=estimated_remaining_sec)

    status_embed = discord.Embed(
        title=f"{e('loading')} Đang Quét Server...",
        description=progress_bar,
        color=discord.Color.orange(),
        timestamp=now
    )
    status_embed.add_field(name="Tiến độ (Kênh)", value=f"{completed_locations_count}/{total_initial_channels}", inline=True)
    status_embed.add_field(name="Tổng Tin Nhắn", value=f"{overall_msgs:,}", inline=True)
    status_embed.add_field(name="Tốc độ (Tổng)", value=f"~{overall_scan_speed:.1f} msg/s", inline=True)
    users_detected = len(scan_data['user_activity'])
    status_embed.add_field(name="Users Phát Hiện", value=f"{users_detected:,}", inline=True)
    status_embed.add_field(name="TG Ước Tính", value=utils.format_timedelta(datetime.timedelta(seconds=estimated_remaining_sec)), inline=True)
    status_embed.add_field(name="Dự Kiến Xong", value=utils.format_discord_time(estimated_completion_time, 'R'), inline=True)

    if scan_data.get("can_scan_reactions", False):
        filtered_reaction_count = scan_data.get("overall_total_filtered_reaction_count", 0)
        status_embed.add_field(name=f"{e('reaction')} React (Lọc)", value=f"{filtered_reaction_count:,}", inline=True)
    else:
        status_embed.add_field(name="\u200b", value="\u200b", inline=True) # Placeholder để giữ layout

    footer_text = f"Quét song song | Server ID: {scan_data['server'].id}"
    if discord_logging.get_log_target_thread():
        footer_text += " | Log chi tiết trong thread"
    status_embed.set_footer(text=footer_text)
    return status_embed
# --- END OF FILE cogs/deep_scan_helpers/scan_channels.py ---