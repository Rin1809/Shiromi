# --- START OF FILE cogs/deep_scan_helpers/dm_sender.py ---
import discord
from discord.ext import commands
import logging
import asyncio
import datetime
import time
from typing import Dict, Any, List, Optional, Set, Tuple, Union
from collections import Counter, defaultdict
import collections

import config
import utils
from reporting import embeds_dm

log = logging.getLogger(__name__)

# Hằng số delay (tính bằng giây)
DELAY_BETWEEN_USERS = 3.5 # Giữ delay để tránh spam admin quá nhanh
DELAY_BETWEEN_MESSAGES = 0.8
DELAY_BETWEEN_EMBEDS = 1.8
DELAY_ON_HTTP_ERROR = 5.0
DELAY_ON_FORBIDDEN = 1.0
DELAY_ON_UNKNOWN_ERROR = 3.0

# --- Hàm _prepare_ranking_data giữ nguyên ---
async def _prepare_ranking_data(scan_data: Dict[str, Any], guild: discord.Guild) -> Dict[str, Dict[int, int]]:

    rankings: Dict[str, Dict[int, int]] = {}
    e = lambda name: utils.get_emoji(name, scan_data["bot"]) # Hàm lấy emoji

    # --- Xác định User Admin cần lọc ---
    admin_ids_to_filter: Set[int] = set()
    try:
        # Lấy admin từ quyền guild
        admin_ids_to_filter.update(m.id for m in guild.members if m.guild_permissions.administrator)
        # Thêm admin từ config
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID:
             admin_ids_to_filter.add(config.ADMIN_USER_ID)
        log.debug(f"Admin IDs to filter from leaderboards: {admin_ids_to_filter}")
    except Exception as admin_err:
        log.error(f"Lỗi khi xác định admin IDs để lọc: {admin_err}")

    # --- Hàm Helper tính Rank từ Counter ---
    def get_ranks_from_counter(
        counter: Optional[Union[collections.Counter, Dict[Any, int]]],
        filter_admin: bool = True,
        min_value: int = 1 # Chỉ xếp hạng nếu giá trị >= min_value
    ) -> Dict[int, int]:
        if not counter: return {}
        # Đảm bảo là Counter để dùng most_common
        if not isinstance(counter, collections.Counter):
            counter = Counter(counter)

        ranks: Dict[int, int] = {}
        current_rank = 0
        # Sắp xếp theo giá trị giảm dần
        sorted_items = counter.most_common()

        for key, count in sorted_items:
            # Đảm bảo key là user_id (int) và count > 0
            user_id: Optional[int] = None
            if isinstance(key, int): user_id = key
            elif isinstance(key, str) and key.isdigit(): user_id = int(key)
            else: continue # Bỏ qua key không hợp lệ

            if count < min_value: continue # Bỏ qua nếu giá trị quá thấp

            # Lọc admin nếu cần
            if filter_admin and user_id in admin_ids_to_filter:
                continue

            # Tăng hạng và lưu
            current_rank += 1
            ranks[user_id] = current_rank
        return ranks

    # --- Hàm Helper tính Rank từ List (ví dụ: oldest members) ---
    def get_ranks_from_list(data_list: List[Dict[str, Any]], id_key: str) -> Dict[int, int]:
        ranks: Dict[int, int] = {}
        for i, item in enumerate(data_list):
             user_id_any = item.get(id_key)
             user_id: Optional[int] = None
             if isinstance(user_id_any, int): user_id = user_id_any
             elif isinstance(user_id_any, str) and user_id_any.isdigit(): user_id = int(user_id_any)

             if user_id is not None:
                 ranks[user_id] = i + 1 # Rank bắt đầu từ 1
        return ranks

    # --- Hàm Helper tính Rank cho Tracked Roles ---
    def get_ranks_from_tracked_roles(
        tracked_counts: Optional[collections.Counter], # Counter { (uid, rid): count }
        role_id: int
    ) -> Dict[int, int]:
        if not isinstance(tracked_counts, collections.Counter): return {}

        # Tạo counter riêng cho role này: {user_id: count}
        role_specific_counter = Counter({
            uid: count
            for (uid, rid), count in tracked_counts.items()
            if rid == role_id and count > 0
        })
        # Dùng hàm get_ranks_from_counter (không lọc admin cho danh hiệu)
        return get_ranks_from_counter(role_specific_counter, filter_admin=False)

    log.debug(f"{e('loading')} Bắt đầu tính toán dữ liệu xếp hạng cho DM...")
    start_rank_time = time.monotonic()

    # === Tính toán các bảng xếp hạng ===
    # Hoạt động & Tương tác
    rankings["messages"] = get_ranks_from_counter(scan_data.get("user_activity_message_counts"), filter_admin=True)
    rankings["reaction_received"] = get_ranks_from_counter(scan_data.get("user_reaction_received_counts"), filter_admin=False)
    rankings["replies"] = get_ranks_from_counter(scan_data.get("user_reply_counts"), filter_admin=True)
    rankings["mention_received"] = get_ranks_from_counter(scan_data.get("user_mention_received_counts"), filter_admin=False)
    rankings["mention_given"] = get_ranks_from_counter(scan_data.get("user_mention_given_counts"), filter_admin=True)
    rankings["distinct_channels"] = get_ranks_from_counter(scan_data.get("user_distinct_channel_counts"), filter_admin=True)
    # THÊM HẠNG REACTION GIVEN
    rankings["reaction_given"] = get_ranks_from_counter(scan_data.get("user_reaction_given_counts"), filter_admin=True)

    # Sáng Tạo Nội Dung
    rankings["custom_emoji_content"] = get_ranks_from_counter(scan_data.get("user_total_custom_emoji_content_counts"), filter_admin=True)
    rankings["stickers_sent"] = get_ranks_from_counter(scan_data.get("user_sticker_counts"), filter_admin=True)
    rankings["links_sent"] = get_ranks_from_counter(scan_data.get("user_link_counts"), filter_admin=True)
    rankings["images_sent"] = get_ranks_from_counter(scan_data.get("user_image_counts"), filter_admin=True)
    rankings["threads_created"] = get_ranks_from_counter(scan_data.get("user_thread_creation_counts"), filter_admin=True)


    # BXH Danh hiệu đặc biệt
    tracked_grants = scan_data.get("tracked_role_grant_counts", Counter())
    for rid in config.TRACKED_ROLE_GRANT_IDS:
        rankings[f"tracked_role_{rid}"] = get_ranks_from_tracked_roles(tracked_grants, rid)

    # BXH Thời gian & Tham gia
    rankings["oldest_members"] = get_ranks_from_list(scan_data.get("oldest_members_data", []), 'id')

    # BXH Activity Span
    user_spans: List[Tuple[int, float]] = []
    for user_id, data in scan_data.get('user_activity', {}).items():
        span_seconds = data.get('activity_span_seconds', 0.0)
        if span_seconds > 0 and not data.get('is_bot', False):
             user_spans.append((user_id, span_seconds))
    user_spans.sort(key=lambda item: item[1], reverse=True)
    rankings["activity_span"] = {user_id: rank + 1 for rank, (user_id, span) in enumerate(user_spans)}

    # BXH Booster Duration
    boosters = scan_data.get("boosters", [])
    rankings["booster_duration"] = {m.id: rank + 1 for rank, m in enumerate(boosters)}

    end_rank_time = time.monotonic()
    log.debug(f"{e('success')} Hoàn thành tính toán dữ liệu xếp hạng ({len(rankings)} BXH) trong {end_rank_time - start_rank_time:.2f}s.")
    return rankings
# --- Kết thúc hàm _prepare_ranking_data ---


async def send_personalized_dm_reports(scan_data: Dict[str, Any], is_testing_mode: bool):
    """Gửi báo cáo DM cá nhân hóa."""
    guild: discord.Guild = scan_data["server"]
    bot: commands.Bot = scan_data["bot"]
    e = lambda name: utils.get_emoji(name, bot)
    recipient_role_id: Optional[int] = config.DM_REPORT_RECIPIENT_ROLE_ID
    thank_you_role_ids: Set[int] = config.BOOSTER_THANKYOU_ROLE_IDS
    admin_user_id: Optional[int] = config.ADMIN_USER_ID

    is_test_mode = is_testing_mode # Dùng trực tiếp tham số
    log.debug(f"[DM Sender] Explicit is_testing_mode received = {is_test_mode}")

    # --- Lấy đối tượng admin (luôn cần nếu test mode) ---
    admin_member: Optional[discord.Member] = None
    admin_dm_channel: Optional[discord.DMChannel] = None
    if is_test_mode:
        if not admin_user_id:
            log.error("Chế độ Test DM bật nhưng ADMIN_USER_ID chưa được cấu hình!")
            scan_data["scan_errors"].append("Test DM thất bại: Thiếu ADMIN_USER_ID.")
            return
        try:
            admin_member = await utils.fetch_user_data(guild, admin_user_id, bot_ref=bot)
            if not admin_member:
                log.error(f"Không tìm thấy Admin ({admin_user_id}) trong server để gửi Test DM.")
                scan_data["scan_errors"].append(f"Test DM thất bại: Không tìm thấy Admin ({admin_user_id}).")
                return
            if isinstance(admin_member, discord.Member): # Đảm bảo admin còn trong server
                admin_dm_channel = admin_member.dm_channel or await admin_member.create_dm()
            else: # Nếu admin không còn trong server (hiếm)
                 log.warning(f"Admin {admin_user_id} không còn trong server, không thể lấy DM channel.")
                 scan_data["scan_errors"].append(f"Test DM thất bại: Admin ({admin_user_id}) không còn trong server.")
                 return
        except discord.Forbidden:
            log.error(f"Không thể tạo DM channel cho Admin ({admin_user_id}). Bot bị chặn?")
            scan_data["scan_errors"].append(f"Test DM thất bại: Không thể tạo DM cho Admin ({admin_user_id}).")
            return
        except Exception as fetch_err:
             log.error(f"Lỗi khi fetch Admin ({admin_user_id}): {fetch_err}", exc_info=True)
             scan_data["scan_errors"].append(f"Test DM thất bại: Lỗi fetch Admin ({admin_user_id}).")
             return

    # --- Xác định danh sách thành viên cần xử lý (để tạo báo cáo) ---
    members_to_process: List[discord.Member] = []
    process_description = ""
    if recipient_role_id:
        recipient_role = guild.get_role(recipient_role_id)
        if recipient_role:
            # Lấy tất cả member có role đó (trừ bot)
            members_to_process = [m for m in guild.members if recipient_role in m.roles and not m.bot]
            process_description = f"thành viên có role '{recipient_role.name}'"
        else:
            log.error(f"Không tìm thấy role nhận DM với ID: {recipient_role_id}.")
            scan_data["scan_errors"].append(f"Không tìm thấy Role nhận DM ({recipient_role_id}).")
            # Có thể dừng hoặc tiếp tục nếu test mode
            if not is_test_mode:
                return # Dừng nếu chạy bình thường mà không có role
    else:
        # Nếu không có role ID, và đang ở chế độ bình thường -> không làm gì
        if not is_test_mode:
            log.info("Không có ID role nhận DM được cấu hình, bỏ qua gửi DM.")
            return
        # Nếu không có role ID, và đang test mode -> Lấy TẤT CẢ user không phải bot làm đối tượng xử lý
        log.warning("Không có role nhận DM được cấu hình, Test Mode sẽ xử lý TẤT CẢ user (không phải bot).")
        members_to_process = [m for m in guild.members if not m.bot]
        process_description = "tất cả thành viên (không phải bot)"


    if not members_to_process:
        log.info(f"Không tìm thấy {process_description} để xử lý báo cáo DM.")
        return

    if is_test_mode:
        log.info(f"Chế độ Test: Sẽ tạo và gửi {len(members_to_process)} báo cáo của {process_description} đến Admin ({admin_member.display_name}).")
    else:
        log.info(f"Chuẩn bị gửi DM báo cáo cho {len(members_to_process)} {process_description}.")

    # Lấy danh sách Role objects cho việc cảm ơn
    thank_you_roles: Set[discord.Role] = {guild.get_role(rid) for rid in thank_you_role_ids if guild.get_role(rid)}
    if thank_you_roles:
        log.info(f"Lời cảm ơn đặc biệt sẽ được thêm cho các role: {[r.name for r in thank_you_roles]}")

    # --- Chuẩn bị dữ liệu xếp hạng MỘT LẦN ---
    ranking_data = await _prepare_ranking_data(scan_data, guild)

    # --- Bắt đầu gửi DM ---
    sent_dm_count = 0
    failed_dm_count = 0
    processed_members_count = 0

    # *** VÒNG LẶP CHÍNH: Luôn lặp qua members_to_process ***
    for member in members_to_process:
        processed_members_count += 1
        log.info(f"{e('loading')} ({processed_members_count}/{len(members_to_process)}) Đang tạo báo cáo cho {member.display_name} ({member.id})...")

        # Danh sách tin nhắn và embeds để gửi CHO USER NÀY
        messages_to_send: List[str] = []
        embeds_to_send: List[discord.Embed] = []

        # --- Xác định đích gửi DM ---
        target_dm_channel: Optional[Union[discord.DMChannel, Any]] = None
        target_description_log = "" # Để log cho rõ
        is_sending_to_admin = False # Cờ để biết có cần thêm prefix không

        if is_test_mode:
            target_dm_channel = admin_dm_channel # Đã lấy ở trên
            target_description_log = f"Admin ({admin_member.id})"
            is_sending_to_admin = True
            # Thêm tiền tố vào tin nhắn đầu tiên để biết báo cáo này của ai
            test_prefix = f"```---\n📝 Báo cáo Test cho: {member.display_name} ({member.id})\n---```\n"
            messages_to_send.append(test_prefix)
        else:
            try:
                # Lấy DM channel của thành viên hiện tại
                target_dm_channel = member.dm_channel or await member.create_dm()
                target_description_log = f"User {member.id}"
            except discord.Forbidden:
                 log.warning(f"❌ Không thể tạo/lấy DM channel cho {member.display_name} ({member.id}). Bỏ qua user này.")
                 failed_dm_count += 1
                 await asyncio.sleep(DELAY_ON_FORBIDDEN)
                 continue # Sang user tiếp theo
            except Exception as dm_create_err:
                 log.error(f"❌ Lỗi khi tạo DM channel cho {member.display_name} ({member.id}): {dm_create_err}", exc_info=True)
                 failed_dm_count += 1
                 await asyncio.sleep(DELAY_ON_UNKNOWN_ERROR)
                 continue # Sang user tiếp theo

        # Nếu không lấy được target channel (kể cả admin), bỏ qua
        if not target_dm_channel:
            log.error(f"Không thể xác định kênh DM đích cho {member.display_name}. Bỏ qua.")
            failed_dm_count +=1
            continue

        # --- Tạo nội dung báo cáo cho 'member' hiện tại ---
        try:
            user_has_thank_you_role = any(role in member.roles for role in thank_you_roles)
            # Tạo tin nhắn chào mừng/cảm ơn (Logic cũ giữ nguyên)
            image_url = "https://cdn.discordapp.com/attachments/1141675354470223887/1368708955911753751/image.png?ex=6819350c&is=6817e38c&hm=2152f8ecd42616638d092986066d6123338aea5e8c485fc3153d52d2f9ede2d5&" # <--- Lưu URL ảnh (đã bỏ & thừa)

            # Tạo tin nhắn chào mừng/cảm ơn
            if user_has_thank_you_role:
                thank_you_title = f"💖 Cảm ơn bạn đã là một phần tuyệt vời của {guild.name}! 💖"
                thank_you_body = (
                     f"🎀 | Chào cậu, {member.mention},\n\n"
                     f"Đầu tiên, thay mặt Rin - Misuzu và mọi người **{guild.name}**, bọn tớ xin gửi lời cảm ơn cậu vì đã **đóng góp/boost** cho server! ✨\n\n"
                     f"Sự đóng góp của cậu giúp server ngày càng phát triển và duy trì một môi trường tuyệt vời cho tất cả mọi người á. \n\n"
                     f"Dưới đây là một chút tổng kết về hoạt động của cậu trong thời gian vừa qua (có thể có một chút sai số). Mong rằng cậu sẽ tiếp tục đồng hành cùng bọn tớ! \n\n"
                     f"Mỗi Member sau khi xác thực role [🔭 | Cư Dân ᓚᘏᗢ] và bật nhận tin nhắn từ người lạ sẽ đều nhận được bức thư này... \n\n"
                     f"Nhưng bức thư đây là dành riêng cho các [Quý tộc (Server Booster)🌠💫] | [| Người đóng góp (quý tộc-)] á \n\n"
                     f"*Một lần nữa, cảm ơn cậu nhé ! 本当にありがとうございます！！* \n\n"
                     f"Tớ là {config.BOT_NAME} | (Bot của Rin, thay mặt cho Rin gửi lời!) \n\n"
                     f"# ᓚᘏᗢ"
                 )
                messages_to_send.append(thank_you_title + "\n\n" + thank_you_body)
                messages_to_send.append(image_url)
            else:
                 greeting_msg = (
                     f"📊 Chào cậu {member.mention}, \n\n"
                     f"Đầu tiên, Thay mặt Rin - Misuzu và mọi người **{guild.name}**, bọn tớ xin gửi lời cảm ơn cậu vì đã có mặt và hoạt động trong server của bọn tớ vào thời gian qua! \n\n"
                     f"Dưới đây là một chút tổng kết về hoạt động của cậu trong thời gian vừa qua (có thể có một chút sai số). Mong rằng cậu sẽ tiếp tục đồng hành cùng bọn tớ! \n\n"
                     f"Mỗi Member sau khi xác thực role [🔭 | Cư Dân ᓚᘏᗢ] và bật nhận tin nhắn từ người lạ sẽ đều nhận được bức thư này... \n\n"
                     f"*Một lần nữa, cảm ơn cậu nhé ! 本当にありがとうございます！！* \n\n"
                     f"Tớ là {config.BOT_NAME} | (Bot của Rin, thay mặt cho Rin gửi lời!) \n\n"
                     f"# ᓚᘏᗢ"
                 )
                 messages_to_send.append(greeting_msg)
                 # THÊM LINK ẢNH NHƯ TIN NHẮN RIÊNG (cho cả người không có role cảm ơn)
                 messages_to_send.append(image_url)

            # --- KẾT THÚC SỬA ĐỔI ---

            # Tạo Embed Hoạt Động Cá Nhân (Gọi hàm đã nâng cấp)
            personal_activity_embed = await embeds_dm.create_personal_activity_embed(member, scan_data, bot, ranking_data)
            if personal_activity_embed: embeds_to_send.append(personal_activity_embed)
            else: log.warning(f"Không thể tạo personal_activity_embed cho {member.display_name}")

            # Tạo Embed Thành Tích & So Sánh (Gọi hàm đã nâng cấp)
            achievements_embed = await embeds_dm.create_achievements_embed(member, scan_data, bot, ranking_data)
            if achievements_embed: embeds_to_send.append(achievements_embed)
            else: log.warning(f"Không thể tạo achievements_embed cho {member.display_name}")

            # Thêm tin nhắn kết thúc
            final_message = f"Đây là báo cáo tự động được tạo bởi {config.BOT_NAME}. Báo cáo này chỉ dành cho cậu. Chúc cậu một ngày vui vẻ! 🎉"
            messages_to_send.append(final_message)

            # --- Gửi DM đến target_dm_channel ---
            if not embeds_to_send and not messages_to_send:
                log.warning(f"Không có nội dung DM để gửi cho {member.display_name}.")
                failed_dm_count += 1
                continue # Bỏ qua user này

            try:
                # Gửi tin nhắn text trước
                for msg_content in messages_to_send:
                    if msg_content:
                        # Chỉ gửi nếu target channel vẫn còn hợp lệ
                        if target_dm_channel:
                            await target_dm_channel.send(content=msg_content)
                            await asyncio.sleep(DELAY_BETWEEN_MESSAGES)
                        else:
                            log.warning(f"Target DM channel không còn hợp lệ khi gửi message cho {target_description_log}")
                            raise Exception("Target DM channel became invalid") # Gây lỗi để vào except bên dưới

                # Gửi embeds sau
                for embed in embeds_to_send:
                    if isinstance(embed, discord.Embed):
                        # Chỉ gửi nếu target channel vẫn còn hợp lệ
                        if target_dm_channel:
                            await target_dm_channel.send(embed=embed)
                            await asyncio.sleep(DELAY_BETWEEN_EMBEDS)
                        else:
                            log.warning(f"Target DM channel không còn hợp lệ khi gửi embed cho {target_description_log}")
                            raise Exception("Target DM channel became invalid") # Gây lỗi

                sent_dm_count += 1
                log.info(f"✅ Gửi báo cáo của {member.display_name} ({member.id}) thành công đến {target_description_log}")

            except discord.Forbidden:
                log.warning(f"❌ Không thể gửi DM đến {target_description_log} (cho báo cáo của {member.id}): User/Admin đã chặn DM hoặc bot.")
                failed_dm_count += 1
                await asyncio.sleep(DELAY_ON_FORBIDDEN)
                # Nếu là test mode và lỗi gửi đến admin -> dừng hẳn việc gửi test
                if is_test_mode:
                    log.error("LỖI NGHIÊM TRỌNG: Không thể gửi Test DM đến Admin. Dừng gửi DM.")
                    scan_data["scan_errors"].append("Test DM thất bại: Không thể gửi DM đến Admin (Forbidden).")
                    return # Dừng hẳn hàm
                # Nếu là chế độ thường, chỉ continue
                target_dm_channel = None # Đánh dấu channel không hợp lệ
            except discord.HTTPException as dm_http_err:
                log.error(f"❌ Lỗi HTTP {dm_http_err.status} khi gửi DM đến {target_description_log} (cho báo cáo của {member.id}): {dm_http_err.text}")
                failed_dm_count += 1
                await asyncio.sleep(DELAY_ON_HTTP_ERROR)
                # Nếu là test mode và lỗi HTTP -> dừng hẳn
                if is_test_mode and dm_http_err.status != 429: # Cho phép retry nếu chỉ là rate limit
                     log.error("LỖI NGHIÊM TRỌNG: Lỗi HTTP khi gửi Test DM đến Admin. Dừng gửi DM.")
                     scan_data["scan_errors"].append(f"Test DM thất bại: Lỗi HTTP {dm_http_err.status} khi gửi đến Admin.")
                     return
                target_dm_channel = None # Đánh dấu channel không hợp lệ
            except Exception as dm_err:
                log.error(f"❌ Lỗi không xác định khi gửi DM đến {target_description_log} (cho báo cáo của {member.id}): {dm_err}", exc_info=True)
                failed_dm_count += 1
                await asyncio.sleep(DELAY_ON_UNKNOWN_ERROR)
                # Nếu là test mode và lỗi lạ -> dừng hẳn
                if is_test_mode:
                    log.error("LỖI NGHIÊM TRỌNG: Lỗi không xác định khi gửi Test DM đến Admin. Dừng gửi DM.")
                    scan_data["scan_errors"].append("Test DM thất bại: Lỗi không xác định khi gửi đến Admin.")
                    return
                target_dm_channel = None # Đánh dấu channel không hợp lệ

            # Delay chung giữa các user (kể cả khi gửi test cho admin)
            await asyncio.sleep(DELAY_BETWEEN_USERS)

        except Exception as user_proc_err:
            log.error(f"Lỗi nghiêm trọng khi xử lý dữ liệu DM cho {member.display_name} ({member.id}): {user_proc_err}", exc_info=True)
            failed_dm_count += 1
            await asyncio.sleep(DELAY_ON_UNKNOWN_ERROR)

    log.info(f"--- {e('success')} Hoàn tất gửi DM báo cáo ---")
    mode_str = "Test Mode (gửi đến Admin)" if is_test_mode else "Normal Mode"
    log.info(f"Chế độ: {mode_str}")
    log.info(f"Tổng cộng: {sent_dm_count} thành công, {failed_dm_count} thất bại.")
    if failed_dm_count > 0:
        scan_data["scan_errors"].append(f"Gửi DM ({mode_str}) thất bại cho {failed_dm_count} báo cáo.")

# --- END OF FILE cogs/deep_scan_helpers/dm_sender.py ---