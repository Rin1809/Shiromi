# --- START OF FILE reporting/embeds_user.py ---
import discord
import datetime
import math
import logging
import collections
import asyncio
from typing import List, Dict, Any, Optional, Union, Tuple
from discord.ext import commands # Cần commands.Bot

# Relative import
try:
    from .. import utils
except ImportError:
    import utils

log = logging.getLogger(__name__)

# --- Constants ---
USERS_PER_ACTIVITY_EMBED = 15         # Số user mỗi embed hoạt động chi tiết
TOP_ACTIVE_USERS_LIMIT = 30           # Giới hạn top user hoạt động (tin nhắn)
TOP_OLDEST_MEMBERS_LIMIT = 30         # Giới hạn top thành viên lâu năm
TOP_LINK_USERS_LIMIT = 30             # Giới hạn top gửi link
TOP_IMAGE_USERS_LIMIT = 30            # Giới hạn top gửi ảnh
TOP_EMOJI_USERS_LIMIT = 30            # Giới hạn top dùng emoji content
TOP_STICKER_USERS_LIMIT = 30          # Giới hạn top gửi sticker
TOP_MENTIONED_USERS_LIMIT = 30        # Giới hạn top được nhắc tên
TOP_MENTIONING_USERS_LIMIT = 30       # Giới hạn top hay nhắc tên
TOP_REPLIERS_LIMIT = 30               # Giới hạn top trả lời tin nhắn
TOP_REACTION_RECEIVED_USERS_LIMIT = 30 # Giới hạn top nhận reaction
TOP_ACTIVITY_SPAN_USERS_LIMIT = 30    # Giới hạn top user hoạt động lâu (span)
TOP_THREAD_CREATORS_LIMIT = 30      # Giới hạn top tạo thread
USER_ROLE_STATS_PER_EMBED = 10        # Số user mỗi embed thống kê role


# --- Hàm Helper Tạo Embed Leaderboard Chung ---
async def create_generic_leaderboard_embed(
    counter_data: collections.Counter, # Dữ liệu Counter {user_id: count}
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot], # Cần bot để fetch user
    title: str, # Tiêu đề embed (đã bao gồm emoji)
    item_name_singular: str, # Tên đơn vị đếm (số ít, vd: "tin nhắn")
    item_name_plural: str, # Tên đơn vị đếm (số nhiều, vd: "tin nhắn")
    limit: int, # Số lượng hiển thị trên bảng xếp hạng
    color: discord.Color = discord.Color.blue(),
    show_total: bool = True, # Có hiển thị tổng số item không
    footer_note: Optional[str] = None # Ghi chú ở footer
) -> Optional[discord.Embed]:
    """
    Hàm chung để tạo embed leaderboard cho user dựa trên dữ liệu Counter.
    Tự động fetch user info và định dạng hiển thị.
    """
    e = lambda name: utils.get_emoji(name, bot)
    if not counter_data:
        log.debug(f"Bỏ qua tạo leaderboard '{title}': Không có dữ liệu counter.")
        return None

    # Tính tổng số item (nếu cần)
    total_items = sum(counter_data.values()) if show_total else 0

    embed = discord.Embed(title=f"{e('award')} {title}", color=color)

    description_lines = []
    if show_total:
        description_lines.append(f"*Tổng cộng: **{total_items:,}** {item_name_plural}. Đã lọc bot.*")
    else:
        description_lines.append("*Đã lọc bot.*") # Thông báo đã lọc bot

    # Lấy top N user
    sorted_users = counter_data.most_common(limit)
    if not sorted_users:
        embed.description = "\n".join(description_lines) + "\n\n_Không có dữ liệu._"
        return embed

    # Fetch thông tin user cho top N và cache lại
    user_ids_to_fetch = [uid for uid, count in sorted_users]
    log.debug(f"Fetching {len(user_ids_to_fetch)} users for leaderboard '{title}'...")
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    fetch_tasks = [
        utils.fetch_user_data(guild, user_id, bot_ref=bot)
        for user_id in user_ids_to_fetch
    ]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    # Xử lý kết quả fetch và điền vào cache
    for idx, result in enumerate(results):
        user_id = user_ids_to_fetch[idx]
        if isinstance(result, (discord.User, discord.Member)):
            user_cache[user_id] = result
        else:
            user_cache[user_id] = None # Đánh dấu là không tìm thấy/lỗi
            if isinstance(result, Exception):
                log.warning(f"Lỗi fetch user {user_id} cho leaderboard '{title}': {result}")
    log.debug(f"Fetch user hoàn thành cho leaderboard '{title}'.")

    # Tạo danh sách xếp hạng
    leaderboard_lines = []
    for rank, (user_id, count) in enumerate(sorted_users, 1):
        user_obj = user_cache.get(user_id)
        user_mention = user_obj.mention if user_obj else f"`{user_id}`"
        user_display = f" (`{utils.escape_markdown(user_obj.display_name)}`)" if user_obj else " (Unknown/Left)"

        # Chọn đúng dạng số ít/nhiều cho đơn vị đếm
        item_name = item_name_plural if count != 1 else item_name_singular

        leaderboard_lines.append(
            f"**`#{rank:02d}`**. {user_mention}{user_display} — **{count:,}** {item_name}"
        )

    description_lines.append("\n" + "\n".join(leaderboard_lines))

    # Thêm thông báo nếu còn nhiều user hơn giới hạn hiển thị
    if len(counter_data) > limit:
        description_lines.append(f"\n... và {len(counter_data) - limit} người dùng khác.")

    embed.description = "\n".join(description_lines)
    # Giới hạn độ dài description
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    if footer_note:
        embed.set_footer(text=footer_note)

    return embed


# --- Các hàm tạo Embed User cụ thể ---

async def create_user_activity_embeds(
    user_activity: Dict[int, Dict[str, Any]], # Dữ liệu user_activity thô
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot],
    min_message_count: int, # Ngưỡng tin nhắn tối thiểu để hiển thị
    scan_start_time: datetime.datetime, # Không dùng nhưng giữ để tương thích API cũ
) -> List[discord.Embed]:
    """Tạo embeds hiển thị hoạt động chi tiết của user, lọc theo ngưỡng tin nhắn."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)

    # Lọc user không phải bot và đạt ngưỡng tin nhắn
    filtered_user_activity = {
        uid: data for uid, data in user_activity.items()
        if not data.get('is_bot', False) and data.get('message_count', 0) >= min_message_count
    }

    if not filtered_user_activity:
        no_activity_embed = discord.Embed(
            title=f"{e('user_activity')} Hoạt động User Chi Tiết",
            description=f"{e('info')} Không có user nào đạt ngưỡng {min_message_count} tin nhắn.",
            color=discord.Color.light_grey()
        )
        return [no_activity_embed]

    # Sắp xếp user theo thời gian hoạt động cuối cùng (mới nhất trước)
    sorted_users = sorted(
        filtered_user_activity.items(),
        key=lambda item: item[1].get('last_seen', datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)),
        reverse=True
    )

    total_users_to_report = len(sorted_users)
    num_activity_embeds = math.ceil(total_users_to_report / USERS_PER_ACTIVITY_EMBED)

    # Fetch thông tin user một lần cho tất cả user cần báo cáo
    all_user_ids = [uid for uid, data in sorted_users]
    log.info(f"Fetching data for {len(all_user_ids)} users for detailed activity report...")
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    fetch_tasks = [utils.fetch_user_data(guild, user_id, bot_ref=bot) for user_id in all_user_ids]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    for idx, result in enumerate(results):
        user_id = all_user_ids[idx]
        if isinstance(result, (discord.User, discord.Member)):
            user_cache[user_id] = result
        else:
            user_cache[user_id] = None
            if isinstance(result, Exception):
                log.warning(f"Lỗi fetch user {user_id} cho activity embed: {result}")
    log.info("Hoàn thành fetch user đồng thời cho activity report.")


    # Tạo các embed theo trang
    for i in range(num_activity_embeds):
        start_index = i * USERS_PER_ACTIVITY_EMBED
        end_index = start_index + USERS_PER_ACTIVITY_EMBED
        user_batch_data = sorted_users[start_index:end_index]

        activity_embed = discord.Embed(
            title=f"{e('user_activity')} Hoạt động User Chi Tiết (Phần {i + 1}/{num_activity_embeds})",
            description=f"*User có >= {min_message_count} tin nhắn. Lọc bot. Sắp xếp theo hoạt động gần nhất.*",
            color=discord.Color.teal()
        )
        description_lines = []

        for user_id, data in user_batch_data:
            user_obj = user_cache.get(user_id)

            # --- Lấy thông tin hiển thị cơ bản ---
            user_display_header = f"`{user_id}` (Unknown/Left)"
            status_display = utils.map_status(None, bot) # Mặc định là offline/unknown
            roles_str = "N/A"
            join_date_str = "N/A"

            # --- Lấy thông tin chi tiết nếu là Member ---
            if isinstance(user_obj, discord.Member):
                user_display_header = f"{user_obj.mention} (`{utils.escape_markdown(user_obj.display_name)}`)"
                status_display = utils.map_status(user_obj.status, bot)
                join_date_str = utils.format_discord_time(user_obj.joined_at, 'D') if user_obj.joined_at else "Không rõ"

                # Lấy danh sách role (không bao gồm @everyone)
                member_roles = sorted([r for r in user_obj.roles if not r.is_default()], key=lambda r: r.position, reverse=True)
                role_mentions = [r.mention for r in member_roles]
                roles_str = ", ".join(role_mentions) if role_mentions else "Không có role"
                # Giới hạn độ dài chuỗi roles
                if len(roles_str) > 150: roles_str = roles_str[:150] + "..."
            elif isinstance(user_obj, discord.User):
                # Trường hợp user không còn trong server
                user_display_header = f"{user_obj.mention} (`{utils.escape_markdown(user_obj.display_name)}`) (Không trong server)"
                roles_str = "N/A (Không trong server)"

            # --- Lấy thông tin hoạt động từ data ---
            first_seen_ts = data.get('first_seen')
            last_seen_ts = data.get('last_seen')
            msg_count = data.get('message_count', 0)

            # Tính khoảng thời gian hoạt động
            activity_span = "N/A"
            if first_seen_ts and last_seen_ts and last_seen_ts >= first_seen_ts:
                try:
                    # Đảm bảo timezone-aware để tính toán chính xác
                    first_aware = first_seen_ts.astimezone(datetime.timezone.utc) if first_seen_ts.tzinfo else first_seen_ts.replace(tzinfo=datetime.timezone.utc)
                    last_aware = last_seen_ts.astimezone(datetime.timezone.utc) if last_seen_ts.tzinfo else last_seen_ts.replace(tzinfo=datetime.timezone.utc)
                    if last_aware >= first_aware:
                        activity_span = utils.format_timedelta(last_aware - first_aware)
                    else:
                        activity_span = f"{e('error')} Lỗi TG" # Trường hợp last < first?
                except Exception as ts_err:
                    log.warning(f"Lỗi tính span cho user {user_id}: {ts_err}")
                    activity_span = f"{e('error')} Lỗi TG"

            # Tạo các dòng hiển thị cho user này
            user_entry = [
                f"**{user_display_header}** ({status_display})",
                f"  ├ {e('stats')} Tin nhắn: **{msg_count:,}**",
                f"  ├ {e('calendar')} Tham gia Server: {join_date_str}",
                f"  ├ {e('calendar')} HĐ Đầu tiên: {utils.format_discord_time(first_seen_ts, 'R')} ({utils.format_discord_time(first_seen_ts, 'd')})",
                f"  ├ {e('calendar')} HĐ Cuối cùng: {utils.format_discord_time(last_seen_ts, 'R')} ({utils.format_discord_time(last_seen_ts, 'd')})",
                f"  ├ {e('role')} Roles: {roles_str}",
                f"  └ {e('clock')} Khoảng TG HĐ: **{activity_span}**"
            ]
            description_lines.extend(user_entry)
            description_lines.append("") # Thêm dòng trống giữa các user

        # Thêm vào description và giới hạn độ dài
        current_desc = activity_embed.description + "\n\n"
        new_content = "\n".join(description_lines).strip()
        if not new_content:
             activity_embed.description += "\nKhông có dữ liệu người dùng cho phần này."
        elif len(current_desc) + len(new_content) > 4000:
            remaining = 4000 - len(current_desc) - 20
            activity_embed.description = current_desc + new_content[:remaining] + "\n... (quá dài)"
        else:
            activity_embed.description = current_desc + new_content

        embeds.append(activity_embed)

    return embeds


async def create_top_active_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot],
    user_role_changes: Dict[int, Dict[str, Dict[str, int]]] # Tham số này có vẻ không cần thiết ở đây?
) -> Optional[discord.Embed]:
    """Tạo embed top N user hoạt động nhiều nhất (theo số tin nhắn)."""
    e = lambda name: utils.get_emoji(name, bot)

    # Lấy ID các admin để loại trừ khỏi bảng xếp hạng này
    admin_ids = {m.id for m in guild.members if m.guild_permissions.administrator}

    # Tạo Counter từ user_activity, lọc bot và admin
    message_counts = collections.Counter({
        uid: data['message_count']
        for uid, data in user_activity.items()
        if not data.get('is_bot', False) # Lọc bot
           and uid not in admin_ids # Lọc admin
           and data.get('message_count', 0) > 0 # Chỉ tính người có tin nhắn
    })

    if not message_counts: return None

    try:
        return await create_generic_leaderboard_embed(
            counter_data=message_counts,
            guild=guild,
            bot=bot,
            title=f"{e('stats')} Top User Hoạt Động (Tin nhắn)",
            item_name_singular="tin nhắn",
            item_name_plural="tin nhắn",
            limit=TOP_ACTIVE_USERS_LIMIT,
            color=discord.Color.orange(),
            show_total=False, # Không cần hiển thị tổng tin nhắn ở đây
            footer_note="Lọc theo số tin nhắn (kênh+luồng). Bot và Admin đã bị loại trừ."
        )
    except NameError:
         log.warning("Không thể tạo embed Top User Hoạt Động do thiếu 'create_generic_leaderboard_embed'.")
         return None
    except Exception as err:
        log.error(f"Lỗi tạo embed Top User Hoạt Động: {err}", exc_info=True)
        return None


async def create_top_oldest_members_embed(
    oldest_members_data: List[Dict[str, Any]], # Dữ liệu đã được sắp xếp
    bot: discord.Client,
    limit: int = TOP_OLDEST_MEMBERS_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed top N thành viên tham gia server lâu nhất."""
    e = lambda name: utils.get_emoji(name, bot)
    if not oldest_members_data: return None

    # Giới hạn số lượng hiển thị
    display_limit = min(limit, len(oldest_members_data))

    embed = discord.Embed(
        title=f"{e('award')}{e('calendar')} Top {display_limit} Thành viên Lâu Năm Nhất",
        description="*Dựa trên ngày tham gia server (`joined_at`). Đã lọc bot.*",
        color=discord.Color.dark_green()
    )

    desc_lines = []
    now = discord.utils.utcnow() # Lấy thời gian hiện tại (UTC) để tính toán

    for rank, data in enumerate(oldest_members_data[:limit], 1):
        joined_at = data.get('joined_at')
        time_in_server_str = "N/A"
        if isinstance(joined_at, datetime.datetime):
            try:
                # Đảm bảo joined_at là timezone-aware (UTC)
                join_aware = joined_at.astimezone(datetime.timezone.utc) if joined_at.tzinfo else joined_at.replace(tzinfo=datetime.timezone.utc)
                if now >= join_aware:
                    time_in_server_str = utils.format_timedelta(now - join_aware)
                else:
                    time_in_server_str = "Lỗi TG (Tương lai?)"
            except Exception as ts_err:
                log.warning(f"Lỗi tính time_in_server cho {data.get('id')}: {ts_err}")
                time_in_server_str = "Lỗi TG"

        user_mention = data.get('mention', f"`{data.get('id', 'N/A')}`")
        user_display = f" (`{utils.escape_markdown(data.get('display_name', 'N/A'))}`)"

        # Dòng 1: Rank và Tên User
        line1 = f"**`#{rank:02d}`**. {user_mention}{user_display}"
        # Dòng 2: Thông tin tham gia
        line2 = f"   └ {e('calendar')} Tham gia: {utils.format_discord_time(joined_at, 'D')} ({time_in_server_str})"
        desc_lines.extend([line1, line2])

    embed.description += "\n\n" + "\n".join(desc_lines)
    # Giới hạn độ dài description
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    return embed


# --- Các hàm Leaderboard khác (sử dụng generic helper) ---

async def create_top_link_posters_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    """Embed top user gửi link."""
    try:
        return await create_generic_leaderboard_embed(
            counts, guild, bot, f"{utils.get_emoji('link', bot)} Gửi Link",
            "link", "links", TOP_LINK_USERS_LIMIT, discord.Color.dark_blue()
        )
    except NameError: return None # Xử lý nếu generic helper không tồn tại

async def create_top_image_posters_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    """Embed top user gửi ảnh."""
    try:
        return await create_generic_leaderboard_embed(
            counts, guild, bot, f"{utils.get_emoji('image', bot)} Gửi Ảnh",
            "ảnh", "ảnh", TOP_IMAGE_USERS_LIMIT, discord.Color.dark_green()
        )
    except NameError: return None

async def create_top_emoji_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    """Embed top user dùng emoji trong nội dung tin nhắn."""
    try:
        return await create_generic_leaderboard_embed(
            counts, guild, bot, f"{utils.get_emoji('reaction', bot)} Dùng Emoji (Content)",
            "emoji", "emojis", TOP_EMOJI_USERS_LIMIT, discord.Color.dark_gold()
        )
    except NameError: return None

async def create_top_sticker_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    """Embed top user gửi sticker."""
    try:
        return await create_generic_leaderboard_embed(
            counts, guild, bot, f"{utils.get_emoji('sticker', bot)} Gửi Sticker",
            "sticker", "stickers", TOP_STICKER_USERS_LIMIT, discord.Color.dark_purple()
        )
    except NameError: return None

async def create_top_mentioned_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    """Embed top user được nhắc tên nhiều nhất."""
    try:
        return await create_generic_leaderboard_embed(
            counts, guild, bot, f"{utils.get_emoji('mention', bot)} Được Nhắc Tên",
            "lần", "lần", TOP_MENTIONED_USERS_LIMIT, discord.Color.purple()
        )
    except NameError: return None

async def create_top_mentioning_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    """Embed top user hay nhắc tên người khác nhất."""
    try:
        return await create_generic_leaderboard_embed(
            counts, guild, bot, f"{utils.get_emoji('mention', bot)} Hay Nhắc Tên",
            "lần nhắc", "lần nhắc", TOP_MENTIONING_USERS_LIMIT, discord.Color.dark_purple()
        )
    except NameError: return None

async def create_top_repliers_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    """Embed top user trả lời tin nhắn nhiều nhất."""
    try:
        e_reply = utils.get_emoji('reply', bot)
        return await create_generic_leaderboard_embed(
            counts, guild, bot, f"{e_reply} Trả Lời Tin Nhắn",
            "lần trả lời", "lần trả lời", TOP_REPLIERS_LIMIT, discord.Color.blue()
        )
    except NameError: return None

async def create_top_reaction_received_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    """Embed top user nhận reaction nhiều nhất."""
    try:
        return await create_generic_leaderboard_embed(
            counts, guild, bot, f"{utils.get_emoji('reaction', bot)} Nhận Reactions",
            "reaction", "reactions",
            TOP_REACTION_RECEIVED_USERS_LIMIT,
            discord.Color.gold(),
            footer_note="Yêu cầu bật Reaction Scan."
        )
    except NameError: return None


async def create_top_activity_span_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    """Embed top user có khoảng thời gian hoạt động dài nhất (từ msg đầu đến msg cuối)."""
    e = lambda name: utils.get_emoji(name, bot)
    user_spans: List[Tuple[int, datetime.timedelta]] = []

    # Tính toán span cho từng user (không phải bot)
    for user_id, data in user_activity.items():
        if data.get('is_bot', False): continue
        first_seen = data.get('first_seen')
        last_seen = data.get('last_seen')
        if first_seen and last_seen and last_seen >= first_seen:
            try:
                # Đảm bảo timezone-aware
                first_aware = first_seen.astimezone(datetime.timezone.utc) if first_seen.tzinfo else first_seen.replace(tzinfo=datetime.timezone.utc)
                last_aware = last_seen.astimezone(datetime.timezone.utc) if last_seen.tzinfo else last_seen.replace(tzinfo=datetime.timezone.utc)
                if last_aware >= first_aware:
                    span = last_aware - first_aware
                    # Chỉ thêm nếu span > 0 (tránh user chỉ có 1 tin nhắn)
                    if span.total_seconds() > 0:
                        user_spans.append((user_id, span))
            except Exception as ts_err:
                 log.warning(f"Lỗi tính span cho user {user_id} (leaderboard): {ts_err}")

    if not user_spans: return None

    # Sắp xếp theo span giảm dần (dài nhất trước)
    user_spans.sort(key=lambda item: item[1], reverse=True)

    embed = discord.Embed(
        title=f"{e('award')}{e('clock')} Top User Hoạt Động Lâu Nhất (Span)",
        description=f"*Dựa trên khoảng TG giữa tin nhắn đầu và cuối trong lần quét. Đã lọc bot.*",
        color=discord.Color.dark_grey()
    )

    # Fetch user info và tạo danh sách hiển thị
    limit = TOP_ACTIVITY_SPAN_USERS_LIMIT
    user_ids_to_fetch = [uid for uid, span in user_spans[:limit]]
    log.debug(f"Fetching {len(user_ids_to_fetch)} users for activity span leaderboard...")
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    fetch_tasks = [utils.fetch_user_data(guild, user_id, bot_ref=bot) for user_id in user_ids_to_fetch]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    for idx, result in enumerate(results):
        user_id = user_ids_to_fetch[idx]
        if isinstance(result, (discord.User, discord.Member)): user_cache[user_id] = result
        else: user_cache[user_id] = None
        if isinstance(result, Exception): log.warning(f"Lỗi fetch user {user_id} cho activity span embed: {result}")
    log.debug("Fetch user hoàn thành cho activity span leaderboard.")

    desc_lines = []
    for rank, (user_id, span) in enumerate(user_spans[:limit], 1):
        user_obj = user_cache.get(user_id)
        user_mention = user_obj.mention if user_obj else f"`{user_id}`"
        user_display = f" (`{utils.escape_markdown(user_obj.display_name)}`)" if user_obj else " (Unknown/Left)"
        span_str = utils.format_timedelta(span) # Định dạng timedelta

        desc_lines.append(f"**`#{rank:02d}`**. {user_mention}{user_display} — **{span_str}**")

    if len(user_spans) > limit:
        desc_lines.append(f"\n... và {len(user_spans) - limit} người dùng khác.")

    embed.description += "\n\n" + "\n".join(desc_lines)
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    return embed


async def create_top_thread_creators_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    """Embed top user tạo thread (dựa trên Audit Log)."""
    try:
        return await create_generic_leaderboard_embed(
            counts, guild, bot,
            f"{utils.get_emoji('thread', bot)} Tạo Thread",
            "thread", "threads",
            TOP_THREAD_CREATORS_LIMIT,
            discord.Color.dark_magenta(),
            footer_note="Yêu cầu quyền View Audit Log và theo dõi thread_create."
        )
    except NameError: return None

# --- END OF FILE reporting/embeds_user.py ---
