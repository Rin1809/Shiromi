# --- START OF FILE reporting/embeds_user.py ---
import discord
import datetime
import math
import logging
import collections
import asyncio
from typing import List, Dict, Any, Optional, Union, Tuple, Set
from discord.ext import commands

try:
    from .. import utils
    from .. import config
except ImportError:
    import utils
    import config

log = logging.getLogger(__name__)

# --- Constants ---
TOP_ACTIVE_USERS_LIMIT = 30
TOP_OLDEST_MEMBERS_LIMIT = 30
TOP_LINK_USERS_LIMIT = 30
TOP_IMAGE_USERS_LIMIT = 30
TOP_EMOJI_USERS_LIMIT = 30 # Custom emoji content
TOP_STICKER_USERS_LIMIT = 30
TOP_MENTIONED_USERS_LIMIT = 30
TOP_MENTIONING_USERS_LIMIT = 30
TOP_REPLIERS_LIMIT = 30
TOP_REACTION_RECEIVED_USERS_LIMIT = 30
TOP_ACTIVITY_SPAN_USERS_LIMIT = 30
TOP_THREAD_CREATORS_LIMIT = 30
TOP_DISTINCT_CHANNEL_USERS_LIMIT = 30 # Giới hạn BXH Đa năng


# --- Hàm Helper Tạo Embed Leaderboard Chung (Cập nhật để lọc admin) ---
async def create_generic_leaderboard_embed(
    counter_data: collections.Counter,
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot],
    title: str,
    item_name_singular: str,
    item_name_plural: str,
    limit: int,
    color: discord.Color = discord.Color.blue(),
    show_total: bool = True,
    footer_note: Optional[str] = None,
    filter_admins: bool = True # <<< THÊM Cờ lọc admin
) -> Optional[discord.Embed]:
    """
    Hàm chung để tạo embed leaderboard cho user dựa trên dữ liệu Counter.
    Tự động fetch user info và định dạng hiển thị. Có tùy chọn lọc admin.
    """
    e = lambda name: utils.get_emoji(name, bot)
    if not counter_data:
        log.debug(f"Bỏ qua tạo leaderboard '{title}': Không có dữ liệu counter.")
        return None

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        # Lấy ID admin từ quyền và từ config
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID:
             admin_ids_to_filter.add(config.ADMIN_USER_ID)

    # Lọc và sắp xếp dữ liệu
    filtered_sorted_users = [
        (uid, count) for uid, count in counter_data.most_common()
        # <<< FIX: Đảm bảo uid là int trước khi kiểm tra lọc admin >>>
        if count > 0 and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
    ]

    if not filtered_sorted_users:
         log.debug(f"Bỏ qua tạo leaderboard '{title}': Không có dữ liệu sau khi lọc.")
         return None # Không còn user nào sau khi lọc

    total_items = sum(count for uid, count in filtered_sorted_users) if show_total else 0
    total_users_in_lb = len(filtered_sorted_users) # Tổng số user sau khi lọc

    embed = discord.Embed(title=f"{e('award')} {title}", color=color)

    description_lines = []
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin."
    description_lines.append(desc_prefix)

    if show_total:
        description_lines.append(f"*Tổng cộng (sau lọc): **{total_items:,}** {item_name_plural} từ {total_users_in_lb} user.*")

    # Lấy top N user để hiển thị
    users_to_display = filtered_sorted_users[:limit]

    # Fetch thông tin user cho top N và cache lại
    # <<< FIX: Chỉ fetch nếu user_id là int >>>
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    log.debug(f"Fetching {len(user_ids_to_fetch)} users for leaderboard '{title}'...")
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    if user_ids_to_fetch: # Chỉ fetch nếu có ID hợp lệ
        fetch_tasks = [utils.fetch_user_data(guild, user_id, bot_ref=bot) for user_id in user_ids_to_fetch]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        for idx, result in enumerate(results):
            user_id = user_ids_to_fetch[idx]
            if isinstance(result, (discord.User, discord.Member)): user_cache[user_id] = result
            else: user_cache[user_id] = None
            if isinstance(result, Exception): log.warning(f"Lỗi fetch user {user_id} cho leaderboard '{title}': {result}")
        log.debug(f"Fetch user hoàn thành cho leaderboard '{title}'.")

    leaderboard_lines = []
    for rank, (user_id, count) in enumerate(users_to_display, 1):
        user_obj = None
        if isinstance(user_id, int): # Chỉ tìm trong cache nếu ID là int
            user_obj = user_cache.get(user_id)

        # <<< FIX: Xử lý hiển thị cho ID không phải int hoặc không fetch được >>>
        if user_obj:
            user_mention = user_obj.mention
            user_display = f" (`{utils.escape_markdown(user_obj.display_name)}`)"
        elif isinstance(user_id, int):
            user_mention = f"`{user_id}`"
            user_display = " (Unknown/Left)"
        else: # Trường hợp key không phải int (vd: sticker ID)
            user_mention = f"`{utils.escape_markdown(str(user_id))}`" # Hiển thị key gốc
            user_display = ""
        # <<< END FIX >>>

        item_name = item_name_plural if count != 1 else item_name_singular
        leaderboard_lines.append(f"**`#{rank:02d}`**. {user_mention}{user_display} — **{count:,}** {item_name}")

    description_lines.append("\n" + "\n".join(leaderboard_lines))

    if total_users_in_lb > limit:
        description_lines.append(f"\n... và {total_users_in_lb - limit} người dùng khác.")

    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4000: embed.description = embed.description[:4000] + "\n... (quá dài)"
    if footer_note: embed.set_footer(text=footer_note)

    return embed


# --- Các hàm tạo Embed User cụ thể ---

async def create_top_active_users_embed(
    user_activity: Dict[int, Dict[str, Any]], # Cần dữ liệu gốc để tạo Counter
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    """Tạo embed top N user hoạt động nhiều nhất (theo số tin nhắn). Lọc admin."""
    e = lambda name: utils.get_emoji(name, bot)

    # Tạo Counter từ user_activity
    message_counts = collections.Counter({
        uid: data['message_count']
        for uid, data in user_activity.items()
        if not data.get('is_bot', False) and data.get('message_count', 0) > 0
    })

    if not message_counts: return None

    try:
        return await create_generic_leaderboard_embed(
            counter_data=message_counts,
            guild=guild, bot=bot,
            title=f"{e('stats')} Top User Gửi Tin Nhắn", # Đổi title rõ hơn
            item_name_singular="tin nhắn", item_name_plural="tin nhắn",
            limit=TOP_ACTIVE_USERS_LIMIT,
            color=discord.Color.orange(),
            show_total=False, # Không cần tổng số tin nhắn ở đây
            filter_admins=True # <<< Lọc admin ở đây
        )
    except NameError: log.warning("Không thể tạo embed Top User Gửi Tin Nhắn do thiếu 'create_generic_leaderboard_embed'."); return None
    except Exception as err: log.error(f"Lỗi tạo embed Top User Gửi Tin Nhắn: {err}", exc_info=True); return None

async def create_top_oldest_members_embed(
    oldest_members_data: List[Dict[str, Any]],
    bot: discord.Client,
    limit: int = TOP_OLDEST_MEMBERS_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed top N thành viên tham gia server lâu nhất (Giữ nguyên logic)."""
    e = lambda name: utils.get_emoji(name, bot)
    if not oldest_members_data: return None
    display_limit = min(limit, len(oldest_members_data))
    embed = discord.Embed(
        title=f"{e('award')}{e('calendar')} Top {display_limit} Thành viên Lâu Năm Nhất",
        description="*Dựa trên ngày tham gia server (`joined_at`). Đã lọc bot.*",
        color=discord.Color.dark_green()
    )
    desc_lines = []
    now = discord.utils.utcnow()
    for rank, data in enumerate(oldest_members_data[:limit], 1):
        joined_at = data.get('joined_at')
        time_in_server_str = "N/A"
        if isinstance(joined_at, datetime.datetime):
            try:
                join_aware = joined_at.astimezone(datetime.timezone.utc) if joined_at.tzinfo else joined_at.replace(tzinfo=datetime.timezone.utc)
                if now >= join_aware: time_in_server_str = utils.format_timedelta(now - join_aware)
                else: time_in_server_str = "Lỗi TG (Tương lai?)"
            except Exception as ts_err: log.warning(f"Lỗi tính time_in_server cho {data.get('id')}: {ts_err}"); time_in_server_str = "Lỗi TG"
        user_mention = data.get('mention', f"`{data.get('id', 'N/A')}`")
        user_display = f" (`{utils.escape_markdown(data.get('display_name', 'N/A'))}`)"
        line1 = f"**`#{rank:02d}`**. {user_mention}{user_display}"
        line2 = f"   └ {e('calendar')} Tham gia: {utils.format_discord_time(joined_at, 'D')} ({time_in_server_str})"
        desc_lines.extend([line1, line2])
    embed.description += "\n\n" + "\n".join(desc_lines)
    if len(embed.description) > 4000: embed.description = embed.description[:4000] + "\n... (quá dài)"
    return embed

# --- Các hàm Leaderboard khác (sử dụng generic helper, có filter_admins) ---

async def create_top_link_posters_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    try: return await create_generic_leaderboard_embed(counts, guild, bot, f"{utils.get_emoji('link', bot)} Gửi Link", "link", "links", TOP_LINK_USERS_LIMIT, discord.Color.dark_blue(), filter_admins=True)
    except NameError: return None

async def create_top_image_posters_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    try: return await create_generic_leaderboard_embed(counts, guild, bot, f"{utils.get_emoji('image', bot)} Gửi Ảnh", "ảnh", "ảnh", TOP_IMAGE_USERS_LIMIT, discord.Color.dark_green(), filter_admins=True)
    except NameError: return None

async def create_top_custom_emoji_users_embed( # Đổi tên hàm rõ hơn
    counts: collections.Counter, # Counter {user_id: {emoji_id: count}} hoặc Counter {user_id: total_count}
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    """Embed top user dùng custom emoji của server trong nội dung tin nhắn."""
    # <<< FIX: Xử lý cả 2 dạng input counter >>>
    user_total_counts = None
    if counts:
        # Kiểm tra xem value đầu tiên là dict hay int để xác định loại counter
        first_value = next(iter(counts.values()), None)
        if isinstance(first_value, dict) or isinstance(first_value, collections.Counter):
            # Input là {user_id: {emoji_id: count}}
            user_total_counts = collections.Counter({uid: sum(ecounts.values()) for uid, ecounts in counts.items() if sum(ecounts.values()) > 0})
        elif isinstance(first_value, int):
             # Input đã là {user_id: total_count}
             user_total_counts = counts
    # <<< END FIX >>>

    if not user_total_counts: return None
    try:
        return await create_generic_leaderboard_embed(
            user_total_counts, guild, bot,
            f"{utils.get_emoji('mention', bot)} Dùng Custom Emoji Server (Content)",
            "emoji", "emojis", TOP_EMOJI_USERS_LIMIT, discord.Color.dark_gold(), filter_admins=True
        )
    except NameError: return None

async def create_top_sticker_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    try: return await create_generic_leaderboard_embed(counts, guild, bot, f"{utils.get_emoji('sticker', bot)} Gửi Sticker", "sticker", "stickers", TOP_STICKER_USERS_LIMIT, discord.Color.dark_purple(), filter_admins=True)
    except NameError: return None

async def create_top_mentioned_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    try: return await create_generic_leaderboard_embed(counts, guild, bot, f"{utils.get_emoji('mention', bot)} Được Nhắc Tên", "lần", "lần", TOP_MENTIONED_USERS_LIMIT, discord.Color.purple(), filter_admins=False) # Không lọc admin ở đây
    except NameError: return None

async def create_top_mentioning_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    try: return await create_generic_leaderboard_embed(counts, guild, bot, f"{utils.get_emoji('mention', bot)} Hay Nhắc Tên", "lần nhắc", "lần nhắc", TOP_MENTIONING_USERS_LIMIT, discord.Color.dark_purple(), filter_admins=True)
    except NameError: return None

async def create_top_repliers_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    try: return await create_generic_leaderboard_embed(counts, guild, bot, f"{utils.get_emoji('reply', bot)} Trả Lời Tin Nhắn", "lần trả lời", "lần trả lời", TOP_REPLIERS_LIMIT, discord.Color.blue(), filter_admins=True)
    except NameError: return None

async def create_top_reaction_received_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    try:
        return await create_generic_leaderboard_embed(
            counts, guild, bot, f"{utils.get_emoji('reaction', bot)} Nhận Reactions", "reaction", "reactions",
            TOP_REACTION_RECEIVED_USERS_LIMIT, discord.Color.gold(),
            footer_note="Yêu cầu bật Reaction Scan.", filter_admins=False # Không lọc admin
        )
    except NameError: return None

async def create_top_distinct_channel_users_embed( # <<< THÊM MỚI
    counts: collections.Counter, # Counter {user_id: distinct_channel_count}
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    """Embed top user hoạt động trong nhiều kênh/luồng khác nhau nhất."""
    try:
        return await create_generic_leaderboard_embed(
            counts, guild, bot,
            f"🗺️ \"Người Đa Năng\"",
            "kênh/luồng", "kênh/luồng khác nhau",
            TOP_DISTINCT_CHANNEL_USERS_LIMIT,
            discord.Color.dark_teal(),
            show_total=False,
            filter_admins=True
        )
    except NameError: return None

async def create_top_activity_span_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    """Embed top user có khoảng thời gian hoạt động dài nhất (Giữ nguyên logic)."""
    e = lambda name: utils.get_emoji(name, bot)
    user_spans: List[Tuple[int, datetime.timedelta]] = []
    for user_id, data in user_activity.items():
        if data.get('is_bot', False): continue
        # <<< FIX: Lấy activity_span_seconds đã tính toán trước đó >>>
        span_seconds = data.get('activity_span_seconds', 0.0)
        if span_seconds > 0:
             user_spans.append((user_id, datetime.timedelta(seconds=span_seconds)))
        # <<< END FIX >>>
    if not user_spans: return None
    user_spans.sort(key=lambda item: item[1], reverse=True)
    embed = discord.Embed(title=f"{e('award')}{e('clock')} Top User Hoạt Động Lâu Nhất (Span)", description=f"*Dựa trên khoảng TG giữa tin nhắn đầu và cuối trong lần quét. Đã lọc bot.*", color=discord.Color.dark_grey())
    limit = TOP_ACTIVITY_SPAN_USERS_LIMIT
    user_ids_to_fetch = [uid for uid, span in user_spans[:limit]]
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    fetch_tasks = [utils.fetch_user_data(guild, user_id, bot_ref=bot) for user_id in user_ids_to_fetch]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    for idx, result in enumerate(results):
        user_id = user_ids_to_fetch[idx]
        if isinstance(result, (discord.User, discord.Member)): user_cache[user_id] = result
        else: user_cache[user_id] = None
        if isinstance(result, Exception): log.warning(f"Lỗi fetch user {user_id} cho activity span embed: {result}")
    desc_lines = []
    for rank, (user_id, span) in enumerate(user_spans[:limit], 1):
        user_obj = user_cache.get(user_id)
        user_mention = user_obj.mention if user_obj else f"`{user_id}`"
        user_display = f" (`{utils.escape_markdown(user_obj.display_name)}`)" if user_obj else " (Unknown/Left)"
        span_str = utils.format_timedelta(span)
        desc_lines.append(f"**`#{rank:02d}`**. {user_mention}{user_display} — **{span_str}**")
    if len(user_spans) > limit: desc_lines.append(f"\n... và {len(user_spans) - limit} người dùng khác.")
    embed.description += "\n\n" + "\n".join(desc_lines)
    if len(embed.description) > 4000: embed.description = embed.description[:4000] + "\n... (quá dài)"
    return embed


async def create_top_thread_creators_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    try:
        return await create_generic_leaderboard_embed(
            counts, guild, bot, f"{utils.get_emoji('thread', bot)} Tạo Thread", "thread", "threads",
            TOP_THREAD_CREATORS_LIMIT, discord.Color.dark_magenta(),
            footer_note="Yêu cầu quyền View Audit Log và theo dõi thread_create.", filter_admins=True
        )
    except NameError: return None

async def create_top_booster_embed( # <<< THÊM MỚI
    boosters: List[discord.Member], # Danh sách booster đã sắp xếp theo tgian boost
    bot: discord.Client,
    scan_end_time: datetime.datetime
) -> Optional[discord.Embed]:
    """Tạo embed top booster bền bỉ."""
    e = lambda name: utils.get_emoji(name, bot)
    if not boosters: return None
    limit = 15 # Giới hạn hiển thị

    embed = discord.Embed(
        title=f"{e('award')} {e('boost')} Top Booster Bền Bỉ",
        description="*Sắp xếp theo thời gian boost server lâu nhất.*",
        color=discord.Color(0xf47fff) # Màu hồng boost
    )
    desc_lines = []
    for rank, member in enumerate(boosters[:limit], 1):
        boost_duration_str = "N/A"
        if member.premium_since:
            try:
                # <<< FIX: Đảm bảo cả 2 thời gian đều aware UTC >>>
                scan_end_time_aware = scan_end_time if scan_end_time.tzinfo else scan_end_time.replace(tzinfo=datetime.timezone.utc)
                premium_since_aware = member.premium_since if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc)
                # <<< END FIX >>>
                if scan_end_time_aware >= premium_since_aware:
                    boost_duration = scan_end_time_aware - premium_since_aware
                    boost_duration_str = utils.format_timedelta(boost_duration)
                else: boost_duration_str = "Lỗi TG (Tương lai?)"
            except Exception as td_err: log.warning(f"Lỗi tính thời gian boost cho {member.id}: {td_err}"); boost_duration_str = "Lỗi TG"

        user_display = f" (`{utils.escape_markdown(member.display_name)}`)"
        desc_lines.append(f"**`#{rank:02d}`**. {member.mention}{user_display} — **{boost_duration_str}**")

    if len(boosters) > limit:
        desc_lines.append(f"\n... và {len(boosters) - limit} booster khác.")

    embed.description += "\n\n" + "\n".join(desc_lines)
    if len(embed.description) > 4000: embed.description = embed.description[:4000] + "\n... (quá dài)"
    return embed

# --- END OF FILE reporting/embeds_user.py ---