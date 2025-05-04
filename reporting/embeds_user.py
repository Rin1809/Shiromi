# --- START OF FILE reporting/embeds_user.py ---
import discord
import datetime
import math
import logging
import collections
import asyncio
from typing import List, Dict, Any, Optional, Union, Tuple, Set
from discord.ext import commands
from collections import Counter, defaultdict # Đảm bảo có defaultdict

try:
    from .. import utils
    from .. import config
except ImportError:
    import utils
    import config

log = logging.getLogger(__name__)

# --- Constants ---
TOP_ACTIVE_USERS_LIMIT = 30
TOP_OLDEST_MEMBERS_LIMIT = 30 # Giữ nguyên hoặc điều chỉnh nếu muốn
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
TOP_DISTINCT_CHANNEL_USERS_LIMIT = 20 # Giới hạn top 20


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
    filter_admins: bool = True # Thêm Cờ lọc admin
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
        # Đảm bảo uid là int trước khi kiểm tra lọc admin
        if count > 0 and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True) # Lọc bot lần nữa cho chắc
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
    # Chỉ fetch nếu user_id là int
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    log.debug(f"Fetching {len(user_ids_to_fetch)} users for leaderboard '{title}'...")
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    if user_ids_to_fetch: # Chỉ fetch nếu có ID hợp lệ
        user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot) # Sử dụng helper mới từ utils
        log.debug(f"Fetch user hoàn thành cho leaderboard '{title}'.")

    leaderboard_lines = []
    for rank, (user_id, count) in enumerate(users_to_display, 1):
        user_obj = None
        if isinstance(user_id, int): # Chỉ tìm trong cache nếu ID là int
            user_obj = user_cache.get(user_id)

        # Xử lý hiển thị cho ID không phải int hoặc không fetch được
        if user_obj:
            user_mention = user_obj.mention
            user_display = f" (`{utils.escape_markdown(user_obj.display_name)}`)"
        elif isinstance(user_id, int):
            user_mention = f"`{user_id}`"
            user_display = " (Unknown/Left)"
        else: # Trường hợp key không phải int (vd: sticker ID)
            user_mention = f"`{utils.escape_markdown(str(user_id))}`" # Hiển thị key gốc
            user_display = ""

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

    # Sử dụng helper generic để tránh lặp code
    return await create_generic_leaderboard_embed(
        counter_data=message_counts,
        guild=guild, bot=bot,
        title=f"{e('stats')} Top User Gửi Tin Nhắn",
        item_name_singular="tin nhắn", item_name_plural="tin nhắn",
        limit=TOP_ACTIVE_USERS_LIMIT,
        color=discord.Color.orange(),
        show_total=False,
        filter_admins=True
    )


# <<< ***ĐẢM BẢO HÀM NÀY CÓ ĐÚNG CHỮ KÝ*** >>>
async def create_top_oldest_members_embed(
    oldest_members_data: List[Dict[str, Any]],
    scan_data: Dict[str, Any], # ***Tham số này PHẢI CÓ***
    bot: discord.Client,
    limit: int = TOP_OLDEST_MEMBERS_LIMIT # Có thể thêm lại limit nếu cần
) -> Optional[discord.Embed]:
    """Tạo embed top N thành viên tham gia server lâu nhất, hiển thị thêm thời gian tin nhắn đầu/cuối từ scan."""
    e = lambda name: utils.get_emoji(name, bot)
    if not oldest_members_data: return None

    user_activity = scan_data.get("user_activity", {}) # <<< Lấy user_activity

    display_limit = min(limit, len(oldest_members_data))
    embed = discord.Embed(
        title=f"{e('award')}{e('calendar')} Top {display_limit} Thành viên Lâu Năm Nhất",
        description="*Dựa trên ngày tham gia server (`joined_at`). Đã lọc bot.\nMsg đầu/cuối tính theo thời gian quét.*",
        color=discord.Color.dark_green()
    )
    desc_lines = []
    now = discord.utils.utcnow()

    # Fetch user data MỘT LẦN cho những người trong list
    user_ids_to_fetch = [d.get('id') for d in oldest_members_data[:limit] if d.get('id')]
    # Cần guild để fetch, lấy từ user đầu tiên nếu có hoặc từ bot nếu là Client
    guild_for_fetch = None
    if oldest_members_data:
        first_user_id = oldest_members_data[0].get('id')
        if first_user_id:
            member_check = bot.get_user(first_user_id) # Thử lấy từ cache bot
            if member_check and hasattr(member_check, 'guild'):
                guild_for_fetch = member_check.guild
            elif isinstance(bot, commands.Bot): # Nếu là Bot class, thử tìm guild
                 for g in bot.guilds:
                     if g.get_member(first_user_id):
                         guild_for_fetch = g
                         break
    user_cache = await utils._fetch_user_dict(guild_for_fetch, user_ids_to_fetch, bot) if guild_for_fetch and user_ids_to_fetch else {}


    for rank, data in enumerate(oldest_members_data[:limit], 1):
        user_id = data.get('id')
        if not user_id: continue

        user_mention = data.get('mention', f"`{user_id}`")
        user_display_name = data.get('display_name', 'N/A')
        user_display = f" (`{utils.escape_markdown(user_display_name)}`)"

        joined_at = data.get('joined_at')
        time_in_server_str = "N/A"
        if isinstance(joined_at, datetime.datetime):
            try:
                join_aware = joined_at.astimezone(datetime.timezone.utc) if joined_at.tzinfo else joined_at.replace(tzinfo=datetime.timezone.utc)
                if now >= join_aware: time_in_server_str = utils.format_timedelta(now - join_aware)
                else: time_in_server_str = "Lỗi TG (Tương lai?)"
            except Exception as ts_err: log.warning(f"Lỗi tính time_in_server cho {user_id}: {ts_err}"); time_in_server_str = "Lỗi TG"

        # Lấy dữ liệu first/last seen từ user_activity
        activity_data = user_activity.get(user_id)
        first_seen = activity_data.get('first_seen') if activity_data else None
        last_seen = activity_data.get('last_seen') if activity_data else None

        # Định dạng thời gian first/last seen
        first_msg_time_str = utils.format_discord_time(first_seen, 'f') if first_seen else "Không ghi nhận"
        last_msg_time_str = utils.format_discord_time(last_seen, 'R') if last_seen else "Không ghi nhận"

        # Tạo các dòng hiển thị
        line1 = f"**`#{rank:02d}`**. {user_mention}{user_display}"
        line2 = f"   ├ {e('calendar')} Tham gia: {utils.format_discord_time(joined_at, 'D')} ({time_in_server_str})"
        line3 = f"   ├ {e('clock')} Msg đầu (scan): {first_msg_time_str}"
        line4 = f"   └ {e('clock')} Msg cuối (scan): {last_msg_time_str}"
        desc_lines.extend([line1, line2, line3, line4])

    embed.description += "\n" + "\n".join(desc_lines)

    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    return embed
# <<< ***KẾT THÚC KIỂM TRA HÀM NÀY*** >>>


# --- Các hàm Leaderboard khác (sử dụng generic helper, có filter_admins) ---

async def create_top_link_posters_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    return await create_generic_leaderboard_embed(counts, guild, bot, f"{utils.get_emoji('link', bot)} Gửi Link", "link", "links", TOP_LINK_USERS_LIMIT, discord.Color.dark_blue(), filter_admins=True)

async def create_top_image_posters_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    return await create_generic_leaderboard_embed(counts, guild, bot, f"{utils.get_emoji('image', bot)} Gửi Ảnh", "ảnh", "ảnh", TOP_IMAGE_USERS_LIMIT, discord.Color.dark_green(), filter_admins=True)

# --- VIẾT LẠI HÀM NÀY ---
async def create_top_custom_emoji_users_embed(
    scan_data: Dict[str, Any], # Nhận scan_data
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    """Embed top user dùng custom emoji của server trong nội dung tin nhắn, hiển thị emoji dùng nhiều nhất."""
    e = lambda name: utils.get_emoji(name, bot)
    limit = TOP_EMOJI_USERS_LIMIT
    filter_admins = True

    user_detailed_counts: Dict[int, Counter] = scan_data.get("user_custom_emoji_content_counts", {})
    user_total_counts = collections.Counter({
        uid: sum(ecounts.values())
        for uid, ecounts in user_detailed_counts.items()
        if sum(ecounts.values()) > 0
    })

    if not user_total_counts:
        log.debug("Bỏ qua tạo Top Custom Emoji Users embed: Không có dữ liệu.")
        return None

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, total_count) for uid, total_count in user_total_counts.most_common()
        if (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]

    if not filtered_sorted_users:
         log.debug("Bỏ qua tạo Top Custom Emoji Users embed: Không còn user sau khi lọc.")
         return None

    total_emojis_after_filter = sum(count for uid, count in filtered_sorted_users)
    total_users_in_lb = len(filtered_sorted_users)

    embed = discord.Embed(
        title=f"{e('award')} {e('mention')} Top User Dùng Custom Emoji Server (Content)",
        color=discord.Color.dark_gold()
    )
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin."
    desc_lines = [
        desc_prefix,
        f"*Tổng cộng (sau lọc): **{total_emojis_after_filter:,}** emojis từ {total_users_in_lb} user.*"
    ]

    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    leaderboard_lines = []
    emoji_cache: Dict[int, discord.Emoji] = scan_data.get("server_emojis_cache", {})

    for rank, (user_id, total_count) in enumerate(users_to_display, 1):
        user_obj = user_cache.get(user_id)
        user_mention = user_obj.mention if user_obj else f"`{user_id}`"
        user_display = f" (`{utils.escape_markdown(user_obj.display_name)}`)" if user_obj else " (Unknown/Left)"

        most_used_emoji_str = ""
        user_specific_counts = user_detailed_counts.get(user_id, Counter())
        if user_specific_counts:
            try:
                most_used_id, _ = max(user_specific_counts.items(), key=lambda item: item[1])
                emoji_obj = emoji_cache.get(most_used_id) or bot.get_emoji(most_used_id)
                if emoji_obj:
                    most_used_emoji_str = f"(Top: {str(emoji_obj)})"
                else:
                    most_used_emoji_str = f"(Top ID: `{most_used_id}`)"
            except ValueError: pass
            except Exception as e_find: log.warning(f"Lỗi tìm top emoji cho user {user_id}: {e_find}")

        leaderboard_lines.append(f"**`#{rank:02d}`**. {user_mention}{user_display} — **{total_count:,}** emojis {most_used_emoji_str}".strip())

    desc_lines.append("\n" + "\n".join(leaderboard_lines))

    if total_users_in_lb > limit:
        desc_lines.append(f"\n... và {total_users_in_lb - limit} người dùng khác.")

    embed.description = "\n".join(desc_lines)
    if len(embed.description) > 4000: embed.description = embed.description[:4000] + "\n... (quá dài)"
    return embed

# --- VIẾT LẠI HÀM NÀY ---
async def create_top_sticker_users_embed(
    scan_data: Dict[str, Any], # Nhận scan_data
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    """Embed top user gửi sticker, hiển thị sticker dùng nhiều nhất."""
    e = lambda name: utils.get_emoji(name, bot)
    limit = TOP_STICKER_USERS_LIMIT
    filter_admins = True

    user_detailed_counts: Dict[int, Counter] = scan_data.get("user_sticker_id_counts", {})
    user_total_counts: Counter = scan_data.get("user_sticker_counts", Counter())

    if not user_total_counts:
        log.debug("Bỏ qua tạo Top Sticker Users embed: Không có dữ liệu.")
        return None

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, total_count) for uid, total_count in user_total_counts.most_common()
        if total_count > 0
           and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]

    if not filtered_sorted_users:
         log.debug("Bỏ qua tạo Top Sticker Users embed: Không còn user sau khi lọc.")
         return None

    total_stickers_after_filter = sum(count for uid, count in filtered_sorted_users)
    total_users_in_lb = len(filtered_sorted_users)

    embed = discord.Embed(
        title=f"{e('award')} {e('sticker')} Top User Gửi Sticker",
        color=discord.Color.dark_purple()
    )
    desc_prefix = "*Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin."
    desc_lines = [
        desc_prefix,
        f"*Tổng cộng (sau lọc): **{total_stickers_after_filter:,}** stickers từ {total_users_in_lb} user.*"
    ]

    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    sticker_ids_to_fetch_names = set()
    for user_id, _ in users_to_display:
        user_specific_counts = user_detailed_counts.get(user_id, Counter())
        if user_specific_counts:
            try:
                most_used_id_str, _ = max(user_specific_counts.items(), key=lambda item: item[1])
                if most_used_id_str.isdigit():
                    sticker_ids_to_fetch_names.add(int(most_used_id_str))
            except ValueError: pass

    sticker_name_cache: Dict[int, str] = {}
    if sticker_ids_to_fetch_names:
        log.debug(f"Fetching {len(sticker_ids_to_fetch_names)} sticker names for top sticker embed...")
        async def fetch_sticker_name(sticker_id):
            try:
                sticker = await bot.fetch_sticker(sticker_id)
                return sticker_id, sticker.name if sticker else None
            except Exception: return sticker_id, None
        fetch_name_tasks = [fetch_sticker_name(sid) for sid in sticker_ids_to_fetch_names]
        results = await asyncio.gather(*fetch_name_tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, tuple):
                sid, name = res
                sticker_name_cache[sid] = name if name else "Unknown/Deleted"
        log.debug("Fetch sticker names complete.")

    leaderboard_lines = []
    for rank, (user_id, total_count) in enumerate(users_to_display, 1):
        user_obj = user_cache.get(user_id)
        user_mention = user_obj.mention if user_obj else f"`{user_id}`"
        user_display = f" (`{utils.escape_markdown(user_obj.display_name)}`)" if user_obj else " (Unknown/Left)"

        most_used_sticker_str = ""
        user_specific_counts = user_detailed_counts.get(user_id, Counter())
        if user_specific_counts:
            try:
                most_used_id_str, _ = max(user_specific_counts.items(), key=lambda item: item[1])
                if most_used_id_str.isdigit():
                    sticker_id = int(most_used_id_str)
                    sticker_name = sticker_name_cache.get(sticker_id, "Loading...")
                    most_used_sticker_str = f"(Top: '{utils.escape_markdown(sticker_name)}')"
                else:
                     most_used_sticker_str = f"(Top ID: `{most_used_id_str}`)"
            except ValueError: pass
            except Exception as e_find: log.warning(f"Lỗi tìm top sticker cho user {user_id}: {e_find}")

        leaderboard_lines.append(f"**`#{rank:02d}`**. {user_mention}{user_display} — **{total_count:,}** stickers {most_used_sticker_str}".strip())

    desc_lines.append("\n" + "\n".join(leaderboard_lines))

    if total_users_in_lb > limit:
        desc_lines.append(f"\n... và {total_users_in_lb - limit} người dùng khác.")

    embed.description = "\n".join(desc_lines)
    if len(embed.description) > 4000: embed.description = embed.description[:4000] + "\n... (quá dài)"
    return embed

async def create_top_mentioned_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    return await create_generic_leaderboard_embed(counts, guild, bot, f"{utils.get_emoji('mention', bot)} Được Nhắc Tên", "lần", "lần", TOP_MENTIONED_USERS_LIMIT, discord.Color.purple(), filter_admins=False)

async def create_top_mentioning_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    return await create_generic_leaderboard_embed(counts, guild, bot, f"{utils.get_emoji('mention', bot)} Hay Nhắc Tên", "lần nhắc", "lần nhắc", TOP_MENTIONING_USERS_LIMIT, discord.Color.dark_purple(), filter_admins=True)

async def create_top_repliers_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    return await create_generic_leaderboard_embed(counts, guild, bot, f"{utils.get_emoji('reply', bot)} Trả Lời Tin Nhắn", "lần trả lời", "lần trả lời", TOP_REPLIERS_LIMIT, discord.Color.blue(), filter_admins=True)

async def create_top_reaction_received_users_embed(counts: collections.Counter, guild: discord.Guild, bot: Union[discord.Client, commands.Bot]) -> Optional[discord.Embed]:
    return await create_generic_leaderboard_embed(
        counts, guild, bot, f"{utils.get_emoji('reaction', bot)} Nhận Reactions", "reaction", "reactions",
        TOP_REACTION_RECEIVED_USERS_LIMIT, discord.Color.gold(),
        footer_note="Yêu cầu bật Reaction Scan.", filter_admins=False
    )

# --- THAY THẾ HOÀN TOÀN HÀM NÀY ---
async def create_top_distinct_channel_users_embed(
    scan_data: Dict[str, Any], # Nhận scan_data
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    """Embed top user hoạt động trong nhiều kênh/luồng khác nhau nhất, hiển thị top 3 kênh/luồng dạng cây."""
    e = lambda name: utils.get_emoji(name, bot)
    limit = TOP_DISTINCT_CHANNEL_USERS_LIMIT
    filter_admins = True

    user_distinct_counts: Counter = scan_data.get("user_distinct_channel_counts", Counter())
    user_channel_msg_counts: Dict[int, Dict[int, int]] = scan_data.get('user_channel_message_counts', {})

    if not user_distinct_counts:
        log.debug("Bỏ qua tạo Top Distinct Channel Users embed: Không có dữ liệu.")
        return None

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, distinct_count) for uid, distinct_count in user_distinct_counts.most_common()
        if distinct_count > 0
           and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]

    if not filtered_sorted_users:
         log.debug("Bỏ qua tạo Top Distinct Channel Users embed: Không còn user sau khi lọc.")
         return None

    total_users_in_lb = len(filtered_sorted_users)

    embed = discord.Embed(
        title=f"{e('award')} 🗺️ Top {limit} \"Người Đa Năng\"",
        color=discord.Color.dark_teal()
    )
    desc_prefix = "*Hoạt động trong nhiều kênh/luồng khác nhau nhất. Đã lọc bot."
    if filter_admins: desc_prefix += " Đã lọc admin."
    desc_lines = [desc_prefix]

    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

    leaderboard_lines = []
    for rank, (user_id, distinct_count) in enumerate(users_to_display, 1):
        user_obj = user_cache.get(user_id)
        user_display_name = utils.escape_markdown(user_obj.display_name) if user_obj else f"User ID: {user_id}"

        leaderboard_lines.append(f"**`#{rank:02d}`**. {user_display_name} / — **{distinct_count}** kênh/luồng")

        user_specific_channel_counts = user_channel_msg_counts.get(user_id, {})
        if user_specific_channel_counts:
            sorted_channels = sorted(user_specific_channel_counts.items(), key=lambda item: item[1], reverse=True)[:3]
            if sorted_channels:
                leaderboard_lines.append("`Top 3 hoạt động sôi nổi nhất.`")
                num_top_channels = len(sorted_channels)
                for i, (loc_id, msg_count) in enumerate(sorted_channels):
                    channel_obj = guild.get_channel_or_thread(loc_id)
                    channel_name_str = utils.escape_markdown(channel_obj.name) if channel_obj else "?"
                    channel_type_emoji = utils.get_channel_type_emoji(channel_obj, bot) if channel_obj else "❓"
                    is_last = (i == num_top_channels - 1)
                    branch = "└──" if is_last else "├──"
                    channel_line = f"│   {branch} {channel_type_emoji} {channel_name_str} - **{msg_count:,}** tin"
                    leaderboard_lines.append(channel_line)
        else:
             leaderboard_lines.append("│   *(Không có dữ liệu chi tiết kênh)*")
        if rank < len(users_to_display):
             leaderboard_lines.append("")

    desc_lines.append("\n" + "\n".join(leaderboard_lines))

    if total_users_in_lb > limit:
        desc_lines.append(f"\n... và {total_users_in_lb - limit} người dùng khác.")

    embed.description = "\n".join(desc_lines)
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (Nội dung quá dài)"
    embed.set_footer(text="Top 3 kênh/luồng hiển thị dựa trên số tin nhắn.")
    return embed
# --- KẾT THÚC THAY THẾ ---


async def create_top_activity_span_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: Union[discord.Client, commands.Bot]
) -> Optional[discord.Embed]:
    """Embed top user có khoảng thời gian hoạt động dài nhất."""
    e = lambda name: utils.get_emoji(name, bot)
    user_spans: List[Tuple[int, datetime.timedelta]] = []
    for user_id, data in user_activity.items():
        if data.get('is_bot', False): continue
        span_seconds = data.get('activity_span_seconds', 0.0)
        if span_seconds > 0:
             user_spans.append((user_id, datetime.timedelta(seconds=span_seconds)))
    if not user_spans: return None
    user_spans.sort(key=lambda item: item[1], reverse=True)
    embed = discord.Embed(
        title=f"{e('award')}{e('clock')} Top User Hoạt Động Lâu Nhất (Span)",
        description=f"*Dựa trên khoảng TG giữa tin nhắn đầu và cuối trong lần quét. Đã lọc bot.*",
        color=discord.Color.dark_grey()
    )
    limit = TOP_ACTIVITY_SPAN_USERS_LIMIT
    user_ids_to_fetch = [uid for uid, span in user_spans[:limit]]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)
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
    return await create_generic_leaderboard_embed(
        counts, guild, bot, f"{utils.get_emoji('thread', bot)} Tạo Thread", "thread", "threads",
        TOP_THREAD_CREATORS_LIMIT, discord.Color.dark_magenta(),
        footer_note="Yêu cầu quyền View Audit Log và theo dõi thread_create.", filter_admins=True
    )


async def create_top_booster_embed( # Thêm mới
    boosters: List[discord.Member], # Danh sách booster đã sắp xếp theo tgian boost
    bot: discord.Client,
    scan_end_time: datetime.datetime
) -> Optional[discord.Embed]:
    """Tạo embed top booster bền bỉ."""
    e = lambda name: utils.get_emoji(name, bot)
    if not boosters: return None
    limit = 15

    embed = discord.Embed(
        title=f"{e('award')} {e('boost')} Top Booster Bền Bỉ",
        description="*Sắp xếp theo thời gian boost server lâu nhất.*",
        color=discord.Color(0xf47fff)
    )
    desc_lines = []
    for rank, member in enumerate(boosters[:limit], 1):
        boost_duration_str = "N/A"
        if member.premium_since:
            try:
                scan_end_time_aware = scan_end_time if scan_end_time.tzinfo else scan_end_time.replace(tzinfo=datetime.timezone.utc)
                premium_since_aware = member.premium_since if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc)
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