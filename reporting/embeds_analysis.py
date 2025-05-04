# --- START OF FILE reporting/embeds_analysis.py ---
import discord
import datetime
import math
import logging # <<< Đảm bảo logging được import
import collections
import asyncio
from typing import List, Dict, Any, Optional, Union, Set, Tuple
import unicodedata

log = logging.getLogger(__name__)

# Relative import
try:
    from .. import utils
    from .. import config # Cần config để lấy emoji exceptions và tracked roles
    from .embeds_user import create_generic_leaderboard_embed # <<< Đảm bảo import này đúng
except ImportError:
    # Fallback cho trường hợp chạy độc lập (nếu có)
    log.warning("Running embeds_analysis.py with fallback imports.")
    import utils
    import config
    try:
        from embeds_user import create_generic_leaderboard_embed
    except ImportError:
        log.warning("Không thể import create_generic_leaderboard_embed từ embeds_user trong fallback.")
        create_generic_leaderboard_embed = None # Đặt là None để tránh lỗi sau này
    pass


# --- Constants ---
KEYWORD_RANKING_LIMIT = 10
TRACKED_ROLE_GRANTS_PER_EMBED = 15 # Giới hạn user mỗi embed danh hiệu
TOP_EMOJI_REACTION_USAGE_LIMIT = 15 # Tăng giới hạn hiển thị emoji reaction
MAX_ROLES_IN_SINGLE_TRACKED_EMBED = 5 # Ngưỡng để quyết định gộp hay tách embed BXH Role

# --- Embed Functions ---

async def create_keyword_analysis_embeds(
    keyword_counts: collections.Counter,
    channel_keyword_counts: Dict[int, collections.Counter],
    thread_keyword_counts: Dict[int, collections.Counter],
    user_keyword_counts: Dict[int, collections.Counter],
    guild: discord.Guild,
    bot: discord.Client,
    target_keywords: List[str]
) -> List[discord.Embed]:
    """Tạo embeds cho kết quả phân tích từ khóa."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    if not keyword_counts:
        no_kw_embed = discord.Embed(
            title=f"{e('hashtag')} Phân tích Từ khóa",
            description=f"{e('info')} Không tìm thấy từ khóa nào được chỉ định trong quá trình quét.",
            color=discord.Color.light_grey()
        )
        return [no_kw_embed]

    kw_overall_embed = discord.Embed(
        title=f"{e('hashtag')} Phân tích Từ khóa",
        description=f"Đếm **{len(target_keywords)}** từ khóa (không phân biệt hoa thường).",
        color=discord.Color.blue()
    )
    kw_summary_lines = []
    sorted_keywords = sorted(keyword_counts.items(), key=lambda item: item[1], reverse=True)
    for keyword, count in sorted_keywords[:15]:
        kw_summary_lines.append(f"- `{utils.escape_markdown(keyword)}`: **{count:,}** lần")
    if len(sorted_keywords) > 15: kw_summary_lines.append(f"- ... và {len(sorted_keywords)-15} từ khóa khác.")
    kw_overall_embed.add_field(name="Tổng số lần xuất hiện", value="\n".join(kw_summary_lines) if kw_summary_lines else "Không tìm thấy.", inline=False)
    embeds.append(kw_overall_embed)

    kw_channel_embed = discord.Embed(title=f"{e('text_channel')}/{e('thread')} Top Kênh/Luồng theo Từ khóa", color=discord.Color.green())
    channel_kw_ranking = []
    all_location_counts = {**channel_keyword_counts, **thread_keyword_counts}
    for loc_id, counts in all_location_counts.items():
        total_count = sum(counts.values())
        if total_count > 0:
            location_obj = guild.get_channel_or_thread(loc_id)
            loc_mention = location_obj.mention if location_obj else f"`ID:{loc_id}`"
            loc_name = f" (`{utils.escape_markdown(location_obj.name)}`)" if location_obj else ""
            loc_type_emoji = utils.get_channel_type_emoji(location_obj, bot) if location_obj else "❓"
            channel_kw_ranking.append({"mention": loc_mention, "name": loc_name, "total": total_count, "details": dict(counts), "emoji": loc_type_emoji})
    channel_kw_ranking.sort(key=lambda x: x['total'], reverse=True)
    channel_rank_lines = []
    for i, item in enumerate(channel_kw_ranking[:KEYWORD_RANKING_LIMIT]):
        details = ", ".join(f"`{kw}`:{c:,}" for kw, c in item['details'].items())
        if len(details) > 150: details = details[:150] + "..."
        channel_rank_lines.append(f"**`#{i+1:02d}`**. {item['emoji']} {item['mention']}{item['name']} ({item['total']:,} tổng)")
        # Hiển thị chi tiết từ khóa chỉ nếu nó không quá dài
        if details: channel_rank_lines.append(f"   └ {details}")

    if not channel_rank_lines: channel_rank_lines.append("Không có kênh/luồng nào chứa từ khóa.")
    if len(channel_kw_ranking) > KEYWORD_RANKING_LIMIT: channel_rank_lines.append(f"\n... và {len(channel_kw_ranking) - KEYWORD_RANKING_LIMIT} kênh/luồng khác.")
    kw_channel_embed.description = "\n".join(channel_rank_lines)
    if len(kw_channel_embed.description) > 4000: kw_channel_embed.description = kw_channel_embed.description[:4000] + "\n... (quá dài)"
    embeds.append(kw_channel_embed)

    # Tạo embed Top User dùng từ khóa
    user_total_keyword_counts = collections.Counter({
         uid: sum(counts.values())
         for uid, counts in user_keyword_counts.items()
         if sum(counts.values()) > 0
    })

    # Kiểm tra xem hàm create_generic_leaderboard_embed có tồn tại không TRƯỚC KHI gọi
    if user_total_keyword_counts and 'create_generic_leaderboard_embed' in globals() and callable(create_generic_leaderboard_embed):
        try:
            kw_user_embed = await create_generic_leaderboard_embed(
                 counter_data=user_total_keyword_counts, guild=guild, bot=bot,
                 title=f"{e('members')} Top User theo Từ khóa (Tổng)",
                 item_name_singular="lần dùng", item_name_plural="lần dùng",
                 limit=15, color=discord.Color.orange(), show_total=False,
                 filter_admins=True # Lọc admin khỏi BXH này
            )
            if kw_user_embed: embeds.append(kw_user_embed)
        except Exception as user_kw_err:
            log.error(f"Lỗi tạo embed Top User theo Keyword: {user_kw_err}", exc_info=True)
    elif not ('create_generic_leaderboard_embed' in globals() and callable(create_generic_leaderboard_embed)):
         log.warning("Bỏ qua tạo embed Top User theo Keyword do thiếu hàm helper.")


    return embeds


async def create_filtered_reaction_embed(
    filtered_reaction_counts: collections.Counter, # Counter đã lọc theo config
    bot: discord.Client,
    limit: int = TOP_EMOJI_REACTION_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top emoji reactions (custom server + exceptions)."""
    if not filtered_reaction_counts: return None
    e = lambda name: utils.get_emoji(name, bot)

    embed = discord.Embed(
        title=f"{e('award')} {e('reaction')} Top Emoji Reactions Phổ Biến",
        color=discord.Color.gold()
    )
    desc_parts = ["*Dựa trên số lượt thả reaction.*"]
    if config.REACTION_UNICODE_EXCEPTIONS:
        desc_parts.append(f"*Chỉ bao gồm emoji của server và: {' '.join(config.REACTION_UNICODE_EXCEPTIONS)}*")
    else:
        desc_parts.append("*Chỉ bao gồm emoji của server.*")
    desc = "\n".join(desc_parts)

    sorted_emojis = filtered_reaction_counts.most_common(limit)
    emoji_lines = []
    for rank, (emoji_key, count) in enumerate(sorted_emojis, 1):
        display_emoji = utils.escape_markdown(str(emoji_key)) # Mặc định là string key

        if isinstance(emoji_key, int): # Nếu key là ID (từ custom emoji server)
            found_emoji = bot.get_emoji(emoji_key)
            if found_emoji: display_emoji = str(found_emoji)
            else: display_emoji = f"`ID:{emoji_key}` (Unknown)" # Không tìm thấy trong cache?
        elif isinstance(emoji_key, str): # Nếu là string (unicode exception)
             # Kiểm tra xem có phải emoji unicode không để hiển thị trực tiếp
            try:
                 # Thử phân tích thành emoji unicode
                 unicodedata.name(emoji_key)
                 display_emoji = emoji_key # Hiển thị trực tiếp emoji unicode được phép
            except (TypeError, ValueError):
                 # Không phải unicode hợp lệ? Vẫn hiển thị dạng string
                 pass

        emoji_lines.append(f"**`#{rank:02d}`**. {display_emoji} — **{count:,}** lần")

    if len(filtered_reaction_counts) > limit:
        emoji_lines.append(f"\n... và {len(filtered_reaction_counts) - limit} emoji khác.")

    embed.description = desc + "\n\n" + "\n".join(emoji_lines)
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    footer_text = "Yêu cầu bật Reaction Scan và quyền đọc lịch sử."
    # Thêm ghi chú nếu có lọc unicode
    if config.REACTION_UNICODE_EXCEPTIONS: footer_text += " Đã lọc emoji Unicode."
    embed.set_footer(text=footer_text)
    return embed


async def create_tracked_role_grant_leaderboards(
    tracked_role_grants: Optional[collections.Counter], # Counter { (uid, rid): count }
    guild: discord.Guild,
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds xếp hạng cho các role được theo dõi lượt cấp."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    if not isinstance(tracked_role_grants, collections.Counter) or not config.TRACKED_ROLE_GRANT_IDS:
        return embeds # Trả về rỗng nếu dữ liệu không đúng hoặc không có role cần theo dõi

    # Lấy tất cả user_id *duy nhất* từ các key của counter
    all_user_ids = {uid for uid, rid in tracked_role_grants.keys()}
    if not all_user_ids: # Không có ai nhận role nào cả
        return embeds

    log.debug(f"Fetching {len(all_user_ids)} users for tracked role grant leaderboards...")
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    # Fetch user data MỘT LẦN
    fetch_tasks = [utils.fetch_user_data(guild, user_id, bot_ref=bot) for user_id in all_user_ids]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    # Xây dựng cache từ kết quả fetch
    user_id_list = list(all_user_ids) # Chuyển set thành list để lấy index
    for idx, result in enumerate(results):
        # Lấy user_id tương ứng với kết quả
        user_id = user_id_list[idx]
        if isinstance(result, (discord.User, discord.Member)):
            user_cache[user_id] = result
        else:
            user_cache[user_id] = None # Đánh dấu là không tìm thấy/lỗi
        if isinstance(result, Exception):
            log.warning(f"Lỗi fetch user {user_id} cho tracked role grants: {result}")
    log.debug("Fetch user data complete for tracked role grants.")

    # <<< FIX: Logic tạo embed gộp hoặc riêng biệt >>>
    if len(config.TRACKED_ROLE_GRANT_IDS) <= MAX_ROLES_IN_SINGLE_TRACKED_EMBED:
        # Gộp vào một embed
        embed = discord.Embed(
            title=f"{e('award')} BXH Danh Hiệu Đặc Biệt",
            description="*Số lần nhận role được theo dõi từ Audit Log.*",
            color=discord.Color.purple()
        )
        field_count = 0
        has_data_in_embed = False # Cờ kiểm tra có dữ liệu không

        for role_id in config.TRACKED_ROLE_GRANT_IDS:
            if field_count >= 25: break # Giới hạn field Discord

            role = guild.get_role(role_id)
            if not role:
                log.warning(f"Không tìm thấy tracked role ID {role_id} trong server.")
                continue

            # Tạo Counter riêng cho role này
            role_counter = collections.Counter({
                uid: count
                for (uid, rid), count in tracked_role_grants.items()
                if rid == role_id and count > 0
            })
            if not role_counter: continue # Bỏ qua nếu không ai nhận role này

            has_data_in_embed = True # Có dữ liệu để hiển thị
            field_name = f"{e('crown')} Top Nhận Role: {role.mention}" # <<< Hiển thị mention >>>
            field_lines = []
            sorted_users = role_counter.most_common(TRACKED_ROLE_GRANTS_PER_EMBED)
            for rank, (user_id, count) in enumerate(sorted_users, 1):
                 user_obj = user_cache.get(user_id) # <<< Lấy user từ cache đã fetch
                 user_mention = user_obj.mention if user_obj else f"`{user_id}`"
                 user_display = f" (`{utils.escape_markdown(user_obj.display_name)}`)" if user_obj else " (Unknown/Left)"
                 field_lines.append(f"`#{rank:02d}`. {user_mention}{user_display} ({count} lần)")

            if len(role_counter) > TRACKED_ROLE_GRANTS_PER_EMBED:
                field_lines.append(f"... và {len(role_counter) - TRACKED_ROLE_GRANTS_PER_EMBED} người khác.")

            field_value = "\n".join(field_lines)
            if len(field_value) > 1024: field_value = field_value[:1020] + "..."
            embed.add_field(name=field_name, value=field_value, inline=False)
            field_count += 1

        if has_data_in_embed: # Chỉ thêm embed nếu có ít nhất 1 field có dữ liệu
             embeds.append(embed)

    else:
        # Tạo embed riêng cho mỗi role
        for role_id in config.TRACKED_ROLE_GRANT_IDS:
            role = guild.get_role(role_id)
            if not role:
                log.warning(f"Không tìm thấy tracked role ID {role_id} trong server.")
                continue

            # Tạo Counter riêng cho role này
            role_counter = collections.Counter({
                uid: count
                for (uid, rid), count in tracked_role_grants.items()
                if rid == role_id and count > 0
            })
            if not role_counter: continue

            embed = discord.Embed(
                title=f"{e('award')} Top Nhận Role: {role.mention}", # <<< Hiển thị mention >>>
                description=f"*Số lần nhận role '{utils.escape_markdown(role.name)}' từ Audit Log.*",
                color=role.color if role.color.value != 0 else discord.Color.purple()
            )

            desc_lines = []
            sorted_users = role_counter.most_common(TRACKED_ROLE_GRANTS_PER_EMBED)
            for rank, (user_id, count) in enumerate(sorted_users, 1):
                 user_obj = user_cache.get(user_id) # <<< Lấy user từ cache đã fetch
                 user_mention = user_obj.mention if user_obj else f"`{user_id}`"
                 user_display = f" (`{utils.escape_markdown(user_obj.display_name)}`)" if user_obj else " (Unknown/Left)"
                 desc_lines.append(f"**`#{rank:02d}`**. {user_mention}{user_display} — **{count}** lần")

            if len(role_counter) > TRACKED_ROLE_GRANTS_PER_EMBED:
                 desc_lines.append(f"\n... và {len(role_counter) - TRACKED_ROLE_GRANTS_PER_EMBED} người khác.")

            # Gắn vào description thay vì field
            embed.description += "\n\n" + "\n".join(desc_lines)
            if len(embed.description) > 4000:
                 embed.description = embed.description[:4000] + "\n... (quá dài)"
            embeds.append(embed)
    # <<< END FIX >>>

    return embeds


async def create_error_embed(
    scan_errors: List[str],
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed tóm tắt các lỗi và cảnh báo xảy ra trong quá trình quét."""
    # Giữ nguyên logic này
    if not scan_errors: return None
    e = lambda name: utils.get_emoji(name, bot)

    error_embed = discord.Embed(
        title=f"{e('error')} Tóm tắt Lỗi và Cảnh báo Khi Quét ({len(scan_errors)} mục)",
        color=discord.Color.dark_red(),
        timestamp=discord.utils.utcnow()
    )

    errors_per_page = 20
    error_text_lines = []
    errors_shown = 0
    total_error_len = 0
    max_len = 4000

    for i, err in enumerate(scan_errors):
        line = f"- {utils.escape_markdown(str(err)[:300])}" # Giới hạn độ dài mỗi lỗi
        if len(str(err)) > 300: line += "..."
        # Kiểm tra độ dài trước khi thêm
        if total_error_len + len(line) + 1 > max_len: # +1 cho dấu xuống dòng
            error_text_lines.append("\n... (quá nhiều lỗi để hiển thị)")
            break
        error_text_lines.append(line)
        total_error_len += len(line) + 1
        errors_shown += 1
        # Kiểm tra số lượng lỗi đã hiển thị
        if errors_shown >= errors_per_page:
            if len(scan_errors) > errors_per_page:
                remaining_errors = len(scan_errors) - errors_per_page
                more_line = f"\n... và {remaining_errors} lỗi/cảnh báo khác."
                # Kiểm tra lại độ dài nếu thêm dòng "và..."
                if total_error_len + len(more_line) <= max_len:
                    error_text_lines.append(more_line)
                else:
                    # Nếu thêm dòng "và..." cũng quá dài, chỉ báo là còn nhiều lỗi khác
                    if not error_text_lines[-1].startswith("\n..."): # Tránh lặp lại
                       error_text_lines.append("\n... (và nhiều lỗi/cảnh báo khác)")
            break # Dừng lại sau khi đạt giới hạn page

    error_embed.description = "\n".join(error_text_lines) if error_text_lines else "Không có lỗi nào được ghi nhận."
    if len(error_embed.description) > 4096: # Kiểm tra lại giới hạn cuối cùng
        error_embed.description = error_embed.description[:4090] + "\n[...]"

    return error_embed

# --- END OF FILE reporting/embeds_analysis.py ---