# --- START OF FILE reporting/embeds_analysis.py ---
import discord
import datetime
import math
import logging
import collections
import asyncio
from typing import List, Dict, Any, Optional, Union, Set, Tuple
import unicodedata
from collections import Counter, defaultdict

log = logging.getLogger(__name__)

# Relative import
# Sử dụng import tuyệt đối cho utils và config
import utils
import config
# Import helper từ embeds_user để định dạng cây
from .embeds_user import _format_user_tree_line

# --- Constants ---
KEYWORD_RANKING_LIMIT = 10
TRACKED_ROLE_GRANTS_PER_EMBED = 10 # Giới hạn hiển thị cho mỗi role
TOP_EMOJI_REACTION_USAGE_LIMIT = 20
TOP_CONTENT_EMOJI_LIMIT = 20 # Limit cho emoji content
TOP_REACTION_GIVERS_LIMIT = 15 # Giảm nhẹ
MAX_ROLES_IN_SINGLE_TRACKED_EMBED = 5 # Không còn dùng do tách embed
LEAST_EMOJI_REACTION_USAGE_LIMIT = 15 # Limit cho ít reaction

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
    """Tạo embeds cho kết quả phân tích từ khóa (BXH user dùng kiểu cây)."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    limit = 15 # Giới hạn cho BXH user keyword
    filter_admins = True
    item_name_singular="lần dùng"
    item_name_plural="lần dùng"
    color = discord.Color.orange()

    if not target_keywords:
        no_kw_embed = discord.Embed(
            title=f"{e('hashtag')} Phân tích Từ khóa",
            description=f"{e('info')} Không có từ khóa nào được chỉ định để tìm kiếm.",
            color=discord.Color.light_grey()
        )
        return [no_kw_embed]

    # Embed tổng quan (Giữ nguyên)
    kw_overall_embed = discord.Embed(
        title=f"{e('hashtag')} Phân tích Từ khóa",
        description=f"Đếm **{len(target_keywords)}** từ khóa: `{'`, `'.join(utils.escape_markdown(kw) for kw in target_keywords)}` (không phân biệt hoa thường).",
        color=discord.Color.blue()
    )
    if not keyword_counts:
        kw_overall_embed.add_field(name="Kết quả", value="Không tìm thấy lần xuất hiện nào của các từ khóa trên.", inline=False)
        embeds.append(kw_overall_embed)
        return embeds
    kw_summary_lines = []
    sorted_keywords = sorted(keyword_counts.items(), key=lambda item: item[1], reverse=True)
    for keyword, count in sorted_keywords[:15]: kw_summary_lines.append(f"- `{utils.escape_markdown(keyword)}`: **{count:,}** lần")
    if len(sorted_keywords) > 15: kw_summary_lines.append(f"- ... và {len(sorted_keywords)-15} từ khóa khác.")
    kw_overall_embed.add_field(name="Tổng số lần xuất hiện", value="\n".join(kw_summary_lines), inline=False)
    embeds.append(kw_overall_embed)

    # Embed kênh/luồng (Giữ nguyên)
    kw_channel_embed = discord.Embed(title=f"{e('text_channel')}{e('thread')} Top Kênh/Luồng theo Từ khóa", color=discord.Color.green())
    channel_kw_ranking = []
    all_location_counts = {**channel_keyword_counts, **thread_keyword_counts}
    for loc_id, counts in all_location_counts.items():
        total_count = sum(counts.values())
        if total_count > 0:
            location_obj = guild.get_channel_or_thread(loc_id); loc_mention = location_obj.mention if location_obj else f"`ID:{loc_id}`"
            loc_name = f" ({utils.escape_markdown(location_obj.name)})" if location_obj else ""; loc_type_emoji = utils.get_channel_type_emoji(location_obj, bot) if location_obj else "❓"
            channel_kw_ranking.append({"mention": loc_mention, "name": loc_name, "total": total_count, "details": dict(counts), "emoji": loc_type_emoji})
    channel_kw_ranking.sort(key=lambda x: x['total'], reverse=True)
    channel_rank_lines = []
    for i, item in enumerate(channel_kw_ranking[:KEYWORD_RANKING_LIMIT]):
        details = ", ".join(f"`{kw}`:{c:,}" for kw, c in item['details'].items());
        if len(details) > 150: details = details[:150] + "..."
        channel_rank_lines.append(f"**`#{i+1:02d}`**. {item['emoji']} {item['mention']}{item['name']} ({item['total']:,} tổng)")
        if details: channel_rank_lines.append(f"   `└` `{details}`")
    if not channel_rank_lines: channel_rank_lines.append("Không có kênh/luồng nào chứa từ khóa.")
    if len(channel_kw_ranking) > KEYWORD_RANKING_LIMIT: channel_rank_lines.append(f"\n... và {len(channel_kw_ranking) - KEYWORD_RANKING_LIMIT} kênh/luồng khác.")
    kw_channel_embed.description = "\n".join(channel_rank_lines)
    if len(kw_channel_embed.description) > 4096: kw_channel_embed.description = kw_channel_embed.description[:4090] + "\n[...]"
    embeds.append(kw_channel_embed)

    # --- Embed User theo Keyword (DẠNG CÂY) ---
    user_total_keyword_counts = collections.Counter({
         uid: sum(counts.values()) for uid, counts in user_keyword_counts.items() if sum(counts.values()) > 0
    })
    if user_total_keyword_counts:
        admin_ids_to_filter: Optional[Set[int]] = None
        if filter_admins:
            admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
            admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
            if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

        filtered_sorted_users = [
            (uid, count) for uid, count in user_total_keyword_counts.most_common()
            if (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
               and not getattr(guild.get_member(uid), 'bot', True)
        ]
        if filtered_sorted_users:
            total_users_in_lb = len(filtered_sorted_users)
            users_to_display = filtered_sorted_users[:limit]
            user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
            user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)

            title_emoji = e('award') if e('award') != '❓' else '🏆'
            title_item_emoji = e('members') if e('members') != '❓' else '👥'
            kw_user_embed = discord.Embed(title=f"{title_emoji} {title_item_emoji} Top User theo Từ khóa (Tổng)", color=color)
            desc_prefix = "*Đã lọc bot."
            if filter_admins: desc_prefix += " Đã lọc admin*"
            description_lines = [desc_prefix, ""]

            for rank, (user_id, total_count) in enumerate(users_to_display, 1):
                secondary_info = None
                user_specific_counts = user_keyword_counts.get(user_id, Counter())
                if user_specific_counts:
                    try:
                        top_kw, top_kw_count = user_specific_counts.most_common(1)[0]
                        secondary_info = f"• Top Keyword: `{utils.escape_markdown(top_kw)}` ({top_kw_count:,})"
                    except (ValueError, IndexError): pass

                lines = await _format_user_tree_line(
                    rank, user_id, total_count, item_name_singular, item_name_plural,
                    guild, user_cache, secondary_info=secondary_info
                )
                description_lines.extend(lines)

            if description_lines and description_lines[-1] == "": description_lines.pop()
            kw_user_embed.description = "\n".join(description_lines)
            if len(kw_user_embed.description) > 4096: kw_user_embed.description = kw_user_embed.description[:4090] + "\n[...]"
            if total_users_in_lb > limit: kw_user_embed.set_footer(text=f"... và {total_users_in_lb - limit} người dùng khác.")
            embeds.append(kw_user_embed)

    return embeds


async def create_filtered_reaction_embed(
    filtered_reaction_counts: collections.Counter,
    bot: discord.Client,
    limit: int = TOP_EMOJI_REACTION_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top emoji reactions (custom server + exceptions)."""
    if not filtered_reaction_counts: return None
    e = lambda name: utils.get_emoji(name, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    title_item_emoji = e('reaction') if e('reaction') != '❓' else '👍'
    embed = discord.Embed(
        title=f"{title_emoji} {title_item_emoji} Top {limit} Emoji Reactions Phổ Biến Nhất",
        color=discord.Color.gold()
    )
    desc_parts = ["*Dựa trên số lượt thả reaction tin nhắn.*"]
    if config.REACTION_UNICODE_EXCEPTIONS:
        desc_parts.append(f"*Chỉ bao gồm emoji của server và: {' '.join(config.REACTION_UNICODE_EXCEPTIONS)}*")
    else:
        desc_parts.append("*Chỉ bao gồm emoji của server.*")
    desc = "\n".join(desc_parts)

    sorted_emojis = filtered_reaction_counts.most_common(limit)
    emoji_lines = []
    podium_emojis = ["🥇", "🥈", "🥉"]

    for rank, (emoji_key, count) in enumerate(sorted_emojis, 1):
        display_emoji = utils.escape_markdown(str(emoji_key)) # Fallback

        if isinstance(emoji_key, int): # Custom emoji ID
            found_emoji = bot.get_emoji(emoji_key)
            if found_emoji: display_emoji = str(found_emoji)
            else: display_emoji = f"`ID:{emoji_key}`"
        elif isinstance(emoji_key, str): # Unicode emoji
            try: unicodedata.name(emoji_key); display_emoji = emoji_key
            except (TypeError, ValueError): pass # Giữ fallback nếu không phải emoji

        rank_prefix = podium_emojis[rank-1] if rank <= 3 else f"`#{rank:02d}`"
        emoji_lines.append(f"{rank_prefix} {display_emoji} — **{count:,}** lần")

    if len(filtered_reaction_counts) > limit:
        emoji_lines.append(f"\n... và {len(filtered_reaction_counts) - limit} emoji khác.")

    embed.description = desc + "\n\n" + "\n".join(emoji_lines)
    if len(embed.description) > 4096:
        embed.description = embed.description[:4090] + "\n[...]"

    footer_text = "Đã bật Reaction Scan và có quyền đọc lịch sử."
    if config.REACTION_UNICODE_EXCEPTIONS: footer_text += " Đã lọc emoji Unicode."
    embed.set_footer(text=footer_text)
    return embed

# --- HÀM MỚI ---
async def create_least_filtered_reaction_embed(
    filtered_reaction_counts: collections.Counter,
    bot: discord.Client,
    limit: int = LEAST_EMOJI_REACTION_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị các emoji reactions ÍT phổ biến nhất (custom server + exceptions)."""
    if not filtered_reaction_counts: return None
    e = lambda name: utils.get_emoji(name, bot)

    title_emoji = '📉' # Emoji khác cho "ít nhất"
    title_item_emoji = e('reaction') if e('reaction') != '❓' else '👍'
    embed = discord.Embed(
        title=f"{title_emoji} {title_item_emoji} Top {limit} Emoji Reactions Ít Phổ Biến Nhất",
        color=discord.Color.light_grey() # Màu khác
    )
    desc_parts = ["*Dựa trên số lượt thả reaction tin nhắn.*"]
    if config.REACTION_UNICODE_EXCEPTIONS:
        desc_parts.append(f"*Chỉ bao gồm emoji của server và: {' '.join(config.REACTION_UNICODE_EXCEPTIONS)}*")
    else:
        desc_parts.append("*Chỉ bao gồm emoji của server.*")
    desc_parts.append("*Chỉ hiển thị emoji có > 0 lượt thả.*") # Thêm ghi chú
    desc = "\n".join(desc_parts)

    # Sắp xếp tăng dần và lấy top `limit`
    sorted_emojis = sorted(
        [item for item in filtered_reaction_counts.items() if item[1] > 0], # Lọc bỏ emoji 0 lượt
        key=lambda item: item[1]
    )[:limit]

    if not sorted_emojis: # Nếu không có emoji nào > 0 lượt
        embed.description = desc + "\n\n*Không có emoji reaction nào (đã lọc) được sử dụng ít nhất 1 lần.*"
        return embed

    emoji_lines = []
    for rank, (emoji_key, count) in enumerate(sorted_emojis, 1):
        display_emoji = utils.escape_markdown(str(emoji_key)) # Fallback

        if isinstance(emoji_key, int): # Custom emoji ID
            found_emoji = bot.get_emoji(emoji_key)
            if found_emoji: display_emoji = str(found_emoji)
            else: display_emoji = f"`ID:{emoji_key}`"
        elif isinstance(emoji_key, str): # Unicode emoji
            try: unicodedata.name(emoji_key); display_emoji = emoji_key
            except (TypeError, ValueError): pass

        rank_prefix = f"`#{rank:02d}`" # Dùng rank số
        emoji_lines.append(f"{rank_prefix} {display_emoji} — **{count:,}** lần")

    if len([item for item in filtered_reaction_counts.items() if item[1] > 0]) > limit:
        emoji_lines.append(f"\n... và {len(filtered_reaction_counts) - limit} emoji khác (có > 0 lượt).")

    embed.description = desc + "\n\n" + "\n".join(emoji_lines)
    if len(embed.description) > 4096:
        embed.description = embed.description[:4090] + "\n[...]"

    footer_text = "Đã bật Reaction Scan và có quyền đọc lịch sử."
    if config.REACTION_UNICODE_EXCEPTIONS: footer_text += " Đã lọc emoji Unicode."
    embed.set_footer(text=footer_text)
    return embed

# --- HÀM MỚI ---
async def create_top_content_emoji_embed(
    content_emoji_counts: collections.Counter, # Counter{emoji_id: count}
    bot: discord.Client,
    guild: discord.Guild, # Cần guild để kiểm tra emoji thuộc server
    limit: int = TOP_CONTENT_EMOJI_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top emoji CỦA SERVER được dùng trong nội dung tin nhắn."""
    if not content_emoji_counts: return None
    e = lambda name: utils.get_emoji(name, bot)

    # Lọc chỉ lấy emoji của server này
    server_emoji_ids = {emoji.id for emoji in guild.emojis}
    server_content_counts = collections.Counter({
        emoji_id: count
        for emoji_id, count in content_emoji_counts.items()
        if emoji_id in server_emoji_ids and count > 0
    })

    if not server_content_counts: return None

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    title_item_emoji = e('mention') if e('mention') != '❓' else '😀' # Emoji chung
    embed = discord.Embed(
        title=f"{title_emoji} {title_item_emoji} Top {limit} Emoji Server Dùng Trong Tin Nhắn",
        color=discord.Color.yellow() # Màu khác
    )
    desc = "*Dựa trên số lần emoji CỦA SERVER NÀY xuất hiện trong nội dung tin nhắn.*"

    sorted_emojis = server_content_counts.most_common(limit)
    emoji_lines = []
    podium_emojis = ["🥇", "🥈", "🥉"]

    for rank, (emoji_id, count) in enumerate(sorted_emojis, 1):
        display_emoji = f"`ID:{emoji_id}`" # Fallback
        found_emoji = bot.get_emoji(emoji_id) # Thử lấy từ cache bot
        if found_emoji: display_emoji = str(found_emoji)

        rank_prefix = podium_emojis[rank-1] if rank <= 3 else f"`#{rank:02d}`"
        emoji_lines.append(f"{rank_prefix} {display_emoji} — **{count:,}** lần")

    if len(server_content_counts) > limit:
        emoji_lines.append(f"\n... và {len(server_content_counts) - limit} emoji server khác.")

    embed.description = desc + "\n\n" + "\n".join(emoji_lines)
    if len(embed.description) > 4096:
        embed.description = embed.description[:4090] + "\n[...]"

    return embed


async def create_top_reaction_givers_embed(
    user_reaction_given_counts: Counter,
    user_reaction_emoji_given_counts: defaultdict,
    scan_data: Dict[str, Any],
    guild: discord.Guild,
    bot: discord.Client,
    limit: int = TOP_REACTION_GIVERS_LIMIT,
    filter_admins: bool = True
) -> Optional[discord.Embed]:
    """Tạo embed xếp hạng người dùng thả reaction nhiều nhất (đã lọc) - DẠNG CÂY."""
    e = lambda name: utils.get_emoji(name, bot)
    title = f"{e('reaction')} Top {limit} Người Thả Reaction Nhiều Nhất"
    color = discord.Color.teal()
    item_name_singular = "reaction"
    item_name_plural = "reactions"

    if not user_reaction_given_counts: return None

    admin_ids_to_filter: Optional[Set[int]] = None
    if filter_admins:
        admin_ids_to_filter = {m.id for m in guild.members if m.guild_permissions.administrator}
        admin_ids_to_filter.update(config.ADMIN_ROLE_IDS_FILTER)
        if config.ADMIN_USER_ID: admin_ids_to_filter.add(config.ADMIN_USER_ID)

    filtered_sorted_users = [
        (uid, total_count) for uid, total_count in user_reaction_given_counts.most_common()
        if total_count > 0 and (not filter_admins or not isinstance(uid, int) or uid not in admin_ids_to_filter)
           and not getattr(guild.get_member(uid), 'bot', True)
    ]
    if not filtered_sorted_users: return None

    total_users_in_lb = len(filtered_sorted_users)
    users_to_display = filtered_sorted_users[:limit]
    user_ids_to_fetch = [uid for uid, count in users_to_display if isinstance(uid, int)]
    user_cache = await utils._fetch_user_dict(guild, user_ids_to_fetch, bot)
    emoji_cache: Dict[int, discord.Emoji] = scan_data.get("server_emojis_cache", {})

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    embed = discord.Embed(title=f"{title_emoji} {title}", color=color)
    desc_prefix = "*Dựa trên số reaction đã thả. Không tính bot.*"
    if filter_admins: desc_prefix += " Không tính Admin."
    description_lines = [desc_prefix, ""]

    for rank, (user_id, total_count) in enumerate(users_to_display, 1):
        secondary_info = None
        user_specific_counts = user_reaction_emoji_given_counts.get(user_id, Counter())
        if user_specific_counts:
            try:
                most_used_key, top_count = max(user_specific_counts.items(), key=lambda item: item[1])
                if isinstance(most_used_key, int):
                    emoji_obj = emoji_cache.get(most_used_key) or bot.get_emoji(most_used_key)
                    if emoji_obj: secondary_info = f"• Top: {str(emoji_obj)} ({top_count:,})"
                    else: secondary_info = f"• Top ID: `{most_used_key}` ({top_count:,})"
                elif isinstance(most_used_key, str): # Unicode
                     try: unicodedata.name(most_used_key); secondary_info = f"• Top: {most_used_key} ({top_count:,})"
                     except (TypeError, ValueError): secondary_info = f"• Top: `{most_used_key}` ({top_count:,})"
            except ValueError: pass
            except Exception as e_find: log.warning(f"Lỗi tìm top reaction giver emoji cho user {user_id}: {e_find}")

        lines = await _format_user_tree_line(
            rank, user_id, total_count, item_name_singular, item_name_plural,
            guild, user_cache, secondary_info=secondary_info
        )
        description_lines.extend(lines)

    if description_lines and description_lines[-1] == "": description_lines.pop()
    embed.description = "\n".join(description_lines)
    if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"

    footer_text = "Đã bật Reaction Scan và có quyền đọc lịch sử."
    if config.REACTION_UNICODE_EXCEPTIONS: footer_text += " Đã lọc emoji Unicode."
    if total_users_in_lb > limit:
        footer_text = f"... và {total_users_in_lb - limit} người dùng khác. | {footer_text}"
    embed.set_footer(text=footer_text)
    return embed

async def create_tracked_role_grant_leaderboards(
    tracked_role_grants: Optional[collections.Counter],
    guild: discord.Guild,
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds xếp hạng cho các role được theo dõi lượt cấp - DẠNG CÂY."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    limit = TRACKED_ROLE_GRANTS_PER_EMBED
    item_name_singular="lần nhận"
    item_name_plural="lần nhận"

    if not isinstance(tracked_role_grants, collections.Counter) or not config.TRACKED_ROLE_GRANT_IDS:
        return embeds

    all_user_ids = {uid for uid, rid in tracked_role_grants.keys()}
    if not all_user_ids: return embeds

    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    if all_user_ids: # Fetch chỉ khi có user ID
        user_ids_list = list(all_user_ids)
        user_cache = await utils._fetch_user_dict(guild, user_ids_list, bot)

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    title_item_emoji = e('crown') if e('crown') != '❓' else '👑'

    for role_id in config.TRACKED_ROLE_GRANT_IDS:
        role = guild.get_role(role_id)
        if not role: log.warning(f"Không tìm thấy tracked role ID {role_id} trong server."); continue

        role_counter = collections.Counter({
            uid: count for (uid, rid), count in tracked_role_grants.items()
            if rid == role_id and count > 0 and not getattr(guild.get_member(uid), 'bot', True)
        })
        if not role_counter: continue

        filtered_sorted_users = role_counter.most_common() # Đã lọc bot
        if not filtered_sorted_users: continue

        total_users_in_lb = len(filtered_sorted_users)
        users_to_display = filtered_sorted_users[:limit]

        embed = discord.Embed(
            title=f"{title_emoji} {title_item_emoji} Top Nhận Role: {role.mention}",
            description=f"*Số lần nhận role '{utils.escape_markdown(role.name)}' từ Audit Log.*",
            color=role.color if role.color.value != 0 else discord.Color.purple()
        )
        description_lines = [""] # Bắt đầu với dòng trống

        for rank, (user_id, count) in enumerate(users_to_display, 1):
            # Không cần thông tin phụ cho BXH này
            lines = await _format_user_tree_line(
                rank, user_id, count, item_name_singular, item_name_plural,
                guild, user_cache, secondary_info=None
            )
            description_lines.extend(lines)

        if description_lines and description_lines[-1] == "": description_lines.pop()
        embed.description += "\n".join(description_lines) # Thêm vào description gốc
        if len(embed.description) > 4096: embed.description = embed.description[:4090] + "\n[...]"

        if total_users_in_lb > limit:
            embed.set_footer(text=f"... và {total_users_in_lb - limit} người khác.")
        embeds.append(embed)

    return embeds


async def create_error_embed(
    scan_errors: List[str],
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed tóm tắt các lỗi và cảnh báo xảy ra trong quá trình quét."""
    # (Giữ nguyên hàm này)
    if not scan_errors: return None
    e = lambda name: utils.get_emoji(name, bot)

    error_embed = discord.Embed(
        title=f"{e('error')} Tóm tắt Lỗi và Cảnh báo Khi Quét ({len(scan_errors)} mục)",
        color=discord.Color.dark_red(),
        timestamp=discord.utils.utcnow()
    )
    errors_per_page = 15 # Giảm số lỗi trên mỗi embed
    error_text_lines = []
    errors_shown = 0
    total_error_len = 0
    max_len = 4000 # Giới hạn description

    for i, err in enumerate(scan_errors):
        # Thêm emoji cảnh báo/lỗi vào đầu mỗi dòng nếu có thể phân biệt
        line_prefix = e('warning') if "warn" in str(err).lower() or "bỏ qua" in str(err).lower() else e('error')
        line = f"{line_prefix} {utils.escape_markdown(str(err)[:350])}" # Giới hạn độ dài dòng
        line += "..." if len(str(err)) > 350 else ""

        if total_error_len + len(line) + 1 > max_len:
            error_text_lines.append(f"\n{e('error')} ... (quá nhiều lỗi để hiển thị toàn bộ)")
            break

        error_text_lines.append(line)
        total_error_len += len(line) + 1
        errors_shown += 1

        if errors_shown >= errors_per_page:
            if len(scan_errors) > errors_per_page:
                remaining_errors = len(scan_errors) - errors_per_page
                more_line = f"\n{e('warning')} ... và {remaining_errors} lỗi/cảnh báo khác."
                if total_error_len + len(more_line) <= max_len:
                    error_text_lines.append(more_line)
                elif not error_text_lines[-1].startswith(f"\n{e('warning')} ..."): # Tránh lặp lại "..."
                    error_text_lines.append(f"\n{e('warning')} ... (và nhiều lỗi/cảnh báo khác)")
            break

    error_embed.description = "\n".join(error_text_lines) if error_text_lines else f"{e('success')} Không có lỗi hoặc cảnh báo nào được ghi nhận."
    if len(error_embed.description) > 4096: error_embed.description = error_embed.description[:4090] + "\n[...]"
    error_embed.set_footer(text="Kiểm tra log chi tiết trong thread (nếu có) hoặc console.")
    return error_embed

# --- END OF FILE reporting/embeds_analysis.py ---