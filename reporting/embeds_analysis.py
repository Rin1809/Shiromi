# --- START OF FILE reporting/embeds_analysis.py ---
import discord
import datetime
import math
import logging
import collections
import asyncio
from typing import List, Dict, Any, Optional, Union, Set, Tuple
import unicodedata
from collections import Counter, defaultdict, OrderedDict

log = logging.getLogger(__name__)

import utils
import config

# --- Constants ---
KEYWORD_RANKING_LIMIT = 10
TRACKED_ROLE_GRANTS_PER_EMBED = 10
TOP_EMOJI_REACTION_USAGE_LIMIT = 20
TOP_CONTENT_EMOJI_LIMIT = 20
TOP_REACTION_GIVERS_LIMIT = 15
LEAST_EMOJI_REACTION_USAGE_LIMIT = 15

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
    limit = 15
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

    kw_overall_embed = discord.Embed(
        title=f"{e('hashtag')} Phân tích Từ khóa",
        description=f"Đếm **{len(target_keywords)}** từ khóa: `{'`, `'.join(utils.escape_markdown(kw) for kw in target_keywords)}` (không phân biệt hoa thường).",
        color=discord.Color.blue()
    )
    if not keyword_counts:
        kw_overall_embed.add_field(name="Kết quả", value="Không tìm thấy lần xuất hiện nào của các từ khóa trên.", inline=False)
        embeds.append(kw_overall_embed)
        return embeds

    data_for_chart_kw = sorted(keyword_counts.items(), key=lambda item: item[1], reverse=True)[:5]
    bar_chart_kw_str = ""
    if data_for_chart_kw:
         bar_chart_kw_str = await utils.create_vertical_text_bar_chart(
             sorted_data=data_for_chart_kw,
             key_formatter=lambda k: utils.escape_markdown(k),
             top_n=5, max_chart_height=5, bar_width=1, bar_spacing=1,
             chart_title="Top 5 Keywords", show_legend=True
         )
         kw_overall_embed.description += "\n\n" + bar_chart_kw_str

    kw_summary_lines = []
    sorted_keywords = sorted(keyword_counts.items(), key=lambda item: item[1], reverse=True)
    for keyword, count in sorted_keywords[:15]: kw_summary_lines.append(f"- `{utils.escape_markdown(keyword)}`: **{count:,}** lần")
    if len(sorted_keywords) > 15: kw_summary_lines.append(f"- ... và {len(sorted_keywords)-15} từ khóa khác.")
    kw_overall_embed.add_field(name="Tổng số lần xuất hiện", value="\n".join(kw_summary_lines), inline=False)

    if len(kw_overall_embed.description) > 4096: kw_overall_embed.description = kw_overall_embed.description[:4090] + "\n[...]"
    embeds.append(kw_overall_embed)

    kw_channel_embed = discord.Embed(title=f"{e('text_channel')}{e('thread')} Top Kênh/Luồng theo Từ khóa", color=discord.Color.green())
    channel_kw_ranking = []
    all_location_counts = {**channel_keyword_counts, **thread_keyword_counts}
    for loc_id, counts in all_location_counts.items():
        total_count = sum(counts.values())
        if total_count > 0:
            location_obj = guild.get_channel_or_thread(loc_id); loc_mention = location_obj.mention if location_obj else f"`ID:{loc_id}`"
            loc_name = f" ({utils.escape_markdown(location_obj.name)})" if location_obj else ""; loc_type_emoji = utils.get_channel_type_emoji(location_obj, bot) if location_obj else "❓"
            channel_kw_ranking.append({"id": loc_id, "mention": loc_mention, "name": loc_name, "total": total_count, "details": dict(counts), "emoji": loc_type_emoji})
    channel_kw_ranking.sort(key=lambda x: x['total'], reverse=True)

    data_for_chart_loc = [(item['id'], item['total']) for item in channel_kw_ranking[:5]]
    bar_chart_loc_str = ""
    if data_for_chart_loc:
         async def format_loc_key(loc_id):
             loc_obj = guild.get_channel_or_thread(loc_id)
             type_emoji = utils.get_channel_type_emoji(loc_obj, bot) if loc_obj else '❓'
             return f"{type_emoji} {utils.escape_markdown(loc_obj.name)}" if loc_obj else f"ID:{loc_id}"

         bar_chart_loc_str = await utils.create_vertical_text_bar_chart(
             sorted_data=data_for_chart_loc,
             key_formatter=format_loc_key,
             top_n=5, max_chart_height=8, bar_width=1, bar_spacing=2,
             chart_title="Top 5 Locations", show_legend=True
         )
         kw_channel_embed.description = bar_chart_loc_str + "\n\n"

    channel_rank_lines = []
    for i, item in enumerate(channel_kw_ranking[:KEYWORD_RANKING_LIMIT]):
        details = ", ".join(f"`{kw}`:{c:,}" for kw, c in item['details'].items());
        if len(details) > 150: details = details[:150] + "..."
        channel_rank_lines.append(f"**`#{i+1:02d}`**. {item['emoji']} {item['mention']}{item['name']} ({item['total']:,} tổng)")
        if details: channel_rank_lines.append(f"   `└` `{details}`")
    if not channel_rank_lines: channel_rank_lines.append("Không có kênh/luồng nào chứa từ khóa.")
    if len(channel_kw_ranking) > KEYWORD_RANKING_LIMIT: channel_rank_lines.append(f"\n... và {len(channel_kw_ranking) - KEYWORD_RANKING_LIMIT} kênh/luồng khác.")

    current_desc = kw_channel_embed.description or ""
    kw_channel_embed.description = current_desc + "\n".join(channel_rank_lines)

    if len(kw_channel_embed.description) > 4096: kw_channel_embed.description = kw_channel_embed.description[:4090] + "\n[...]"
    embeds.append(kw_channel_embed)

    user_total_keyword_counts = collections.Counter({
         uid: sum(counts.values()) for uid, counts in user_keyword_counts.items() if sum(counts.values()) > 0
    })
    if user_total_keyword_counts:
        async def get_top_kw_user(user_id, data_source):
            user_specific_counts = user_keyword_counts.get(user_id, Counter())
            if user_specific_counts:
                try:
                    top_kw, top_kw_count = user_specific_counts.most_common(1)[0]
                    return f"• Top: `{utils.escape_markdown(top_kw)}` ({top_kw_count:,})"
                except (ValueError, IndexError): pass
            return None
        # Gọi helper từ utils
        user_kw_embed = await utils.create_user_leaderboard_embed(
             title=f"{e('members')} Top User theo Từ khóa (Tổng)",
             counts=user_total_keyword_counts,
             value_key=None,
             guild=guild,
             bot=bot,
             limit=limit,
             item_name_singular=item_name_singular,
             item_name_plural=item_name_plural,
             e=e,
             color=color,
             filter_admins=filter_admins,
             secondary_info_getter=get_top_kw_user,
             show_bar_chart=True
         )
        if user_kw_embed: embeds.append(user_kw_embed)

    return embeds


async def create_filtered_reaction_embed(
    filtered_reaction_counts: collections.Counter,
    bot: discord.Client,
    limit: int = TOP_EMOJI_REACTION_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top emoji reactions."""
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
    desc_base = "\n".join(desc_parts)

    sorted_emojis = filtered_reaction_counts.most_common()
    if not sorted_emojis: return None

    bar_chart_str = ""
    data_for_chart = sorted_emojis[:5]
    if data_for_chart:
        async def format_emoji_key(emoji_key):
            display_emoji = utils.escape_markdown(str(emoji_key))
            if isinstance(emoji_key, int):
                found_emoji = bot.get_emoji(emoji_key)
                if found_emoji: display_emoji = str(found_emoji)
                else: display_emoji = f"ID:{emoji_key}"
            elif isinstance(emoji_key, str):
                try: unicodedata.name(emoji_key); display_emoji = emoji_key
                except (TypeError, ValueError): pass
            return display_emoji

        bar_chart_str = await utils.create_vertical_text_bar_chart(
            sorted_data=data_for_chart,
            key_formatter=format_emoji_key,
            top_n=5, max_chart_height=8, bar_width=1, bar_spacing=1,
            chart_title="Top 5 Reactions", show_legend=True
        )

    display_list_emojis = sorted_emojis[:limit]
    emoji_lines = []
    podium_emojis = ["🥇", "🥈", "🥉"]

    for rank, (emoji_key, count) in enumerate(display_list_emojis, 1):
        display_emoji = utils.escape_markdown(str(emoji_key))
        if isinstance(emoji_key, int):
            found_emoji = bot.get_emoji(emoji_key)
            if found_emoji: display_emoji = str(found_emoji)
            else: display_emoji = f"`ID:{emoji_key}`"
        elif isinstance(emoji_key, str):
            try: unicodedata.name(emoji_key); display_emoji = emoji_key
            except (TypeError, ValueError): pass

        rank_prefix = podium_emojis[rank-1] if rank <= 3 else f"`#{rank:02d}`"
        emoji_lines.append(f"{rank_prefix} {display_emoji} — **{count:,}** lần")

    if len(sorted_emojis) > limit:
        emoji_lines.append(f"\n... và {len(sorted_emojis) - limit} emoji khác.")

    embed.description = desc_base
    if bar_chart_str:
        embed.description += "\n\n" + bar_chart_str
    embed.description += "\n\n" + "\n".join(emoji_lines)

    if len(embed.description) > 4096:
        embed.description = embed.description[:4090] + "\n[...]"

    footer_text = "Đã bật Reaction Scan và có quyền đọc lịch sử."
    if config.REACTION_UNICODE_EXCEPTIONS: footer_text += " Đã lọc emoji Unicode."
    embed.set_footer(text=footer_text)
    return embed

async def create_least_filtered_reaction_embed(
    filtered_reaction_counts: collections.Counter,
    bot: discord.Client,
    limit: int = LEAST_EMOJI_REACTION_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị các emoji reactions ÍT phổ biến nhất."""
    if not filtered_reaction_counts: return None
    e = lambda name: utils.get_emoji(name, bot)

    title_emoji = '📉'
    title_item_emoji = e('reaction') if e('reaction') != '❓' else '👍'
    embed = discord.Embed(
        title=f"{title_emoji} {title_item_emoji} Top {limit} Emoji Reactions Ít Phổ Biến Nhất",
        color=discord.Color.light_grey()
    )
    desc_parts = ["*Dựa trên số lượt thả reaction tin nhắn.*"]
    if config.REACTION_UNICODE_EXCEPTIONS:
        desc_parts.append(f"*Chỉ bao gồm emoji của server và: {' '.join(config.REACTION_UNICODE_EXCEPTIONS)}*")
    else:
        desc_parts.append("*Chỉ bao gồm emoji của server.*")
    desc_parts.append("*Chỉ hiển thị emoji có > 0 lượt thả.*")
    desc_base = "\n".join(desc_parts)

    sorted_emojis = sorted(
        [item for item in filtered_reaction_counts.items() if item[1] > 0],
        key=lambda item: item[1]
    )

    if not sorted_emojis:
        embed.description = desc_base + "\n\n*Không có emoji reaction nào (đã lọc) được sử dụng ít nhất 1 lần.*"
        return embed

    display_list_emojis = sorted_emojis[:limit]
    emoji_lines = []
    for rank, (emoji_key, count) in enumerate(display_list_emojis, 1):
        display_emoji = utils.escape_markdown(str(emoji_key))
        if isinstance(emoji_key, int):
            found_emoji = bot.get_emoji(emoji_key)
            if found_emoji: display_emoji = str(found_emoji)
            else: display_emoji = f"`ID:{emoji_key}`"
        elif isinstance(emoji_key, str):
            try: unicodedata.name(emoji_key); display_emoji = emoji_key
            except (TypeError, ValueError): pass

        rank_prefix = f"`#{rank:02d}`"
        emoji_lines.append(f"{rank_prefix} {display_emoji} — **{count:,}** lần")

    if len([item for item in filtered_reaction_counts.items() if item[1] > 0]) > limit:
        emoji_lines.append(f"\n... và {len(filtered_reaction_counts) - len(display_list_emojis)} emoji khác (> 0 lượt).")


    embed.description = desc_base + "\n\n" + "\n".join(emoji_lines)
    if len(embed.description) > 4096:
        embed.description = embed.description[:4090] + "\n[...]"

    footer_text = "Đã bật Reaction Scan và có quyền đọc lịch sử."
    if config.REACTION_UNICODE_EXCEPTIONS: footer_text += " Đã lọc emoji Unicode."
    embed.set_footer(text=footer_text)
    return embed

async def create_top_content_emoji_embed(
    content_emoji_counts: collections.Counter,
    bot: discord.Client,
    guild: discord.Guild,
    limit: int = TOP_CONTENT_EMOJI_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top emoji CỦA SERVER được dùng trong nội dung tin nhắn."""
    if not content_emoji_counts: return None
    e = lambda name: utils.get_emoji(name, bot)

    server_emoji_ids = {emoji.id for emoji in guild.emojis}
    server_content_counts = collections.Counter({
        emoji_id: count
        for emoji_id, count in content_emoji_counts.items()
        if emoji_id in server_emoji_ids and count > 0
    })

    if not server_content_counts: return None

    title_emoji = e('award') if e('award') != '❓' else '🏆'
    title_item_emoji = e('mention') if e('mention') != '❓' else '😀'
    embed = discord.Embed(
        title=f"{title_emoji} {title_item_emoji} Top {limit} Emoji Server Dùng Trong Tin Nhắn",
        color=discord.Color.yellow()
    )
    desc_base = "*Dựa trên số lần emoji CỦA SERVER NÀY xuất hiện trong nội dung tin nhắn.*"

    sorted_emojis = server_content_counts.most_common()
    if not sorted_emojis: return None

    bar_chart_str = ""
    data_for_chart = sorted_emojis[:5]
    if data_for_chart:
        async def format_emoji_id_key(emoji_id):
            emoji = bot.get_emoji(emoji_id)
            return str(emoji) if emoji else f"ID:{emoji_id}"

        bar_chart_str = await utils.create_vertical_text_bar_chart(
            sorted_data=data_for_chart,
            key_formatter=format_emoji_id_key,
            top_n=5, max_chart_height=8, bar_width=1, bar_spacing=1,
            chart_title="Top 5 Emoji Content", show_legend=True
        )

    display_list_emojis = sorted_emojis[:limit]
    emoji_lines = []
    podium_emojis = ["🥇", "🥈", "🥉"]

    for rank, (emoji_id, count) in enumerate(display_list_emojis, 1):
        display_emoji = f"`ID:{emoji_id}`"
        found_emoji = bot.get_emoji(emoji_id)
        if found_emoji: display_emoji = str(found_emoji)

        rank_prefix = podium_emojis[rank-1] if rank <= 3 else f"`#{rank:02d}`"
        emoji_lines.append(f"{rank_prefix} {display_emoji} — **{count:,}** lần")

    if len(server_content_counts) > limit:
        emoji_lines.append(f"\n... và {len(server_content_counts) - limit} emoji server khác.")

    embed.description = desc_base
    if bar_chart_str:
        embed.description += "\n\n" + bar_chart_str
    embed.description += "\n\n" + "\n".join(emoji_lines)

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
    """Tạo embed xếp hạng người dùng thả reaction nhiều nhất (đã lọc)."""
    e = lambda name: utils.get_emoji(name, bot)
    emoji_cache: Dict[int, discord.Emoji] = scan_data.get("server_emojis_cache", {})

    async def get_top_given_emoji(user_id, _):
        user_specific_counts = user_reaction_emoji_given_counts.get(user_id, Counter())
        if user_specific_counts:
            try:
                most_used_key, top_count = max(user_specific_counts.items(), key=lambda item: item[1])
                if isinstance(most_used_key, int):
                    emoji_obj = emoji_cache.get(most_used_key) or bot.get_emoji(most_used_key)
                    if emoji_obj: return f"• Top Thả: {str(emoji_obj)} ({top_count:,})"
                    else: return f"• Top Thả ID: `{most_used_key}` ({top_count:,})"
                elif isinstance(most_used_key, str):
                     try: unicodedata.name(most_used_key); return f"• Top Thả: {most_used_key} ({top_count:,})"
                     except (TypeError, ValueError): return f"• Top Thả: `{most_used_key}` ({top_count:,})"
            except ValueError: pass
            except Exception as e_find: log.warning(f"Lỗi tìm top reaction giver emoji cho user {user_id}: {e_find}")
        return None

    def get_footer_note(*args):
        footer = "Đã bật Reaction Scan và có quyền đọc lịch sử."
        if config.REACTION_UNICODE_EXCEPTIONS: footer += " Đã lọc emoji Unicode."
        return footer

    # Gọi helper từ utils
    return await utils.create_user_leaderboard_embed(
        title=f"{e('reaction')} Top {limit} Người Thả Reaction Nhiều Nhất",
        counts=user_reaction_given_counts,
        value_key=None,
        guild=guild,
        bot=bot,
        limit=limit,
        item_name_singular="reaction",
        item_name_plural="reactions",
        e=e,
        color=discord.Color.teal(),
        filter_admins=filter_admins,
        secondary_info_getter=get_top_given_emoji,
        tertiary_info_getter=get_footer_note,
        minimum_value=1,
        show_bar_chart=True
    )

async def create_tracked_role_grant_leaderboards(
    tracked_role_grants: Optional[collections.Counter],
    guild: discord.Guild,
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds xếp hạng cho các role được theo dõi lượt cấp."""
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
    if all_user_ids:
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

        filtered_sorted_users = role_counter.most_common()
        if not filtered_sorted_users: continue

        total_users_in_lb = len(filtered_sorted_users)
        users_to_display = filtered_sorted_users[:limit]

        bar_chart_str = ""
        data_for_chart = filtered_sorted_users[:5]
        if data_for_chart:
             async def format_user_key(user_id):
                 user = user_cache.get(user_id)
                 return utils.escape_markdown(user.display_name) if user else f"ID:{user_id}"

             bar_chart_str = await utils.create_vertical_text_bar_chart(
                 sorted_data=data_for_chart,
                 key_formatter=format_user_key,
                 top_n=5, max_chart_height=8, bar_width=1, bar_spacing=2,
                 chart_title=f"Top 5 Nhận {role.name}", show_legend=True
             )

        embed = discord.Embed(
            title=f"{title_emoji} {title_item_emoji} Top Nhận Role: {role.mention}",
            color=role.color if role.color.value != 0 else discord.Color.purple()
        )
        desc_prefix = f"*Số lần nhận role '{utils.escape_markdown(role.name)}' từ Audit Log.*"
        description_lines = [desc_prefix]
        if bar_chart_str: description_lines.append(bar_chart_str)
        description_lines.append("")

        for rank, (user_id, count) in enumerate(users_to_display, 1):
            # Gọi helper định dạng cây từ utils
            lines = await utils._format_user_tree_line(
                rank, user_id, count, item_name_singular, item_name_plural,
                guild, user_cache, secondary_info=None
            )
            description_lines.extend(lines)

        if description_lines and description_lines[-1] == "": description_lines.pop()
        final_description = "\n".join(description_lines)
        if len(final_description) > 4096:
             cutoff_point = final_description.rfind('\n', 0, 4080);
             if cutoff_point != -1: final_description = final_description[:cutoff_point] + "\n[...]"
             else: final_description = final_description[:4090] + "\n[...]"
        embed.description = final_description

        if total_users_in_lb > limit:
            embed.set_footer(text=f"... và {total_users_in_lb - limit} người khác.")
        embeds.append(embed)

    return embeds


async def create_error_embed(
    scan_errors: List[str],
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed tóm tắt các lỗi và cảnh báo xảy ra trong quá trình quét."""
    if not scan_errors: return None
    e = lambda name: utils.get_emoji(name, bot)

    error_embed = discord.Embed(
        title=f"{e('error')} Tóm tắt Lỗi và Cảnh báo Khi Quét ({len(scan_errors)} mục)",
        color=discord.Color.dark_red(),
        timestamp=discord.utils.utcnow()
    )
    errors_per_page = 15
    error_text_lines = []
    errors_shown = 0
    total_error_len = 0
    max_len = 4000

    for i, err in enumerate(scan_errors):
        line_prefix = e('warning') if "warn" in str(err).lower() or "bỏ qua" in str(err).lower() else e('error')
        line = f"{line_prefix} {utils.escape_markdown(str(err)[:350])}"
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
                elif not error_text_lines[-1].startswith(f"\n{e('warning')} ..."):
                    error_text_lines.append(f"\n{e('warning')} ... (và nhiều lỗi/cảnh báo khác)")
            break

    error_embed.description = "\n".join(error_text_lines) if error_text_lines else f"{e('success')} Không có lỗi hoặc cảnh báo nào được ghi nhận."
    if len(error_embed.description) > 4096: error_embed.description = error_embed.description[:4090] + "\n[...]"
    error_embed.set_footer(text="Kiểm tra log chi tiết trong thread (nếu có) hoặc console.")
    return error_embed

# --- END OF FILE reporting/embeds_analysis.py ---