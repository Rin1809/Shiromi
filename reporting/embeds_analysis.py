# --- START OF FILE reporting/embeds_analysis.py ---
import discord
import datetime
import math
import logging
import collections
import asyncio
from typing import List, Dict, Any, Optional, Union

# Relative import
try:
    from .. import utils
    from .embeds_user import create_generic_leaderboard_embed
except ImportError:
    import utils
    # Cần import trực tiếp nếu chạy riêng lẻ
    # from embeds_user import create_generic_leaderboard_embed
    pass # Bỏ qua nếu không import được embeds_user khi chạy riêng

log = logging.getLogger(__name__)

# --- Constants ---
AUDIT_LOG_ENTRIES_PER_EMBED = 12      # Số entry audit log mỗi embed chi tiết
PERMISSION_AUDIT_ITEMS_PER_EMBED = 10 # Số mục mỗi embed phân tích quyền
KEYWORD_RANKING_LIMIT = 10            # Giới hạn hiển thị top kênh/user theo keyword
ROLES_STATS_PER_EMBED = 15            # Số role mỗi embed thống kê role
TOP_ROLES_GRANTED_LIMIT = 30          # Giới hạn hiển thị top role được cấp
REACTIONS_PER_EMBED = 20              # Giới hạn emoji reaction mỗi embed
TOP_EMOJI_REACTION_USAGE_LIMIT = 10   # Giới hạn top emoji reaction


# --- Embed Functions ---

async def create_permission_audit_embeds(
    permission_results: Dict[str, List[Dict[str, Any]]],
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo danh sách embeds cho kết quả phân tích quyền."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    if not permission_results:
        # Trả về embed thông báo nếu không có kết quả
        no_result_embed = discord.Embed(
            title=f"{e('shield')} Phân tích Quyền",
            description=f"{e('info')} Không có kết quả phân tích quyền.",
            color=discord.Color.light_grey()
        )
        return [no_result_embed]

    # 1. Roles có quyền Administrator
    roles_admin = permission_results.get("roles_with_admin", [])
    admin_embed = discord.Embed(
        title=f"{e('shield')}{e('crown')} Roles có quyền Administrator",
        color=discord.Color.red()
    )
    if roles_admin:
        admin_list_lines = []
        # Sắp xếp theo vị trí cao nhất trước
        roles_admin.sort(key=lambda r: r.get('position', 0), reverse=True)
        for role_info in roles_admin[:PERMISSION_AUDIT_ITEMS_PER_EMBED]:
            member_count_str = f"({e('members')} {role_info.get('member_count', 'N/A')})"
            line = (f"- <@&{role_info['id']}> (`{utils.escape_markdown(role_info['name'])}`) "
                    f"- Pos: {role_info['position']} {member_count_str}")
            admin_list_lines.append(line)

        if len(roles_admin) > PERMISSION_AUDIT_ITEMS_PER_EMBED:
            admin_list_lines.append(f"\n... và {len(roles_admin) - PERMISSION_AUDIT_ITEMS_PER_EMBED} role khác.")
        admin_embed.description = "\n".join(admin_list_lines)
    else:
        admin_embed.description = f"{e('success')} Không tìm thấy role nào có quyền Administrator."
        admin_embed.color = discord.Color.green()
    embeds.append(admin_embed)

    # 2. Kênh có quyền @everyone Tiềm ẩn Rủi ro
    risky_everyone = permission_results.get("risky_everyone_overwrites", [])
    everyone_embed = discord.Embed(
        title=f"{e('error')} Kênh có quyền @everyone Tiềm ẩn Rủi ro",
        color=discord.Color.orange()
    )
    if risky_everyone:
        everyone_list_lines = []
        # Nhóm theo kênh để hiển thị gọn hơn
        channels_affected = collections.defaultdict(list)
        for item in risky_everyone:
            channel_mention = f"<#{item['channel_id']}>"
            perm_list = ", ".join(f"`{p}`" for p in item['permissions'])
            type_emoji = item.get('channel_type_emoji', '❓')
            channels_affected[channel_mention].append(f"{type_emoji} {perm_list}")

        items_shown = 0
        for channel_mention, perms_list in channels_affected.items():
            if items_shown >= PERMISSION_AUDIT_ITEMS_PER_EMBED:
                break
            # Tìm tên kênh từ dữ liệu gốc (nếu có)
            original_item = next((i for i in risky_everyone if f"<#{i.get('channel_id')}>" == channel_mention), None)
            channel_name = f" (`{utils.escape_markdown(original_item.get('channel_name', 'N/A'))}`)" if original_item else ""

            everyone_list_lines.append(f"**{channel_mention}{channel_name}**:")
            for perms_with_emoji in perms_list:
                everyone_list_lines.append(f"  └ {perms_with_emoji}") # Thêm thụt lề
            items_shown += 1

        if len(risky_everyone) > items_shown: # Tổng số mục gốc, không phải số kênh
            everyone_list_lines.append(f"\n... và {len(risky_everyone) - items_shown} mục quyền khác trong các kênh.")
        everyone_embed.description = "\n".join(everyone_list_lines)
    else:
        everyone_embed.description = f"{e('success')} Không tìm thấy kênh nào có quyền @everyone nguy hiểm."
        everyone_embed.color = discord.Color.green()
    embeds.append(everyone_embed)

    # 3. Roles Khác có Quyền Rủi ro (Không phải Admin/Bot)
    other_risky = permission_results.get("other_risky_role_perms", [])
    other_embed = discord.Embed(
        title=f"{e('warning')} Roles Khác có Quyền Rủi ro (Không phải Admin/Bot)",
        color=discord.Color.gold()
    )
    if other_risky:
        other_list_lines = []
        other_risky.sort(key=lambda r: r.get('position', 0), reverse=True)
        for role_info in other_risky[:PERMISSION_AUDIT_ITEMS_PER_EMBED]:
            perm_list = ", ".join(f"`{p}`" for p in role_info['permissions'])
            member_count_str = f"({e('members')} {role_info.get('member_count', 'N/A')})"
            role_line = (f"**<@&{role_info['role_id']}> (`{utils.escape_markdown(role_info['role_name'])}`)** "
                         f"- Pos: {role_info['position']}")
            perm_line = f"  └ Quyền: {perm_list} {member_count_str}"
            other_list_lines.append(role_line)
            other_list_lines.append(perm_line)

        if len(other_risky) > PERMISSION_AUDIT_ITEMS_PER_EMBED:
            other_list_lines.append(f"\n... và {len(other_risky) - PERMISSION_AUDIT_ITEMS_PER_EMBED} role khác.")
        other_embed.description = "\n".join(other_list_lines)
    else:
        other_embed.description = f"{e('success')} Không tìm thấy role không phải admin/bot nào có các quyền nguy hiểm được liệt kê."
        other_embed.color = discord.Color.green()
    embeds.append(other_embed)

    return embeds


async def _fetch_and_format_username(user_id: Optional[Union[str, int]], guild: discord.Guild, bot: discord.Client, user_cache: Dict[int, Optional[Union[discord.User, discord.Member]]]) -> str:
    """Lấy tên user từ cache hoặc fetch, định dạng cho hiển thị."""
    if user_id is None: return "Không rõ"
    try:
        user_id_int = int(user_id)
    except (ValueError, TypeError):
        return f"ID không hợp lệ: `{user_id}`"

    # Kiểm tra cache trước
    if user_id_int not in user_cache:
        user = await utils.fetch_user_data(guild, user_id_int, bot_ref=bot)
        user_cache[user_id_int] = user # Lưu vào cache (kể cả None)

    user = user_cache.get(user_id_int)

    if user:
        return f"{user.mention} (`{utils.escape_markdown(user.display_name)}`)"
    else:
        return f"ID: `{user_id_int}` (Unknown/Left)"


async def create_audit_log_summary_embeds(
    audit_logs: List[Dict[str, Any]],
    guild: discord.Guild,
    bot: discord.Client,
    limit_per_embed: int = AUDIT_LOG_ENTRIES_PER_EMBED
) -> List[discord.Embed]:
    """Tạo embeds tóm tắt và chi tiết Audit Log từ dữ liệu đã fetch."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    if not audit_logs:
        no_logs_embed = discord.Embed(
            title=f"{e('shield')} Audit Log",
            description=f"{e('info')} Không có dữ liệu Audit Log để hiển thị (theo bộ lọc).",
            color=discord.Color.light_grey()
        )
        return [no_logs_embed]

    # Sắp xếp log theo thời gian giảm dần (mới nhất trước)
    audit_logs.sort(
        key=lambda x: x.get('created_at') if isinstance(x.get('created_at'), datetime.datetime) else datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
        reverse=True
    )

    # --- Tạo Embed Tóm tắt ---
    action_counts = collections.Counter(log['action_type'] for log in audit_logs)
    mod_action_counts = collections.Counter()
    user_cache: Dict[int, Optional[Union[discord.User, discord.Member]]] = {} # Cache để fetch user 1 lần
    bot_ids = {m.id for m in guild.members if m.bot} # Lấy ID các bot trong guild

    # Đếm số action của mod (không phải bot)
    for log_entry in audit_logs:
        user_id = log_entry.get('user_id')
        if user_id:
            try:
                mod_id_int = int(user_id)
                if mod_id_int not in bot_ids:
                    mod_action_counts[mod_id_int] += 1
            except (ValueError, TypeError): pass # Bỏ qua nếu ID không hợp lệ

    num_total_logs_in_batch = len(audit_logs)
    summary_embed = discord.Embed(
        title=f"{e('shield')} Tóm tắt Audit Log Gần Đây",
        description=f"Phân tích **{num_total_logs_in_batch}** entry gần nhất được lưu.",
        color=discord.Color.dark_blue(),
        timestamp=discord.utils.utcnow()
    )

    # Top Actions
    top_actions = action_counts.most_common(10)
    action_summary_lines = [f"- `{action}`: {count:,}" for action, count in top_actions]
    if len(action_counts) > 10:
        action_summary_lines.append(f"- ... và {len(action_counts) - 10} loại khác.")
    action_summary = "\n".join(action_summary_lines)
    summary_embed.add_field(
        name=f"{e('stats')} Top Actions",
        value=action_summary if action_summary else "Không có",
        inline=False
    )

    # Top Hoạt động Mod (Lọc Bot)
    top_mods = mod_action_counts.most_common(5)
    mod_summary_lines = []
    for mod_id_int, count in top_mods:
        mod_name = await _fetch_and_format_username(mod_id_int, guild, bot, user_cache)
        mod_summary_lines.append(f"- {mod_name}: {count:,} hành động")
    if len(mod_action_counts) > 5:
        mod_summary_lines.append(f"- ... và {len(mod_action_counts) - 5} mod khác.")
    mod_summary = "\n".join(mod_summary_lines)
    summary_embed.add_field(
        name=f"{e('crown')} Top Hoạt động Mod (Lọc Bot)",
        value=mod_summary if mod_summary else "Không có",
        inline=False
    )
    embeds.append(summary_embed)

    # --- Tạo Embeds Chi tiết (chia trang) ---
    if audit_logs:
        num_detail_embeds = math.ceil(num_total_logs_in_batch / limit_per_embed)
        for i in range(num_detail_embeds):
            start_index = i * limit_per_embed
            end_index = start_index + limit_per_embed
            log_batch = audit_logs[start_index:end_index]

            detail_embed = discord.Embed(
                title=f"{e('shield')} Chi tiết Audit Log (Phần {i + 1}/{num_detail_embeds})",
                color=discord.Color.blue()
            )
            log_details_lines = []
            for log_entry_dict in log_batch:
                actor = await _fetch_and_format_username(log_entry_dict.get('user_id'), guild, bot, user_cache)
                action = log_entry_dict.get('action_type', 'unknown_action')
                target_str = ""
                target_id = log_entry_dict.get('target_id')
                if target_id:
                    # Cố gắng hiển thị tên target nếu có thể (ví dụ: user, role, channel)
                    # Tuy nhiên, để đơn giản, chỉ hiển thị ID
                    target_str = f" -> Target ID: `{target_id}`"

                reason = log_entry_dict.get('reason')
                reason_str = f" | Lý do: *{utils.escape_markdown(reason)}*" if reason else ""

                created_at_dt = log_entry_dict.get('created_at')
                time_str = utils.format_discord_time(created_at_dt, 'R') if isinstance(created_at_dt, datetime.datetime) else "N/A"

                log_line = f"**[{action}]** {actor}{target_str} ({time_str}){reason_str}"
                log_details_lines.append(log_line)

            detail_embed.description = "\n".join(log_details_lines) if log_details_lines else "Không có log trong phần này."
            # Giới hạn độ dài description
            if len(detail_embed.description) > 4000:
                detail_embed.description = detail_embed.description[:4000] + "\n... (quá dài)"
            embeds.append(detail_embed)
    return embeds


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

    # --- Embed Tổng quan Keyword ---
    kw_overall_embed = discord.Embed(
        title=f"{e('hashtag')} Phân tích Từ khóa",
        description=f"Đếm **{len(target_keywords)}** từ khóa (không phân biệt hoa thường).",
        color=discord.Color.blue()
    )
    kw_summary_lines = []
    # Sắp xếp theo số lần xuất hiện giảm dần
    sorted_keywords = sorted(keyword_counts.items(), key=lambda item: item[1], reverse=True)
    for keyword, count in sorted_keywords[:15]: # Giới hạn 15 dòng đầu
        kw_summary_lines.append(f"- `{utils.escape_markdown(keyword)}`: **{count:,}** lần")
    if len(sorted_keywords) > 15:
        kw_summary_lines.append(f"- ... và {len(sorted_keywords)-15} từ khóa khác.")
    kw_overall_embed.add_field(
        name="Tổng số lần xuất hiện",
        value="\n".join(kw_summary_lines) if kw_summary_lines else "Không tìm thấy.",
        inline=False
    )
    embeds.append(kw_overall_embed)

    # --- Embed Top Kênh/Luồng theo Keyword ---
    kw_channel_embed = discord.Embed(
        title=f"{e('text_channel')}/{e('thread')} Top Kênh/Luồng theo Từ khóa",
        color=discord.Color.green()
    )
    channel_kw_ranking = []
    # Gộp dữ liệu từ kênh và luồng
    all_location_counts = {**channel_keyword_counts, **thread_keyword_counts}
    for loc_id, counts in all_location_counts.items():
        total_count = sum(counts.values())
        if total_count > 0:
            location_obj = guild.get_channel_or_thread(loc_id) # Thử lấy object từ cache
            loc_mention = location_obj.mention if location_obj else f"`ID:{loc_id}`"
            loc_name = f" (`{utils.escape_markdown(location_obj.name)}`)" if location_obj else ""
            loc_type_emoji = utils.get_channel_type_emoji(location_obj, bot) if location_obj else "❓"
            channel_kw_ranking.append({
                "mention": loc_mention, "name": loc_name,
                "total": total_count, "details": dict(counts), "emoji": loc_type_emoji
            })

    # Sắp xếp theo tổng số keyword giảm dần
    channel_kw_ranking.sort(key=lambda x: x['total'], reverse=True)
    channel_rank_lines = []
    for i, item in enumerate(channel_kw_ranking[:KEYWORD_RANKING_LIMIT]):
        # Hiển thị chi tiết các keyword trong kênh/luồng đó
        details = ", ".join(f"`{kw}`:{c:,}" for kw, c in item['details'].items())
        if len(details) > 150: details = details[:150] + "..." # Giới hạn độ dài chi tiết

        channel_rank_lines.append(f"**`#{i+1:02d}`**. {item['emoji']} {item['mention']}{item['name']} ({item['total']:,} tổng)")
        channel_rank_lines.append(f"   └ {details}") # Chi tiết thụt vào

    if not channel_rank_lines:
        channel_rank_lines.append("Không có kênh/luồng nào chứa từ khóa.")
    if len(channel_kw_ranking) > KEYWORD_RANKING_LIMIT:
        channel_rank_lines.append(f"\n... và {len(channel_kw_ranking) - KEYWORD_RANKING_LIMIT} kênh/luồng khác.")

    kw_channel_embed.description = "\n".join(channel_rank_lines)
    embeds.append(kw_channel_embed)

    # --- Embed Top User theo Keyword (dùng generic helper) ---
    user_total_keyword_counts = collections.Counter({
        uid: sum(counts.values())
        for uid, counts in user_keyword_counts.items() if sum(counts.values()) > 0
    })
    if user_total_keyword_counts:
        try:
            # Gọi hàm tạo leaderboard chung
            kw_user_embed = await create_generic_leaderboard_embed(
                user_total_keyword_counts, guild, bot,
                f"{e('members')} Top User theo Từ khóa (Tổng)",
                "lần dùng", "lần dùng", 15, discord.Color.orange(), show_total=False
            )
            if kw_user_embed:
                embeds.append(kw_user_embed)
        except NameError:
             log.warning("Không thể tạo embed Top User theo Keyword do thiếu 'create_generic_leaderboard_embed'.")
        except Exception as user_kw_err:
             log.error(f"Lỗi tạo embed Top User theo Keyword: {user_kw_err}", exc_info=True)


    return embeds


async def create_role_change_stats_embeds(
    role_change_stats: Dict[str, Dict[str, collections.Counter]],
    guild: discord.Guild,
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds thống kê việc cấp/hủy role bởi moderator."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    if not role_change_stats:
        no_stats_embed = discord.Embed(
            title=f"{e('role')}{e('stats')} Thống kê Cấp/Hủy Role (Bởi Mod)",
            description=f"{e('info')} Không có dữ liệu thay đổi role từ Audit Log.",
            color=discord.Color.light_grey()
        )
        return [no_stats_embed]

    # Lấy ID tất cả các mod đã thực hiện thay đổi để fetch một lần
    all_mod_ids = set()
    for stats in role_change_stats.values():
        all_mod_ids.update(stats.get("added", {}).keys())
        all_mod_ids.update(stats.get("removed", {}).keys())

    # Fetch user data cho các mod và cache lại
    log.debug(f"Fetching data for {len(all_mod_ids)} moderators for role change stats...")
    mod_user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    fetch_tasks = [
        _fetch_and_format_username(mod_id, guild, bot, mod_user_cache)
        for mod_id in all_mod_ids
    ]
    # Chạy song song các task fetch
    mod_names_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    # Xử lý kết quả (chủ yếu là để log lỗi nếu có)
    for idx, result in enumerate(mod_names_results):
        if isinstance(result, Exception):
             mod_id = list(all_mod_ids)[idx]
             log.warning(f"Lỗi fetch mod {mod_id} for role stats: {result}")
    log.debug("Fetch moderator data complete.")


    # Sắp xếp role theo tổng số lần thay đổi (added + removed)
    sorted_role_ids = sorted(
        role_change_stats.keys(),
        key=lambda rid: sum(role_change_stats[rid].get('added', {}).values()) + sum(role_change_stats[rid].get('removed', {}).values()),
        reverse=True
    )

    num_role_stat_embeds = math.ceil(len(sorted_role_ids) / ROLES_STATS_PER_EMBED)

    for i in range(num_role_stat_embeds):
        start_index = i * ROLES_STATS_PER_EMBED
        end_index = start_index + ROLES_STATS_PER_EMBED
        role_batch_ids = sorted_role_ids[start_index:end_index]

        embed = discord.Embed(
            title=f"{e('role')}{e('stats')} Thống kê Cấp/Hủy Role (Bởi Mod - Phần {i + 1}/{num_role_stat_embeds})",
            description="*Dựa trên Audit Log đã quét.*",
            color=discord.Color.magenta()
        )
        field_count = 0
        for role_id_str in role_batch_ids:
            # Giới hạn số field mỗi embed để tránh lỗi Discord
            if field_count >= 24: break

            try:
                role_id_int = int(role_id_str)
            except ValueError:
                log.warning(f"Bỏ qua role_id không hợp lệ: {role_id_str}")
                continue

            role = guild.get_role(role_id_int)
            role_mention = role.mention if role else f"`{role_id_str}`"
            role_name = f" (`{utils.escape_markdown(role.name)}`)" if role else " (Unknown/Deleted)"

            added_counter = role_change_stats[role_id_str].get('added', Counter())
            removed_counter = role_change_stats[role_id_str].get('removed', Counter())
            total_added = sum(added_counter.values())
            total_removed = sum(removed_counter.values())

            # Chỉ hiển thị nếu có thay đổi
            if total_added == 0 and total_removed == 0:
                continue

            value_lines = []
            # Hiển thị thông tin cấp role
            if total_added > 0:
                top_adder_info = ""
                if added_counter:
                    adder_id, add_count = added_counter.most_common(1)[0]
                    adder_name = await _fetch_and_format_username(adder_id, guild, bot, mod_user_cache)
                    top_adder_info = f" (Top: {adder_name} - {add_count:,})"
                value_lines.append(f"**Được cấp:** {total_added:,} lần{top_adder_info}")

            # Hiển thị thông tin hủy role
            if total_removed > 0:
                top_remover_info = ""
                if removed_counter:
                    remover_id, remove_count = removed_counter.most_common(1)[0]
                    remover_name = await _fetch_and_format_username(remover_id, guild, bot, mod_user_cache)
                    top_remover_info = f" (Top: {remover_name} - {remove_count:,})"
                value_lines.append(f"**Bị hủy:** {total_removed:,} lần{top_remover_info}")

            if value_lines:
                field_name = f"{role_mention}{role_name}"
                field_value = "\n".join(value_lines)
                # Giới hạn độ dài value của field
                if len(field_value) > 1024:
                    field_value = field_value[:1020] + "..."

                try:
                    embed.add_field(name=field_name, value=field_value, inline=False)
                    field_count += 1
                except Exception as field_err:
                    log.error(f"Lỗi thêm field role stats (mod) {role_id_str}: {field_err}")

        if not embed.fields:
            embed.description = "Không có dữ liệu thay đổi role cho phần này."
        embeds.append(embed)
    return embeds


async def create_user_role_change_embeds(
    user_role_changes: Dict[int, Dict[str, Dict[str, int]]],
    guild: discord.Guild,
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds thống kê việc cấp/hủy role cho từng user."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    if not user_role_changes:
        no_stats_embed = discord.Embed(
            title=f"{e('members')}{e('role')} Thống kê Cấp/Hủy Role (Cho User)",
            description=f"{e('info')} Không có dữ liệu thay đổi role cho user từ Audit Log.",
            color=discord.Color.light_grey()
        )
        return [no_stats_embed]

    # Sắp xếp user theo tổng số lần thay đổi role (added + removed)
    sorted_user_ids = sorted(
        user_role_changes.keys(),
        key=lambda uid: sum(stats.get("added", 0) + stats.get("removed", 0) for stats in user_role_changes[uid].values()),
        reverse=True
    )

    # Fetch user data và cache
    log.debug(f"Fetching data for {len(sorted_user_ids)} users for user role change stats...")
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    fetch_tasks = [
        _fetch_and_format_username(uid, guild, bot, user_cache)
        for uid in sorted_user_ids
    ]
    await asyncio.gather(*fetch_tasks, return_exceptions=True) # Cache sẽ được điền trong _fetch_and_format_username
    log.debug("Fetch user data complete for user role stats.")


    num_user_stat_embeds = math.ceil(len(sorted_user_ids) / USER_ROLE_STATS_PER_EMBED)

    for i in range(num_user_stat_embeds):
        start_index = i * USER_ROLE_STATS_PER_EMBED
        end_index = start_index + USER_ROLE_STATS_PER_EMBED
        user_batch_ids = sorted_user_ids[start_index:end_index]

        embed = discord.Embed(
            title=f"{e('members')}{e('role')} Thống kê Cấp/Hủy Role (Cho User - Phần {i + 1}/{num_user_stat_embeds})",
            description="*Dựa trên Audit Log. Sắp xếp theo tổng số thay đổi.*",
            color=discord.Color.dark_magenta()
        )
        field_count = 0
        for user_id in user_batch_ids:
            if field_count >= 24: break

            user_stats = user_role_changes.get(user_id)
            if not user_stats: continue # Bỏ qua nếu không có dữ liệu (dù không nên xảy ra)

            user_display_name = await _fetch_and_format_username(user_id, guild, bot, user_cache)

            value_lines = []
            # Sắp xếp các role thay đổi của user này theo tổng số lần thay đổi
            sorted_roles_for_user = sorted(
                user_stats.items(),
                key=lambda item: item[1].get("added", 0) + item[1].get("removed", 0),
                reverse=True
            )

            roles_shown = 0
            max_roles_per_user = 4 # Giới hạn số role hiển thị cho mỗi user để tránh quá dài
            for role_id_str, changes in sorted_roles_for_user:
                if roles_shown >= max_roles_per_user: break

                added_count = changes.get("added", 0)
                removed_count = changes.get("removed", 0)

                if added_count > 0 or removed_count > 0:
                    try:
                        role_id_int = int(role_id_str)
                        role = guild.get_role(role_id_int)
                        role_mention = role.mention if role else f"`{role_id_str}`"
                        role_name = f" (`{utils.escape_markdown(role.name)}`)" if role else " (Unknown/Deleted)"
                    except ValueError:
                        role_mention = f"`{role_id_str}`"
                        role_name = "(Invalid ID)"

                    change_parts = []
                    if added_count > 0: change_parts.append(f"+{added_count}")
                    if removed_count > 0: change_parts.append(f"-{removed_count}")
                    change_summary = ' / '.join(change_parts)

                    value_lines.append(f"- {role_mention}{role_name}: ({change_summary})")
                    roles_shown += 1

            if len(sorted_roles_for_user) > roles_shown:
                value_lines.append(f"- ... và {len(sorted_roles_for_user) - roles_shown} role khác.")

            if value_lines:
                field_name = user_display_name
                field_value = "\n".join(value_lines)
                if len(field_value) > 1024:
                    field_value = field_value[:1020] + "..."
                try:
                    embed.add_field(name=field_name, value=field_value, inline=False)
                    field_count += 1
                except Exception as field_err:
                     log.error(f"Lỗi thêm field user role stats {user_id}: {field_err}")

        if not embed.fields:
            embed.description = "Không có dữ liệu thay đổi role cho user trong phần này."
        embeds.append(embed)
    return embeds


async def create_top_roles_granted_embed(
    role_change_stats: Dict[str, Dict[str, collections.Counter]],
    guild: discord.Guild,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top roles được cấp nhiều nhất."""
    e = lambda name: utils.get_emoji(name, bot)
    if not role_change_stats: return None

    # Tính tổng số lần mỗi role được cấp
    role_grant_counts = collections.Counter({
        role_id_str: sum(stats.get('added', {}).values())
        for role_id_str, stats in role_change_stats.items()
        if sum(stats.get('added', {}).values()) > 0
    })

    if not role_grant_counts: return None

    embed = discord.Embed(
        title=f"{e('award')}{e('role')} Top Roles Được Cấp Nhiều Nhất",
        description="*Dựa trên số lần role được cấp (ADD) trong Audit Log.*",
        color=discord.Color.blue()
    )

    sorted_roles = role_grant_counts.most_common(TOP_ROLES_GRANTED_LIMIT)
    desc_lines = []
    for rank, (role_id_str, count) in enumerate(sorted_roles, 1):
        try:
            role_id_int = int(role_id_str)
            role = guild.get_role(role_id_int)
            role_mention = role.mention if role else f"`{role_id_str}`"
            role_name = f" (`{utils.escape_markdown(role.name)}`)" if role else " (Unknown/Deleted)"
        except ValueError:
            role_mention = f"`{role_id_str}`"
            role_name = "(Invalid ID)"

        desc_lines.append(f"**`#{rank:02d}`**. {role_mention}{role_name} — **{count:,}** lần")

    if len(role_grant_counts) > TOP_ROLES_GRANTED_LIMIT:
        desc_lines.append(f"\n... và {len(role_grant_counts) - TOP_ROLES_GRANTED_LIMIT} roles khác.")

    embed.description += "\n\n" + "\n".join(desc_lines)
    # Giới hạn độ dài description
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"
    return embed


async def create_reaction_analysis_embed(
    reaction_emoji_counts: collections.Counter,
    overall_total_reaction_count: int, # Tổng số reaction để tham khảo (không dùng trực tiếp)
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed cho phân tích sử dụng biểu cảm (thực chất là top usage)."""
    # Hàm này giờ chỉ là alias cho create_top_emoji_usage_embed
    return await create_top_emoji_usage_embed(reaction_emoji_counts, bot)


async def create_top_emoji_usage_embed(
    reaction_counts: collections.Counter,
    bot: discord.Client,
    limit: int = TOP_EMOJI_REACTION_USAGE_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed hiển thị top emoji reactions được sử dụng."""
    if not reaction_counts: return None
    e = lambda name: utils.get_emoji(name, bot)

    embed = discord.Embed(
        title=f"{e('award')} Top {limit} Emoji Reactions Được Dùng",
        color=discord.Color.gold()
    )
    desc = "*Dựa trên số lượt thả reaction.*"

    sorted_emojis = reaction_counts.most_common(limit)
    emoji_lines = []
    for rank, (emoji_key, count) in enumerate(sorted_emojis, 1):
        display_emoji = utils.escape_markdown(emoji_key) # Mặc định hiển thị key
        # Cố gắng lấy emoji thực tế từ bot nếu là emoji custom
        if emoji_key.startswith('<') and emoji_key.endswith('>') and bot:
            try:
                partial_emoji = discord.PartialEmoji.from_str(emoji_key)
                if partial_emoji.id:
                    found = bot.get_emoji(partial_emoji.id)
                    if found:
                        display_emoji = str(found) # Hiển thị emoji thực tế
            except Exception as parse_err:
                log.debug(f"Lỗi parse partial emoji '{emoji_key}': {parse_err}")

        emoji_lines.append(f"**`#{rank:02d}`**. {display_emoji} — **{count:,}** lần")

    if len(reaction_counts) > limit:
        emoji_lines.append(f"\n... và {len(reaction_counts) - limit} emoji khác.")

    embed.description = desc + "\n\n" + "\n".join(emoji_lines)
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    embed.set_footer(text="Yêu cầu bật Reaction Scan và quyền đọc lịch sử.")
    return embed


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

    errors_per_page = 20 # Số lỗi hiển thị tối đa trên embed
    error_text_lines = []
    errors_shown = 0
    total_error_len = 0
    max_len = 4000 # Giới hạn description

    for i, err in enumerate(scan_errors):
        # Escape markdown và giới hạn độ dài mỗi dòng lỗi
        line = f"- {utils.escape_markdown(str(err)[:300])}" # Giới hạn 300 ký tự mỗi lỗi
        if len(line) > 300: line += "..."

        # Kiểm tra độ dài trước khi thêm
        if total_error_len + len(line) + 1 > max_len:
            error_text_lines.append("\n... (quá nhiều lỗi để hiển thị)")
            break

        error_text_lines.append(line)
        total_error_len += len(line) + 1 # +1 cho dấu xuống dòng
        errors_shown += 1

        if errors_shown >= errors_per_page:
            if len(scan_errors) > errors_per_page:
                remaining_errors = len(scan_errors) - errors_per_page
                more_line = f"\n... và {remaining_errors} lỗi/cảnh báo khác."
                if total_error_len + len(more_line) <= max_len:
                     error_text_lines.append(more_line)
                else: # Nếu thêm dòng '...' cũng quá dài
                     error_text_lines.append("\n... (và nhiều lỗi khác)")
            break # Dừng sau khi đủ page hoặc hết lỗi

    error_embed.description = "\n".join(error_text_lines) if error_text_lines else "Không có lỗi nào."

    return error_embed

# --- END OF FILE reporting/embeds_analysis.py ---