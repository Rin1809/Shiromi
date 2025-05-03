# --- START OF FILE reporting/embeds_guild.py ---
import discord
import datetime
import math
import logging
import collections
from typing import List, Dict, Any, Optional, Union
from discord.ext import commands # Cần Context để lấy author nếu có

# Relative import
try:
    from .. import utils
except ImportError:
    import utils

log = logging.getLogger(__name__)

# --- Constants ---
VOICE_CHANNELS_PER_EMBED = 20   # Số kênh voice tĩnh mỗi embed
BOOSTERS_PER_EMBED = 20         # Số booster mỗi embed
ROLES_PER_EMBED = 25            # Số role mỗi embed
FIRST_MESSAGES_LIMIT = 10       # Số tin nhắn đầu tiên hiển thị trong log
FIRST_MESSAGES_CONTENT_PREVIEW = 100 # Độ dài preview nội dung tin nhắn đầu


# --- Embed Creation Functions ---

async def create_summary_embed(
    server: discord.Guild,
    bot: discord.Client,
    processed_channels_count: int,
    processed_threads_count: int,
    skipped_channels_count: int,
    skipped_threads_count: int,
    overall_total_message_count: int,
    user_activity_count: int,
    overall_duration: datetime.timedelta,
    initial_member_status_counts: collections.Counter,
    channel_counts: collections.Counter,
    all_roles_count: int,
    start_time: datetime.datetime, # Thời điểm bắt đầu quét
    ctx: Optional[commands.Context] = None, # Context để lấy tên người yêu cầu
    overall_total_reaction_count: Optional[int] = None # Tổng reaction nếu quét
) -> discord.Embed:
    """Tạo embed tóm tắt chính thông tin server và kết quả quét."""
    e = lambda name: utils.get_emoji(name, bot)

    # --- Chuẩn bị các giá trị hiển thị ---
    explicit_filter = str(server.explicit_content_filter).replace('_', ' ').title()
    mfa_level = "Yêu cầu (Cho Mod)" if server.mfa_level >= discord.MFALevel.require_2fa else "Không yêu cầu"
    notifications = "Chỉ @mention" if server.default_notifications == discord.NotificationLevel.only_mentions else "Tất cả tin nhắn"
    sys_channel_mention = server.system_channel.mention if server.system_channel else "Không có"
    rules_channel_mention = server.rules_channel.mention if server.rules_channel else "Không có"
    public_updates_channel_mention = server.public_updates_channel.mention if server.public_updates_channel else "Không có"
    afk_channel_mention = server.afk_channel.mention if server.afk_channel else "Không có"
    afk_timeout_str = f"{server.afk_timeout // 60} phút" if server.afk_timeout >= 60 else "N/A"

    # Tính năng server
    features_str = ", ".join(server.features) if server.features else "Không có"
    if len(features_str) > 800: features_str = features_str[:800] + "... (nhiều)"

    # Đếm user/bot từ cache (có thể không chính xác 100% nếu intent members tắt)
    member_count = len([m for m in server.members if not m.bot])
    bot_count = len([m for m in server.members if m.bot])

    # Chuỗi tóm tắt kết quả quét
    reaction_line = f"\n{e('reaction')} Tổng **{overall_total_reaction_count:,}** biểu cảm." if overall_total_reaction_count is not None else ""
    scan_summary = (
        f"Quét **{processed_channels_count:,}** kênh text/voice ({skipped_channels_count} lỗi/bỏ qua).\n"
        f"Quét **{processed_threads_count:,}** luồng ({skipped_threads_count} lỗi/bỏ qua).\n"
        f"Tổng **{overall_total_message_count:,}** tin nhắn."
        f"{reaction_line}\n"
        f"**{user_activity_count:,}** users có hoạt động.\n\n"
        f"{e('clock')} **Tổng thời gian quét:** {utils.format_timedelta(overall_duration, high_precision=True)}"
    )

    # --- Tạo Embed ---
    summary_embed = discord.Embed(
        title=f"{e('stats')} Báo cáo Quét Sâu Server: {server.name}",
        description=scan_summary,
        color=discord.Color.purple(),
        timestamp=start_time + overall_duration # Timestamp là lúc quét xong
    )
    if server.icon:
        summary_embed.set_thumbnail(url=server.icon.url)

    # --- Thêm Fields ---
    # Thông tin cơ bản
    summary_embed.add_field(name=f"{e('id_card')} Server ID", value=f"`{server.id}`", inline=True)

    # Lấy thông tin Owner
    owner = server.owner
    if not owner and server.owner_id: # Fetch nếu chưa có trong cache
        try:
            owner = await utils.fetch_user_data(server, server.owner_id, bot_ref=bot)
        except Exception as owner_err:
             log.warning(f"Lỗi fetch owner {server.owner_id}: {owner_err}")
             owner = None
    owner_mention = owner.mention if owner else (f'`{server.owner_id}` (Không rõ)' if server.owner_id else 'Không rõ')
    summary_embed.add_field(name=f"{e('crown')} Chủ sở hữu", value=owner_mention, inline=True)

    summary_embed.add_field(name=f"{e('calendar')} Ngày tạo", value=utils.format_discord_time(server.created_at, 'D'), inline=True)

    # Số lượng members
    summary_embed.add_field(name=f"{e('members')} Tổng Members", value=f"{server.member_count:,} (Cache)", inline=True)
    summary_embed.add_field(name="🧑‍🤝‍🧑 Users", value=f"{member_count:,}", inline=True)
    summary_embed.add_field(name=f"{e('bot_tag')} Bots", value=f"{bot_count:,}", inline=True)

    # Thông tin Boost và Cài đặt
    summary_embed.add_field(name=f"{e('boost')} Cấp Boost", value=f"Cấp {server.premium_tier}", inline=True)
    summary_embed.add_field(name=f"{e('boost')} Số Boost", value=f"{server.premium_subscription_count}", inline=True)
    summary_embed.add_field(name=f"{e('success')} Xác minh", value=str(server.verification_level).capitalize(), inline=True)
    summary_embed.add_field(name=f"{e('shield')} Lọc Nội dung", value=explicit_filter, inline=True)
    summary_embed.add_field(name=f"{e('lock')} MFA", value=mfa_level, inline=True)
    summary_embed.add_field(name=f"{e('bell')} Thông báo", value=notifications, inline=True)

    # Thống kê kênh
    channel_stats_lines = [
        f"{utils.get_channel_type_emoji(discord.ChannelType.text, bot)} Text: {channel_counts.get(discord.ChannelType.text, 0)}",
        f"{utils.get_channel_type_emoji(discord.ChannelType.voice, bot)} Voice: {channel_counts.get(discord.ChannelType.voice, 0)}",
        f"{utils.get_channel_type_emoji(discord.ChannelType.category, bot)} Category: {channel_counts.get(discord.ChannelType.category, 0)}",
        f"{utils.get_channel_type_emoji(discord.ChannelType.stage_voice, bot)} Stage: {channel_counts.get(discord.ChannelType.stage_voice, 0)}",
        f"{utils.get_channel_type_emoji(discord.ChannelType.forum, bot)} Forum: {channel_counts.get(discord.ChannelType.forum, 0)}",
        f"{utils.get_channel_type_emoji(discord.ChannelType.public_thread, bot)} Thread (đã quét): {processed_threads_count}"
    ]
    summary_embed.add_field(
        name=f"{e('info')} Kênh ({sum(channel_counts.values())}) & Luồng",
        value=" | ".join(channel_stats_lines),
        inline=False # Để full width
    )

    # Số lượng Roles, Emojis, Stickers
    summary_embed.add_field(name=f"{e('role')} Roles", value=f"{all_roles_count:,}", inline=True)
    summary_embed.add_field(name=f"{utils.get_emoji('mention', bot)} Emojis", value=f"{len(server.emojis):,}", inline=True)
    summary_embed.add_field(name=f"{utils.get_emoji('sticker', bot)} Stickers", value=f"{len(server.stickers):,}", inline=True)

    # Các kênh đặc biệt
    summary_embed.add_field(name=f"{e('text_channel')} Kênh Hệ thống", value=sys_channel_mention, inline=True)
    summary_embed.add_field(name=f"{e('rules')} Kênh Luật lệ", value=rules_channel_mention, inline=True)
    summary_embed.add_field(name=f"{e('megaphone')} Kênh Cập nhật", value=public_updates_channel_mention, inline=True)
    summary_embed.add_field(name=f"{e('zzz')} Kênh AFK", value=afk_channel_mention, inline=True)
    summary_embed.add_field(name=f"{e('clock')} AFK Timeout", value=afk_timeout_str, inline=True)
    summary_embed.add_field(name="\u200b", value="\u200b", inline=True) # Field trống để căn chỉnh

    # Trạng thái members
    status_stats = (
        f"{utils.map_status(discord.Status.online, bot)}: {initial_member_status_counts.get('online', 0)}\n"
        f"{utils.map_status(discord.Status.idle, bot)}: {initial_member_status_counts.get('idle', 0)}\n"
        f"{utils.map_status(discord.Status.dnd, bot)}: {initial_member_status_counts.get('dnd', 0)}\n"
        f"{utils.map_status(discord.Status.offline, bot)}: {initial_member_status_counts.get('offline', 0) + initial_member_status_counts.get('invisible', 0)}"
    )
    summary_embed.add_field(name=f"{e('members')} Trạng thái Member (Khi quét)", value=status_stats, inline=False)

    # Tính năng server
    summary_embed.add_field(name=f"{e('star')} Tính năng Server", value=features_str, inline=False)

    # Footer
    footer_text = f"ID Server: {server.id}"
    if ctx:
        footer_text += f" | Yêu cầu bởi: {ctx.author.display_name} ({ctx.author.id})"
    summary_embed.set_footer(text=footer_text)

    return summary_embed


async def create_text_channel_embed(
    detail: Dict[str, Any], # Dữ liệu chi tiết của kênh từ scan_data['channel_details']
    bot: discord.Client
) -> discord.Embed:
    """Tạo embed hiển thị chi tiết của một kênh text hoặc voice đã quét."""
    e = lambda name: utils.get_emoji(name, bot)
    channel_id = detail.get('id', 'N/A')
    channel_name = detail.get('name', 'Không rõ')
    channel_type_str = detail.get("type", "unknown")
    channel_error = detail.get("error")
    processed = detail.get("processed", False)
    channel_msg_count = detail.get('message_count', 0)

    is_voice_channel = channel_type_str == str(discord.ChannelType.voice)
    channel_type_name = "Voice" if is_voice_channel else "Text"
    channel_type_emoji = utils.get_channel_type_emoji(channel_type_str, bot)

    # --- Xử lý trường hợp lỗi nghiêm trọng (không quét được gì) ---
    if channel_error and not processed:
        error_embed = discord.Embed(
            title=f"{e('error')} Kênh {channel_type_name}: #{utils.escape_markdown(channel_name)}",
            description=f"**Lỗi nghiêm trọng khi quét:**\n```\n{utils.escape_markdown(str(channel_error))}\n```",
            color=discord.Color.dark_red()
        )
        error_embed.add_field(name="ID Kênh", value=f"`{channel_id}`")
        # Hiển thị reaction count nếu có (dù lỗi)
        reaction_count = detail.get('reaction_count')
        if reaction_count is not None:
            error_embed.add_field(name=f"{e('reaction')} Biểu cảm (Trước lỗi)", value=f"{reaction_count:,}", inline=True)
        return error_embed

    # --- Tạo Embed cho kênh đã quét (có thể có lỗi phụ) ---
    embed_color = discord.Color.green() if channel_msg_count > 0 else discord.Color.light_grey()
    if channel_error: # Lỗi phụ sau khi đã quét được phần nào
        embed_color = discord.Color.orange()

    # --- Chuẩn bị Description ---
    desc_lines = [
        f"**ID:** `{channel_id}` | {e('category')} **Danh mục:** {utils.escape_markdown(detail.get('category', 'N/A'))}",
        f"**NSFW:** {detail.get('nsfw', 'N/A')}",
    ]
    if not is_voice_channel:
        desc_lines.append(f"**Slowmode:** {detail.get('slowmode', 'N/A')}")
        # Giới hạn độ dài topic hiển thị
        topic_str = utils.escape_markdown(detail.get('topic', 'Không có'))
        if len(topic_str) > 200: topic_str = topic_str[:200] + "..."
        desc_lines.append(f"**Chủ đề:** {topic_str}")

    # Thông tin về luồng (nếu có)
    threads_data = detail.get("threads_data", [])
    if not is_voice_channel:
        scanned_thread_count = len([t for t in threads_data if not t.get("error")])
        scanned_thread_msg_count = sum(t.get("message_count", 0) for t in threads_data if not t.get("error"))
        # Tổng reaction từ các luồng đã quét thành công
        scanned_thread_reaction_count = sum(t.get("reaction_count", 0) for t in threads_data if not t.get("error") and t.get("reaction_count") is not None)
        reaction_thread_str = f" ({e('reaction')} {scanned_thread_reaction_count:,})" if scanned_thread_reaction_count > 0 else ""
        skipped_thread_count = len([t for t in threads_data if t.get("error")])

        thread_count_str = f"{e('thread')} **Luồng đã quét:** {scanned_thread_count} ({scanned_thread_msg_count:,} tin nhắn{reaction_thread_str})"
        if skipped_thread_count > 0:
            thread_count_str += f" ({skipped_thread_count} lỗi/bỏ qua)"
        desc_lines.append(thread_count_str)
    else:
        desc_lines.append(f"{e('thread')} **Luồng:** N/A (Kênh Voice)")

    channel_embed = discord.Embed(
        title=f"{channel_type_emoji} Kênh {channel_type_name}: #{utils.escape_markdown(channel_name)}",
        description="\n".join(line for line in desc_lines if line).strip(), # Bỏ dòng trống
        color=embed_color,
        timestamp=detail.get('created_at') # Hiển thị thời gian tạo kênh
    )

    # --- Thêm Fields ---
    channel_embed.add_field(name=f"{e('calendar')} Ngày tạo", value=utils.format_discord_time(detail.get('created_at')), inline=True)

    msg_field_name = f"{e('stats')} Tin nhắn ({channel_type_name})"
    channel_embed.add_field(name=msg_field_name, value=f"{channel_msg_count:,}", inline=True)

    scan_duration = detail.get('duration', datetime.timedelta(0))
    channel_embed.add_field(name=f"{e('clock')} TG Quét", value=utils.format_timedelta(scan_duration), inline=True)

    channel_react_count = detail.get('reaction_count')
    if channel_react_count is not None:
        react_field_name = f"{e('reaction')} Biểu cảm ({channel_type_name})"
        channel_embed.add_field(name=react_field_name, value=f"{channel_react_count:,}", inline=True)
    else:
        # Thêm field trống để giữ layout 3 cột
        channel_embed.add_field(name="\u200b", value="\u200b", inline=True)

    top_chatter = detail.get('top_chatter', "Không có")
    top_chatter_roles = detail.get('top_chatter_roles', "N/A")
    channel_embed.add_field(name=f"{e('crown')} Top Chatter (Kênh)", value=top_chatter, inline=True)
    # Giới hạn độ dài role của top chatter
    if len(top_chatter_roles) > 1000: top_chatter_roles = top_chatter_roles[:1000] + "..."
    channel_embed.add_field(name=f"{e('role')} Roles Top Chatter", value=top_chatter_roles, inline=True)

    # Log tin nhắn đầu tiên
    first_msgs_log = detail.get('first_messages_log', ["`[N/A]`"])
    first_msgs_log_content = "\n".join(first_msgs_log)
    # Giới hạn độ dài field log
    if len(first_msgs_log_content) > 1000:
        first_msgs_log_content = first_msgs_log_content[:1000] + "\n`[...]` (quá dài)"
    elif not first_msgs_log_content.strip():
        first_msgs_log_content = "`[Không có hoặc lỗi]`"
    channel_embed.add_field(
        name=f"📝 Log ~{FIRST_MESSAGES_LIMIT} Tin nhắn đầu tiên",
        value=first_msgs_log_content,
        inline=False
    )

    # Hiển thị lỗi phụ (nếu có)
    if channel_error and processed:
        error_str = utils.escape_markdown(str(channel_error))
        if len(error_str) > 1000: error_str = error_str[:1000] + "..."
        channel_embed.add_field(
            name=f"{e('warning')} Lưu ý lỗi phụ",
            value=f"```\n{error_str}\n```",
            inline=False
        )

    return channel_embed


async def create_voice_channel_embeds(
    voice_channel_data: List[Dict[str, Any]], # Dữ liệu kênh voice tĩnh
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds hiển thị thông tin cấu hình tĩnh của các kênh Voice/Stage."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    if not voice_channel_data:
        return embeds # Trả về list rỗng nếu không có dữ liệu

    num_vc_embeds = math.ceil(len(voice_channel_data) / VOICE_CHANNELS_PER_EMBED)

    for i in range(num_vc_embeds):
        start_index = i * VOICE_CHANNELS_PER_EMBED
        end_index = start_index + VOICE_CHANNELS_PER_EMBED
        vc_batch = voice_channel_data[start_index:end_index]

        vc_embed = discord.Embed(
            title=f"{e('voice_channel')}{e('stage')} Thông tin Kênh Voice/Stage (Tĩnh - Phần {i + 1}/{num_vc_embeds})",
            description=f"{e('info')} *Thông tin cấu hình kênh. Lịch sử chat (nếu có) được quét riêng.*",
            color=discord.Color.blue()
        )

        vc_list_lines = []
        for vc in vc_batch:
            channel_type_str = vc.get('type', 'unknown')
            type_emoji = utils.get_channel_type_emoji(channel_type_str, bot)
            # Hiển thị giới hạn user
            user_limit_str = str(vc['user_limit']) if isinstance(vc['user_limit'], int) and vc['user_limit'] > 0 else vc.get('user_limit', 'N/A')

            # Dòng chính: Tên và ID
            line1 = f"**{type_emoji} #{utils.escape_markdown(vc['name'])}** (`{vc['id']}`)"
            # Dòng phụ 1: Category, Limit, Bitrate
            line2 = f"  └ {e('category')} {utils.escape_markdown(vc['category'])} | {e('members')} Limit: {user_limit_str} | {e('stats')} Bitrate: {vc['bitrate']}"
            # Dòng phụ 2: Ngày tạo
            line3 = f"  └ {e('calendar')} Tạo: {utils.format_discord_time(vc.get('created_at'), 'd')}"

            vc_list_lines.extend([line1, line2, line3])

        # Thêm vào description, giới hạn độ dài
        current_desc = vc_embed.description + "\n\n"
        new_content = "\n".join(vc_list_lines) if vc_list_lines else "Không có dữ liệu."

        if len(current_desc) + len(new_content) > 4000:
             remaining_space = 4000 - len(current_desc) - 20 # Trừ đi khoảng trống cho '...'
             vc_embed.description = current_desc + new_content[:remaining_space] + "\n... (quá dài)"
        else:
             vc_embed.description = current_desc + new_content

        embeds.append(vc_embed)

    return embeds


async def create_booster_embeds(
    boosters: List[discord.Member],
    bot: discord.Client,
    scan_end_time: datetime.datetime # Thời điểm quét xong để tính thời gian boost
) -> List[discord.Embed]:
    """Tạo embeds danh sách những người đang boost server."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    boost_emoji = e('boost_animated') or e('boost') # Ưu tiên emoji động
    if not boosters:
        return embeds

    # Sắp xếp theo thời gian boost (lâu nhất trước)
    boosters.sort(key=lambda m: m.premium_since or datetime.datetime.now(datetime.timezone.utc))
    num_booster_embeds = math.ceil(len(boosters) / BOOSTERS_PER_EMBED)

    for i in range(num_booster_embeds):
        start_index = i * BOOSTERS_PER_EMBED
        end_index = start_index + BOOSTERS_PER_EMBED
        booster_batch = boosters[start_index:end_index]

        booster_embed = discord.Embed(
            title=f"{boost_emoji} Server Boosters (Phần {i + 1}/{num_booster_embeds})",
            color=discord.Color(0xf47fff) # Màu hồng boost
        )

        booster_list_lines = []
        for member in booster_batch:
            boost_duration_str = "N/A"
            if member.premium_since:
                try:
                    # Đảm bảo cả hai thời điểm đều có timezone (UTC)
                    scan_end_time_aware = scan_end_time if scan_end_time.tzinfo else scan_end_time.replace(tzinfo=datetime.timezone.utc)
                    premium_since_aware = member.premium_since if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc)
                    if scan_end_time_aware >= premium_since_aware:
                        boost_duration = scan_end_time_aware - premium_since_aware
                        boost_duration_str = utils.format_timedelta(boost_duration)
                    else:
                        boost_duration_str = "Lỗi TG (Tương lai?)"
                except Exception as td_err:
                    log.warning(f"Lỗi tính thời gian boost cho {member.id}: {td_err}")
                    boost_duration_str = "Lỗi TG"

            user_display = f" (`{utils.escape_markdown(member.display_name)}`)"
            line1 = f"{member.mention}{user_display}"
            line2 = f" └ {e('calendar')} Boost từ: {utils.format_discord_time(member.premium_since, 'D')} ({boost_duration_str})"
            booster_list_lines.extend([line1, line2])

        # Thêm vào description, giới hạn độ dài
        new_content = "\n".join(booster_list_lines) if booster_list_lines else "Không có dữ liệu."
        if len(new_content) > 4000:
            booster_embed.description = new_content[:4000] + "\n... (quá dài)"
        else:
             booster_embed.description = new_content

        embeds.append(booster_embed)
    return embeds


async def create_role_embeds(
    all_roles: List[discord.Role], # Danh sách roles đã sắp xếp
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds danh sách các role của server."""
    embeds = []
    e = lambda name: utils.get_emoji(name, bot)
    if not all_roles:
        return embeds

    num_role_embeds = math.ceil(len(all_roles) / ROLES_PER_EMBED)

    for i in range(num_role_embeds):
        start_index = i * ROLES_PER_EMBED
        end_index = start_index + ROLES_PER_EMBED
        role_batch = all_roles[start_index:end_index]

        role_embed = discord.Embed(
            title=f"{e('role')} Roles (Phần {i + 1}/{num_role_embeds})",
            description="*Sắp xếp theo vị trí từ cao xuống thấp.*",
            color=discord.Color.gold()
        )

        role_list_lines = []
        for role in role_batch:
            color_str = f" (`{role.color}`)" if str(role.color) != "#000000" else "" # Chỉ hiện màu nếu khác màu đen mặc định
            member_count = len(role.members) # Lấy số member từ cache
            perm_value = role.permissions.value
            # Hiển thị giá trị permissions nếu khác 0
            perm_str = f" | Perms: `{perm_value}`" if perm_value > 0 else ""
            # Các thông tin khác (có thể thêm nếu cần)
            hoist_str = " [Hoisted]" if role.hoist else ""
            mention_str = " [Mentionable]" if role.mentionable else ""

            role_line = f"{role.mention}{color_str} - ID: `{role.id}` ({e('members')} {member_count}){perm_str}{hoist_str}{mention_str}"
            role_list_lines.append(role_line)

        # Thêm vào description, giới hạn độ dài
        new_content = "\n".join(role_list_lines) if role_list_lines else "Không có dữ liệu."
        current_desc = role_embed.description + "\n\n"
        if len(current_desc) + len(new_content) > 4000:
            remaining = 4000 - len(current_desc) - 20
            role_embed.description = current_desc + new_content[:remaining] + "\n... (quá dài)"
        else:
            role_embed.description = current_desc + new_content

        embeds.append(role_embed)
    return embeds

# --- END OF FILE reporting/embeds_guild.py ---