# --- START OF FILE reporting.py ---
import discord
import datetime
import math
import csv
import io
import json
import traceback
import re # Thêm re
from collections import Counter, defaultdict # Thêm defaultdict
import collections
from typing import List, Dict, Any, Optional, Union
import asyncio
import logging

# Import helpers from utils.py
from utils import (
    format_timedelta, format_discord_time, map_status, get_emoji,
    get_channel_type_emoji, sanitize_for_csv, fetch_user_data,
    parse_slowmode, parse_bitrate
)

log = logging.getLogger(__name__)

# --- Constants ---
USERS_PER_ACTIVITY_EMBED = 15
BOOSTERS_PER_EMBED = 20
VOICE_CHANNELS_PER_EMBED = 20
ROLES_PER_EMBED = 25
INVITES_PER_EMBED = 15
WEBHOOKS_PER_EMBED = 15
INTEGRATIONS_PER_EMBED = 15
# <<< MODIFIED: Increased Audit Log embed limit slightly >>>
AUDIT_LOG_ENTRIES_PER_EMBED = 12 # Increased slightly
PERMISSION_AUDIT_ITEMS_PER_EMBED = 10
KEYWORD_RANKING_LIMIT = 10
# <<< MODIFIED: Reduced Top Active Users limit for the summary embed >>>
TOP_ACTIVE_USERS_LIMIT = 20 # Reduced for the summary embed
TOP_OLDEST_MEMBERS_LIMIT = 30
ROLES_STATS_PER_EMBED = 15
FIRST_MESSAGES_LIMIT = 10
FIRST_MESSAGES_CONTENT_PREVIEW = 100
REACTIONS_PER_EMBED = 20
TOP_REACTORS_LIMIT = 15 # No longer used directly?
TOP_INVITERS_LIMIT = 15
USER_ROLE_STATS_PER_EMBED = 10
TOP_LINK_USERS_LIMIT = 30
TOP_IMAGE_USERS_LIMIT = 30
TOP_EMOJI_USERS_LIMIT = 30
TOP_STICKER_USERS_LIMIT = 30
TOP_ROLES_GRANTED_LIMIT = 20

# --- Embed Creation Functions ---

# --- create_summary_embed ---
# (No significant changes needed here based on request)
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
    initial_member_status_counts: Counter,
    channel_counts: Counter,
    all_roles_count: int,
    start_time: datetime.datetime,
    ctx: Optional[discord.ext.commands.Context] = None,
    overall_total_reaction_count: Optional[int] = None
) -> discord.Embed:
    """Tạo embed tóm tắt server chính."""
    e = lambda name: get_emoji(name, bot)

    explicit_filter = str(server.explicit_content_filter).replace('_', ' ').title()
    mfa_level = "Yêu cầu (Cho Mod)" if server.mfa_level >= 1 else "Không yêu cầu"
    notifications = "Chỉ @mention" if server.default_notifications == discord.NotificationLevel.only_mentions else "Tất cả tin nhắn"
    sys_channel = server.system_channel.mention if server.system_channel else "Không có"
    rules_channel = server.rules_channel.mention if server.rules_channel else "Không có"
    public_updates_channel = server.public_updates_channel.mention if server.public_updates_channel else "Không có"
    afk_channel = server.afk_channel.mention if server.afk_channel else "Không có"
    afk_timeout = f"{server.afk_timeout // 60} phút" if server.afk_timeout else "N/A"

    features = ", ".join(server.features) if server.features else "Không có"
    if len(features) > 800: features = features[:800] + "... (nhiều)"

    member_count = len([m for m in server.members if not m.bot])
    bot_count = len([m for m in server.members if m.bot])

    reaction_line = ""
    if overall_total_reaction_count is not None:
        reaction_line = f"\n{e('reaction')} Tổng **{overall_total_reaction_count:,}** biểu cảm."

    # <<< MODIFIED: Update description slightly for clarity >>>
    summary_embed = discord.Embed(
        title=f"{e('stats')} Báo cáo Quét Sâu Server: {server.name}",
        description=(
            f"Quét **{processed_channels_count:,}** kênh text/voice ({skipped_channels_count} lỗi/bỏ qua).\n" # Clarified text/voice
            f"Quét **{processed_threads_count:,}** luồng ({skipped_threads_count} lỗi/bỏ qua).\n"
            f"Tổng **{overall_total_message_count:,}** tin nhắn (kênh + luồng)."
            f"{reaction_line}\n"
            f"**{user_activity_count:,}** users có hoạt động.\n"
            f"{e('clock')} **Tổng thời gian quét:** {format_timedelta(overall_duration, high_precision=True)}"
        ),
        color=discord.Color.purple(),
        timestamp=start_time + overall_duration
    )
    if server.icon: summary_embed.set_thumbnail(url=server.icon.url)

    summary_embed.add_field(name=f"{e('id_card')} Server ID", value=f"`{server.id}`", inline=True)
    summary_embed.add_field(name=f"{e('crown')} Chủ sở hữu", value=f"{server.owner.mention if server.owner else 'Không rõ'}", inline=True)
    summary_embed.add_field(name=f"{e('calendar')} Ngày tạo", value=format_discord_time(server.created_at, 'D'), inline=True)

    summary_embed.add_field(name=f"{e('members')} Tổng Members", value=f"{server.member_count:,} (Cache)", inline=True)
    summary_embed.add_field(name="🧑‍🤝‍🧑 Users (Thật)", value=f"{member_count:,}", inline=True)
    summary_embed.add_field(name=f"{e('bot_tag')} Bots", value=f"{bot_count:,}", inline=True)

    summary_embed.add_field(name=f"{e('boost')} Cấp Boost", value=f"Cấp {server.premium_tier}", inline=True)
    summary_embed.add_field(name=f"{e('boost')} Số Boost", value=f"{server.premium_subscription_count}", inline=True)
    summary_embed.add_field(name=f"{e('success')} Xác minh", value=str(server.verification_level).capitalize(), inline=True)

    summary_embed.add_field(name=f"{e('shield')} Lọc Nội dung", value=explicit_filter, inline=True)
    summary_embed.add_field(name=f"{e('lock')} MFA", value=mfa_level, inline=True)
    summary_embed.add_field(name=f"{e('bell')} Thông báo", value=notifications, inline=True)

    # <<< MODIFIED: Update channel counts in summary >>>
    channel_stats = (
        f"{e('text_channel')} Text: {channel_counts.get(discord.ChannelType.text, 0)} | "
        f"{e('voice_channel')} Voice: {channel_counts.get(discord.ChannelType.voice, 0)} | "
        f"{e('category')} Category: {channel_counts.get(discord.ChannelType.category, 0)}\n"
        f"{e('stage')} Stage: {channel_counts.get(discord.ChannelType.stage_voice, 0)} | "
        f"{e('forum')} Forum: {channel_counts.get(discord.ChannelType.forum, 0)} | "
        f"{e('thread')} Thread: {processed_threads_count}"
    )
    summary_embed.add_field(name=f"{e('info')} Kênh ({sum(channel_counts.values())})", value=channel_stats, inline=False)

    summary_embed.add_field(name=f"{e('role')} Roles", value=f"{all_roles_count:,} (Ko tính @everyone)", inline=True)
    summary_embed.add_field(name=f"{get_emoji('mention', bot)} Emojis", value=f"{len(server.emojis):,}", inline=True)
    summary_embed.add_field(name=f"{get_emoji('sticker', bot)} Stickers", value=f"{len(server.stickers):,}", inline=True)

    summary_embed.add_field(name=f"{e('text_channel')} Kênh Hệ thống", value=sys_channel, inline=True)
    summary_embed.add_field(name=f"{e('rules')} Kênh Luật lệ", value=rules_channel, inline=True)
    summary_embed.add_field(name=f"{e('megaphone')} Kênh Cập nhật", value=public_updates_channel, inline=True)

    summary_embed.add_field(name=f"{e('zzz')} Kênh AFK", value=afk_channel, inline=True)
    summary_embed.add_field(name=f"{e('clock')} AFK Timeout", value=afk_timeout, inline=True)
    summary_embed.add_field(name="\u200b", value="\u200b", inline=True) # Khoảng trống

    status_stats = (
        f"{map_status(discord.Status.online, bot)}: {initial_member_status_counts.get('online', 0)}\n"
        f"{map_status(discord.Status.idle, bot)}: {initial_member_status_counts.get('idle', 0)}\n"
        f"{map_status(discord.Status.dnd, bot)}: {initial_member_status_counts.get('dnd', 0)}\n"
        f"{map_status(discord.Status.offline, bot)}: {initial_member_status_counts.get('offline', 0) + initial_member_status_counts.get('invisible', 0)}"
    )
    summary_embed.add_field(name=f"{e('members')} Trạng thái Member (Khi quét)", value=status_stats, inline=False)

    summary_embed.add_field(name=f"{e('star')} Tính năng Server", value=features, inline=False)
    if ctx:
         summary_embed.set_footer(text=f"Yêu cầu bởi: {ctx.author.display_name} | ID: {ctx.author.id}")
    else:
         summary_embed.set_footer(text="Báo cáo quét sâu tự động")

    return summary_embed


# --- create_text_channel_embed ---
# (This function should now correctly handle voice channel details passed in `detail`)
async def create_text_channel_embed(detail: Dict[str, Any], bot: discord.Client) -> discord.Embed:
    """Tạo embed cho chi tiết của một kênh text hoặc voice đã quét."""
    e = lambda name: get_emoji(name, bot)
    is_incremental_scan = detail.get("scan_type_note", "") == "(Quét tăng tiến)" # Not used currently
    channel_msg_count = detail.get('message_count', 0)
    # <<< MODIFIED: Determine type based on 'type' field in detail >>>
    channel_type_str = detail.get("type", "unknown")
    is_voice_channel = channel_type_str == str(discord.ChannelType.voice)
    channel_type_name = "Voice" if is_voice_channel else "Text"
    channel_type_emoji = e('voice_channel') if is_voice_channel else e('text_channel')


    if detail.get("error") and not detail.get("processed"):
         channel_embed = discord.Embed(
             # <<< MODIFIED: Use dynamic type name >>>
             title=f"{e('error')} Kênh {channel_type_name}: #{detail.get('name', 'Không rõ')}",
             description=f"**Lỗi nghiêm trọng khi quét:**\n{detail['error']}",
             color=discord.Color.dark_red()
         )
         channel_embed.add_field(name="ID Kênh", value=f"`{detail.get('id', 'N/A')}`")
         if detail.get('reaction_count') is not None:
             channel_embed.add_field(name=f"{e('reaction')} Biểu cảm (Trước lỗi)", value=f"{detail.get('reaction_count', 0):,}", inline=True)

    elif "name" in detail:
         embed_color = discord.Color.green() if channel_msg_count > 0 else discord.Color.light_grey()
         if detail.get("error"): embed_color = discord.Color.orange()

         # Threads only apply to text channels
         threads_data = detail.get("threads_data", []) if not is_voice_channel else None # Explicitly None for voice
         thread_count_str = ""
         if threads_data is not None: # Check if thread data exists (i.e., it was a text channel)
            scanned_thread_count = len([t for t in threads_data if t.get("error") is None])
            scanned_thread_msg_count = sum(t.get("message_count", 0) for t in threads_data if t.get("error") is None)
            scanned_thread_reaction_count = sum(t.get("reaction_count", 0) for t in threads_data if t.get("error") is None)
            reaction_thread_str = f" ({e('reaction')} {scanned_thread_reaction_count:,})" if scanned_thread_reaction_count > 0 else ""
            skipped_thread_count = len([t for t in threads_data if t.get("error")])
            thread_count_str = f"\n**{e('thread')} Luồng đã quét:** {scanned_thread_count} ({scanned_thread_msg_count:,} tin nhắn{reaction_thread_str})"
            if skipped_thread_count > 0: thread_count_str += f" ({skipped_thread_count} lỗi/bỏ qua)"
         elif is_voice_channel:
             thread_count_str = f"\n**{e('thread')} Luồng:** N/A (Kênh Voice)"


         scan_note = "" # Removed incremental logic note
         clarification_note = "" # Removed incremental logic note

         desc_lines = [
            f"**ID:** `{detail['id']}` | {e('category')} **Danh mục:** {detail.get('category', 'N/A')}",
            f"**NSFW:** {detail.get('nsfw', 'N/A')}", # Slowmode/Topic handled below
         ]
         # Add relevant fields based on type
         if is_voice_channel:
             # Voice channels don't have topics or slowmode in the same way
             pass # No extra lines needed here based on current detail structure
         else: # Text Channel
             desc_lines.append(f"**Slowmode:** {detail.get('slowmode', 'N/A')}")
             desc_lines.append(f"**Chủ đề:** {detail.get('topic', 'Không có')}")

         # Add thread info (calculated above)
         desc_lines.append(thread_count_str)

         # Add notes if any
         if scan_note: desc_lines.append(scan_note)
         if clarification_note: desc_lines.append(clarification_note)

         channel_embed = discord.Embed(
             # <<< MODIFIED: Use dynamic type emoji and name >>>
             title=f"{channel_type_emoji} Kênh {channel_type_name}: #{detail['name']}",
             description="\n".join(line for line in desc_lines if line).strip(), # Filter out empty lines
             color=embed_color,
             timestamp=detail.get('created_at')
         )


         channel_embed.add_field(name=f"{e('calendar')} Ngày tạo", value=format_discord_time(detail.get('created_at')), inline=True)
         # <<< MODIFIED: Use dynamic type name in field >>>
         msg_field_name = f"{e('stats')} Tin nhắn ({channel_type_name})"
         channel_embed.add_field(name=msg_field_name, value=f"{channel_msg_count:,}", inline=True)
         channel_embed.add_field(name=f"{e('clock')} TG Quét (Kênh)", value=format_timedelta(detail.get('duration', datetime.timedelta(0))), inline=True)
         channel_react_count = detail.get('reaction_count')
         if channel_react_count is not None:
             # <<< MODIFIED: Use dynamic type name in field >>>
             react_field_name = f"{e('reaction')} Biểu cảm ({channel_type_name})"
             channel_embed.add_field(name=react_field_name, value=f"{channel_react_count:,}", inline=True)

         top_chatter = detail.get('top_chatter', "Không có (hoặc chỉ bot)")
         top_chatter_roles = detail.get('top_chatter_roles', "N/A")
         if len(top_chatter_roles) > 200: top_chatter_roles = top_chatter_roles[:200] + "..."
         chatter_field_name = f"{e('crown')} Top Chatter (Kênh)" # Simplified name
         channel_embed.add_field(name=chatter_field_name, value=top_chatter, inline=True)
         channel_embed.add_field(name=f"{e('role')} Roles Top Chatter", value=top_chatter_roles, inline=True)
         if channel_react_count is None:
              channel_embed.add_field(name="\u200b", value="\u200b", inline=True) # Keep spacer if no reaction count

         first_msgs_log = detail.get('first_messages_log', ["`[N/A]`"])
         first_msgs_log_content = "\n".join(first_msgs_log)
         if len(first_msgs_log_content) > 1000:
             first_msgs_log_content = first_msgs_log_content[:1000] + "\n`[...]` (quá dài)"
         elif not first_msgs_log_content:
              first_msgs_log_content = "`[Không có hoặc lỗi]`"
         channel_embed.add_field(name=f"📝 Log ~{FIRST_MESSAGES_LIMIT} Tin nhắn đầu tiên (Lịch sử)", value=first_msgs_log_content, inline=False)

         if detail.get("error"):
             channel_embed.add_field(name=f"{e('error')} Lưu ý lỗi phụ", value=detail["error"], inline=False)

    else:
        # <<< MODIFIED: Use dynamic type name >>>
        channel_embed = discord.Embed(title=f"{e('error')} Kênh {channel_type_name}: #{detail.get('name', 'Không rõ')}", description="Dữ liệu kênh không đầy đủ hoặc lỗi không xác định.", color=discord.Color.greyple())
        if detail.get("id"): channel_embed.add_field(name="ID Kênh", value=f"`{detail.get('id')}`")
        if detail.get('reaction_count') is not None:
             channel_embed.add_field(name=f"{e('reaction')} Biểu cảm (Trước lỗi)", value=f"{detail.get('reaction_count', 0):,}", inline=True)


    return channel_embed

# --- create_voice_channel_embeds ---
# (Now reports STATIC info - scanning handled by create_text_channel_embed)
async def create_voice_channel_embeds(voice_channel_data: List[Dict[str, Any]], bot: discord.Client) -> List[discord.Embed]:
    """Tạo embeds cho chi tiết kênh voice/stage TĨNH."""
    embeds = []
    e = lambda name: get_emoji(name, bot)
    if not voice_channel_data:
        return embeds

    num_vc_embeds = math.ceil(len(voice_channel_data) / VOICE_CHANNELS_PER_EMBED)

    for i in range(num_vc_embeds):
        start_index = i * VOICE_CHANNELS_PER_EMBED
        end_index = start_index + VOICE_CHANNELS_PER_EMBED
        vc_batch = voice_channel_data[start_index:end_index]

        vc_embed = discord.Embed(
            # <<< MODIFIED: Title clarifies static info >>>
            title=f"{e('voice_channel')}{e('stage')} Thông tin Kênh Voice/Stage (Tĩnh - Phần {i + 1}/{num_vc_embeds})",
            description=f"{e('info')} *Thông tin cấu hình kênh. Lịch sử chat được quét (nếu có quyền) và báo cáo trong phần Kênh Text/Voice.*",
            color=discord.Color.blue()
        )
        vc_list_str = ""
        for vc in vc_batch:
             # <<< MODIFIED: Use type info from data >>>
             channel_type = vc.get('type', 'unknown') # Get type stored during static collection
             type_emoji = get_channel_type_emoji(channel_type, bot) # Get emoji from type string

             vc_list_str += (f"**{type_emoji} #{vc['name']}** (`{vc['id']}`)\n"
                             f" └ {e('category')} {vc['category']} | {e('members')} {vc['user_limit']} | {e('stats')} {vc['bitrate']}\n"
                             f"   └ {e('calendar')} Tạo: {format_discord_time(vc.get('created_at'), 'd')}\n")

        vc_embed.description += "\n" + (vc_list_str if vc_list_str else "Không có dữ liệu.")
        if len(vc_embed.description) > 4000:
            vc_embed.description = vc_embed.description[:4000] + "\n... (quá dài)"
        embeds.append(vc_embed)

    return embeds

# --- create_booster_embeds ---
# (No significant changes needed)
async def create_booster_embeds(boosters: List[discord.Member], bot: discord.Client, scan_end_time: datetime.datetime) -> List[discord.Embed]:
    """Tạo embeds cho người boost server."""
    embeds = []
    e = lambda name: get_emoji(name, bot)
    if not boosters:
        return embeds

    boosters.sort(key=lambda m: m.premium_since or scan_end_time)
    num_booster_embeds = math.ceil(len(boosters) / BOOSTERS_PER_EMBED)

    for i in range(num_booster_embeds):
         start_index = i * BOOSTERS_PER_EMBED
         end_index = start_index + BOOSTERS_PER_EMBED
         booster_batch = boosters[start_index:end_index]

         booster_embed = discord.Embed(
             title=f"{e('boost_animated')}{e('boost')} Server Boosters (Phần {i + 1}/{num_booster_embeds})",
             color=discord.Color(0xf47fff)
         )
         booster_list_str = ""
         for member in booster_batch:
             boost_duration_str = "N/A"
             if member.premium_since:
                 try:
                     # Ensure both datetimes are timezone-aware for correct subtraction
                     scan_end_time_aware = scan_end_time if scan_end_time.tzinfo else scan_end_time.replace(tzinfo=datetime.timezone.utc)
                     premium_since_aware = member.premium_since if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc)
                     if scan_end_time_aware >= premium_since_aware:
                         boost_duration = scan_end_time_aware - premium_since_aware
                         boost_duration_str = format_timedelta(boost_duration) # Use standard precision
                     else:
                         boost_duration_str = "Lỗi TG (Tương lai?)"
                 except Exception as td_err:
                      log.warning(f"Lỗi tính thời gian boost cho {member.id}: {td_err}")
                      boost_duration_str = "Lỗi TG"

             booster_list_str += (f"{member.mention} (`{member.display_name}`)\n"
                                f" └ {e('calendar')} Boost từ: {format_discord_time(member.premium_since, 'D')} ({boost_duration_str})\n")

         booster_embed.description = booster_list_str if booster_list_str else "Không có dữ liệu."
         if len(booster_embed.description) > 4000:
             booster_embed.description = booster_embed.description[:4000] + "\n... (quá dài)"
         embeds.append(booster_embed)
    return embeds

# --- create_role_embeds ---
# (No significant changes needed)
async def create_role_embeds(all_roles: List[discord.Role], bot: discord.Client) -> List[discord.Embed]:
    """Tạo embeds cho các role của server."""
    embeds = []
    e = lambda name: get_emoji(name, bot)
    if not all_roles:
        return embeds

    num_role_embeds = math.ceil(len(all_roles) / ROLES_PER_EMBED)

    for i in range(num_role_embeds):
        start_index = i * ROLES_PER_EMBED
        end_index = start_index + ROLES_PER_EMBED
        role_batch = all_roles[start_index:end_index]

        role_embed = discord.Embed(
            title=f"{e('role')} Roles (Phần {i + 1}/{num_role_embeds})",
            color=discord.Color.gold()
        )
        role_list_str = ""
        for role in role_batch:
            color_str = f" (`{role.color}`)" if str(role.color) != "#000000" else ""
            member_count = len(role.members) # Lấy từ cache
            role_list_str += f"{role.mention}{color_str} - `{role.id}` ({e('members')} {member_count} members)\n"

        role_embed.description = role_list_str if role_list_str else "Không có dữ liệu."
        if len(role_embed.description) > 4000:
            role_embed.description = role_embed.description[:4000] + "\n... (quá dài)"
        embeds.append(role_embed)
    return embeds

# --- create_user_activity_embeds ---
# (This one shows more detail, keep it as is for now)
async def create_user_activity_embeds(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: discord.Client,
    min_message_count: int,
    scan_start_time: datetime.datetime,
) -> List[discord.Embed]:
    """Tạo embeds cho hoạt động user, bao gồm roles và lọc bot."""
    embeds = []
    e = lambda name: get_emoji(name, bot)

    filtered_user_activity = {
        uid: data for uid, data in user_activity.items()
        if not data.get('is_bot', False) and data.get('message_count', 0) >= min_message_count
    }

    if not filtered_user_activity:
        return embeds

    sorted_users = sorted(
        filtered_user_activity.items(),
        key=lambda item: item[1].get('last_seen', scan_start_time.replace(tzinfo=datetime.timezone.utc)),
        reverse=True
    )

    total_users_to_report = len(sorted_users)
    num_activity_embeds = math.ceil(total_users_to_report / USERS_PER_ACTIVITY_EMBED)

    for i in range(num_activity_embeds):
        start_index = i * USERS_PER_ACTIVITY_EMBED
        end_index = start_index + USERS_PER_ACTIVITY_EMBED
        user_batch = sorted_users[start_index:end_index]

        activity_embed = discord.Embed(
             title=f"{e('user_activity')} Hoạt động User (Phần {i + 1}/{num_activity_embeds})",
             description=f"*Chỉ hiển thị user có >= {min_message_count} tin nhắn (kênh + luồng). Đã lọc bot. Sắp xếp theo hoạt động gần nhất.*",
             color=discord.Color.teal()
        )
        description_lines = []
        user_ids_to_fetch = [user_id for user_id, data in user_batch]

        log.info(f"Đang fetch {len(user_ids_to_fetch)} users đồng thời cho embed hoạt động {i+1}...")
        fetch_tasks = [fetch_user_data(guild, user_id, bot_ref=bot) for user_id in user_ids_to_fetch]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        fetched_users: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
        for idx, result in enumerate(results):
            user_id = user_ids_to_fetch[idx]
            if isinstance(result, (discord.User, discord.Member)):
                fetched_users[user_id] = result
            else:
                fetched_users[user_id] = None
                if isinstance(result, Exception): # Log fetch errors
                    log.warning(f"Lỗi fetch user {user_id} cho activity embed: {result}")
        log.info("Hoàn thành fetch user đồng thời.")

        processed_in_batch = 0
        for user_id, data in user_batch:
             user_obj = fetched_users.get(user_id)
             user_display = f"{e('offline')} ID: `{user_id}` (Không tìm thấy/Rời server)"
             status_display = map_status(None, bot)
             roles_str = "N/A"

             if isinstance(user_obj, discord.Member):
                 user_display = f"{user_obj.mention} (`{user_obj.display_name}`)"
                 status_display = map_status(user_obj.status, bot)
                 member_roles = [r.mention for r in user_obj.roles if not r.is_default()]
                 roles_str = ", ".join(member_roles) if member_roles else "Không có role"
                 if len(roles_str) > 150: roles_str = roles_str[:150] + "..."
             elif isinstance(user_obj, discord.User):
                 user_display = f"{e('offline')} {user_obj.mention} (`{user_obj.display_name}`) (Không trong server)"
                 status_display = map_status(None, bot)
                 roles_str = "N/A (Không trong server)"

             first_seen_ts = data.get('first_seen')
             last_seen_ts = data.get('last_seen')
             msg_count = data.get('message_count', 0)

             activity_span = "N/A"
             if first_seen_ts and last_seen_ts and last_seen_ts >= first_seen_ts:
                  try:
                      # Ensure timezone aware for correct calculation
                      first_aware = first_seen_ts.astimezone(datetime.timezone.utc) if first_seen_ts.tzinfo else first_seen_ts.replace(tzinfo=datetime.timezone.utc)
                      last_aware = last_seen_ts.astimezone(datetime.timezone.utc) if last_seen_ts.tzinfo else last_seen_ts.replace(tzinfo=datetime.timezone.utc)
                      if last_aware >= first_aware:
                          activity_span = format_timedelta(last_aware - first_aware) # Standard precision
                      else: activity_span = f"{e('error')} Lỗi TG (Span < 0)"
                  except Exception as ts_err:
                       log.warning(f"Lỗi tính khoảng thời gian hoạt động cho user {user_id}: {ts_err}")
                       activity_span = f"{e('error')} Lỗi TG"
             elif first_seen_ts and last_seen_ts:
                 activity_span = f"{e('error')} Lỗi TG (First > Last)"

             description_lines.append(
                 f"**{user_display}** ({status_display})\n"
                 f"  ├ {e('stats')} Tin nhắn: **{msg_count:,}**\n"
                 f"  ├ {e('calendar')} Đầu tiên: {format_discord_time(first_seen_ts, 'R')} ({format_discord_time(first_seen_ts, 'd')})\n"
                 f"  ├ {e('calendar')} Cuối cùng: {format_discord_time(last_seen_ts, 'R')} ({format_discord_time(last_seen_ts, 'd')})\n"
                 f"  ├ {e('role')} Roles: {roles_str}\n"
                 f"  └ {e('clock')} Khoảng TG HĐ: **{activity_span}**"
             )
             processed_in_batch += 1

        activity_embed.description += "\n\n" + "\n".join(description_lines)
        if not description_lines: activity_embed.description += "\nKhông có dữ liệu người dùng cho phần này."
        if len(activity_embed.description) > 4000:
            activity_embed.description = activity_embed.description[:4000] + "\n... (quá dài)"
        embeds.append(activity_embed)

    return embeds

# --- create_top_active_users_embed ---
# <<< MODIFIED: Reformat to be more concise, like the log example >>>
async def create_top_active_users_embed(
    user_activity: Dict[int, Dict[str, Any]],
    guild: discord.Guild,
    bot: discord.Client,
    user_role_changes: Dict[int, Dict[str, Dict[str, int]]] # Keep param even if unused here
) -> Optional[discord.Embed]:
    """Tạo embed cho top N user hoạt động nhiều nhất (không phải admin/bot) dựa trên số tin nhắn."""
    e = lambda name: get_emoji(name, bot)

    admin_ids = {m.id for m in guild.members if m.guild_permissions.administrator}

    non_admin_bot_users = {
        uid: data for uid, data in user_activity.items()
        if uid not in admin_ids and not data.get('is_bot', False) and data.get('message_count', 0) > 0
    }

    if not non_admin_bot_users:
        return None

    sorted_active_users = sorted(
        non_admin_bot_users.items(),
        key=lambda item: item[1]['message_count'],
        reverse=True
    )

    # Use the constant defined earlier
    top_users_batch = sorted_active_users[:TOP_ACTIVE_USERS_LIMIT]

    if not top_users_batch:
         return None

    top_embed = discord.Embed(
        title=f"{e('award')} Top {len(top_users_batch)} User Hoạt Động Nhất (Theo Tin Nhắn)",
        description=f"*Dựa trên số lượng tin nhắn đã quét (kênh + luồng). Bot và Admin đã được loại trừ.*",
        color=discord.Color.orange()
    )

    description_lines = []
    user_ids_to_fetch = [user_id for user_id, data in top_users_batch]

    # Fetch user data efficiently
    fetch_tasks = [fetch_user_data(guild, user_id, bot_ref=bot) for user_id in user_ids_to_fetch]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    fetched_users: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    for idx, result in enumerate(results):
        user_id = user_ids_to_fetch[idx]
        if isinstance(result, (discord.User, discord.Member)):
            fetched_users[user_id] = result
        else:
            fetched_users[user_id] = None
            if isinstance(result, Exception): # Log fetch errors
                log.warning(f"Lỗi fetch user {user_id} cho top active embed: {result}")

    rank = 1
    for user_id, data in top_users_batch:
        user_obj = fetched_users.get(user_id)
        user_display = f"`{user_id}` (Không tìm thấy/Rời server)"
        if isinstance(user_obj, discord.Member):
             user_display = f"{user_obj.mention} (`{user_obj.display_name}`)"
        elif isinstance(user_obj, discord.User):
             user_display = f"{user_obj.mention} (`{user_obj.display_name}`) (Không trong server)"

        msg_count = data['message_count']
        last_seen_ts = data.get('last_seen')
        last_seen_str = f"(Last: {format_discord_time(last_seen_ts, 'R')})" if last_seen_ts else ""

        # <<< MODIFIED: Single concise line per user >>>
        description_lines.append(
            f"**`#{rank:02d}`**. {user_display} - **{msg_count:,}** tin nhắn {last_seen_str}"
        )
        rank += 1

    top_embed.description += "\n" + "\n".join(description_lines) # Add newline before list
    if len(top_embed.description) > 4000:
            top_embed.description = top_embed.description[:4000] + "\n... (quá dài)"

    return top_embed

# --- create_top_oldest_members_embed ---
# (No significant changes needed)
async def create_top_oldest_members_embed(
    oldest_members_data: List[Dict[str, Any]],
    bot: discord.Client,
    limit: int = TOP_OLDEST_MEMBERS_LIMIT
) -> Optional[discord.Embed]:
    """Tạo embed cho top thành viên tham gia server lâu nhất (đã lọc bot)."""
    e = lambda name: get_emoji(name, bot)
    if not oldest_members_data:
        return None

    embed = discord.Embed(
        title=f"{e('award')}{e('calendar')} Top {len(oldest_members_data)} Thành viên Lâu Năm Nhất",
        description="*Dựa trên ngày tham gia server (`joined_at`). Đã lọc bot.*",
        color=discord.Color.dark_green()
    )

    desc_lines = []
    rank = 1
    now = discord.utils.utcnow()
    for data in oldest_members_data[:limit]: # Giới hạn hiển thị
        joined_at = data.get('joined_at')
        time_in_server_str = "N/A"
        if isinstance(joined_at, datetime.datetime):
             try:
                 join_aware = joined_at.astimezone(datetime.timezone.utc) if joined_at.tzinfo else joined_at.replace(tzinfo=datetime.timezone.utc)
                 if now >= join_aware:
                     time_in_server = now - join_aware
                     time_in_server_str = format_timedelta(time_in_server) # Use standard precision
                 else: time_in_server_str = "Lỗi TG (Tương lai?)"
             except: time_in_server_str = "Lỗi TG"

        desc_lines.append(
            f"**`#{rank:02d}`**. {data.get('mention', '')} (`{data.get('display_name', 'N/A')}`)\n" # Added rank format
            f"   └ {e('calendar')} Tham gia: {format_discord_time(joined_at, 'D')} ({time_in_server_str})"
        )
        rank += 1

    embed.description += "\n\n" + "\n".join(desc_lines)
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    return embed

# --- create_invite_embeds ---
# (No significant changes needed)
async def create_invite_embeds(invites: List[discord.Invite], bot: discord.Client) -> List[discord.Embed]:
    """Tạo embeds cho lời mời server."""
    embeds = []
    e = lambda name: get_emoji(name, bot)
    if not invites:
        return embeds

    invites.sort(key=lambda inv: inv.uses or 0, reverse=True) # Sắp xếp theo lượt dùng
    num_invite_embeds = math.ceil(len(invites) / INVITES_PER_EMBED)

    for i in range(num_invite_embeds):
        start_index = i * INVITES_PER_EMBED
        end_index = start_index + INVITES_PER_EMBED
        invite_batch = invites[start_index:end_index]

        invite_embed = discord.Embed(
            title=f"{e('invite')} Lời mời Server Đang Hoạt Động (Phần {i + 1}/{num_invite_embeds})",
            description="*Sắp xếp theo số lượt sử dụng giảm dần.*",
            color=discord.Color.blurple()
        )
        invite_list_str = ""
        for inv in invite_batch:
            inviter_mention = "Không rõ (Webhook?)"
            if inv.inviter:
                 inviter_mention = inv.inviter.mention

            channel_mention = inv.channel.mention if inv.channel else "Không rõ"
            uses_str = f"{inv.uses or 0:,}/{inv.max_uses:,}" if inv.max_uses and inv.max_uses > 0 else f"{inv.uses or 0:,}" # Added :,
            expires_str = format_discord_time(inv.expires_at, 'R') if inv.expires_at else "Không hết hạn"
            created_str = format_discord_time(inv.created_at, 'R') if inv.created_at else "Không rõ"

            invite_list_str += (f"**`{inv.code}`** (Tạo bởi: {inviter_mention})\n"
                                f" └ {e('text_channel')} Kênh: {channel_mention} | {e('members')} SD: **{uses_str}**\n"
                                f"   └ {e('calendar')} Tạo: {created_str} | {e('clock')} Hết hạn: {expires_str}\n")

        invite_embed.description += "\n" + (invite_list_str if invite_list_str else "Không có dữ liệu.")
        if len(invite_embed.description) > 4000:
            invite_embed.description = invite_embed.description[:4000] + "\n... (quá dài)"
        embeds.append(invite_embed)
    return embeds

# --- create_webhook_integration_embeds ---
# (No significant changes needed)
async def create_webhook_integration_embeds(
    webhooks: List[discord.Webhook],
    integrations: List[discord.Integration],
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds cho webhooks và tích hợp."""
    embeds = []
    e = lambda name: get_emoji(name, bot)

    if webhooks:
        num_webhook_embeds = math.ceil(len(webhooks) / WEBHOOKS_PER_EMBED)
        for i in range(num_webhook_embeds):
            start_index = i * WEBHOOKS_PER_EMBED
            end_index = start_index + WEBHOOKS_PER_EMBED
            webhook_batch = webhooks[start_index:end_index]
            webhook_embed = discord.Embed(
                title=f"{e('webhook')} Webhooks Đang Hoạt Động (Phần {i + 1}/{num_webhook_embeds})",
                color=discord.Color.dark_grey()
            )
            webhook_list_str = ""
            for wh in webhook_batch:
                creator = wh.user.mention if wh.user else "Không rõ (Bot tạo?)"
                channel_mention = wh.channel.mention if wh.channel else f"ID: `{wh.channel_id}` (Không rõ)"
                created_at_str = format_discord_time(wh.created_at, 'R') if wh.created_at else "Không rõ"
                webhook_list_str += (f"**{wh.name}** (`{wh.id}`)\n"
                                     f" └ {e('text_channel')} Kênh: {channel_mention} | {e('crown')} Tạo bởi: {creator}\n"
                                     f"   └ {e('calendar')} Tạo: {created_at_str} | {e('invite')} URL: ||`{wh.url}`||\n")
            webhook_embed.description = webhook_list_str if webhook_list_str else "Không có dữ liệu."
            if len(webhook_embed.description) > 4000: webhook_embed.description = webhook_embed.description[:4000] + "\n... (quá dài)"
            embeds.append(webhook_embed)

    if integrations:
        num_integration_embeds = math.ceil(len(integrations) / INTEGRATIONS_PER_EMBED)
        for i in range(num_integration_embeds):
            start_index = i * INTEGRATIONS_PER_EMBED
            end_index = start_index + INTEGRATIONS_PER_EMBED
            integration_batch = integrations[start_index:end_index]
            integration_embed = discord.Embed(
                title=f"{e('integration')} Tích Hợp Server (Phần {i + 1}/{num_integration_embeds})",
                color=discord.Color.dark_purple()
            )
            integration_list_str = ""
            for integ in integration_batch:
                 type_str = integ.type if isinstance(integ.type, str) else integ.type.name
                 account_info = f"{integ.account.name} (`{integ.account.id}`)" if integ.account else "N/A"
                 enabled_str = f"{e('success')} Bật" if integ.enabled else f"{e('error')} Tắt"
                 sync_str = f" Đồng bộ: {'Có' if integ.syncing else 'Không'}" if hasattr(integ, 'syncing') else ""
                 role_str = f" Role: {integ.role.mention}" if hasattr(integ, 'role') and integ.role else ""
                 expire_str = f" Hành vi hết hạn: {integ.expire_behaviour.name}" if hasattr(integ, 'expire_behaviour') and integ.expire_behaviour else ""
                 grace_str = f" TG chờ: {integ.expire_grace_period}s" if hasattr(integ, 'expire_grace_period') and integ.expire_grace_period is not None else ""

                 integration_list_str += (f"**{integ.name}** ({type_str.capitalize()})\n"
                                         f" └ {e('id_card')} TK: {account_info} | {enabled_str}{sync_str}{role_str}\n"
                                         f"   └{expire_str}{grace_str}\n")
            integration_embed.description = integration_list_str if integration_list_str else "Không có dữ liệu."
            if len(integration_embed.description) > 4000: integration_embed.description = integration_embed.description[:4000] + "\n... (quá dài)"
            embeds.append(integration_embed)

    if not webhooks and not integrations:
        no_data_embed = discord.Embed(
             title=f"{e('webhook')}/{e('integration')} Webhooks & Tích Hợp",
             description="Không tìm thấy webhook hoặc tích hợp nào.",
             color=discord.Color.light_grey()
        )
        embeds.append(no_data_embed)

    return embeds

# --- create_audit_log_summary_embeds ---
# (Using increased limit per embed)
async def create_audit_log_summary_embeds(
    audit_logs: List[Dict[str, Any]],
    guild: discord.Guild,
    bot: discord.Client,
    limit_per_embed: int = AUDIT_LOG_ENTRIES_PER_EMBED
) -> List[discord.Embed]:
    """Tạo embeds tóm tắt hoạt động audit log gần đây."""
    embeds = []
    e = lambda name: get_emoji(name, bot)
    if not audit_logs:
        return embeds

    # Sort logs first by timestamp (descending) for correct reporting order
    audit_logs.sort(
         key=lambda x: x.get('created_at') if isinstance(x.get('created_at'), datetime.datetime) else datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
         reverse=True
    )

    # Calculate stats based on the *full* fetched list (before pagination)
    action_counts = collections.Counter(log['action_type'] for log in audit_logs)
    mod_action_counts = collections.Counter()
    channel_create_counts = collections.Counter()
    channel_delete_counts = collections.Counter()
    role_create_counts = collections.Counter()
    role_delete_counts = collections.Counter()

    user_cache: Dict[int, Optional[Union[discord.User, discord.Member]]] = {}
    bot_ids = {m.id for m in guild.members if m.bot} # Cache bot IDs

    async def get_user_name(user_id: Optional[Union[str, int]]) -> str:
        # (Helper function remains the same)
        if user_id is None: return "Không rõ"
        try: user_id_int = int(user_id)
        except (ValueError, TypeError): return f"ID không hợp lệ: `{user_id}`"
        if user_id_int not in user_cache:
             user = guild.get_member(user_id_int)
             if not user:
                 try: user = await guild.fetch_member(user_id_int)
                 except (discord.NotFound, discord.HTTPException): user = await fetch_user_data(None, user_id_int, bot_ref=bot)
             user_cache[user_id_int] = user
        user = user_cache[user_id_int]
        return user.mention if user else f"ID: `{user_id_int}`"

    for log_entry in audit_logs:
        action_type = log_entry.get('action_type')
        user_id = log_entry.get('user_id')

        if user_id:
             try:
                 mod_id_int = int(user_id)
                 if mod_id_int not in bot_ids: # Filter bots
                      if action_type in ['kick', 'ban', 'unban', 'member_update', 'moderate_members']: mod_action_counts[mod_id_int] += 1
                      elif action_type in ['channel_create', 'thread_create']: channel_create_counts[mod_id_int] += 1
                      elif action_type in ['channel_delete', 'thread_delete']: channel_delete_counts[mod_id_int] += 1
                      elif action_type == 'role_create': role_create_counts[mod_id_int] += 1
                      elif action_type == 'role_delete': role_delete_counts[mod_id_int] += 1
             except (ValueError, TypeError): pass

    # --- Create Summary Embed ---
    num_total_logs_in_batch = len(audit_logs) # Total logs passed to this function
    summary_embed = discord.Embed(
        title=f"{e('shield')} Tóm tắt Audit Log Gần Đây",
        description=f"Phân tích **{num_total_logs_in_batch}** entry gần nhất được lưu.",
        color=discord.Color.dark_blue(),
        timestamp=discord.utils.utcnow()
    )
    top_actions = action_counts.most_common(10)
    action_summary = "\n".join([f"- `{action}`: {count:,}" for action, count in top_actions])
    if len(action_counts) > 10: action_summary += f"\n- ... và {len(action_counts) - 10} loại khác."
    summary_embed.add_field(name=f"{e('stats')} Top Actions", value=action_summary if action_summary else "Không có", inline=False)

    top_mods = mod_action_counts.most_common(5)
    mod_summary_lines = []
    for mod_id_int, count in top_mods:
         mod_name = await get_user_name(mod_id_int)
         mod_summary_lines.append(f"- {mod_name}: {count:,} hành động") # Clarified 'actions'
    if len(mod_action_counts) > 5: mod_summary_lines.append(f"- ... và {len(mod_action_counts) - 5} mod khác.")

    mod_summary = "\n".join(mod_summary_lines)
    summary_embed.add_field(name=f"{e('crown')} Top Hoạt động Mod (Lọc Bot)", value=mod_summary if mod_summary else "Không có", inline=True) # Renamed field

    create_delete_summary = ""
    total_chan_create = sum(channel_create_counts.values())
    total_chan_delete = sum(channel_delete_counts.values())
    total_role_create = sum(role_create_counts.values())
    total_role_delete = sum(role_delete_counts.values())
    if total_chan_create > 0 or total_chan_delete > 0:
        create_delete_summary += f"{e('text_channel')}/{e('thread')} Kênh/Luồng: +{total_chan_create:,} / -{total_chan_delete:,}\n" # Added thread
    if total_role_create > 0 or total_role_delete > 0:
        create_delete_summary += f"{e('role')} Role: +{total_role_create:,} / -{total_role_delete:,}"

    summary_embed.add_field(name="Tạo/Xóa (Lọc Bot)", value=create_delete_summary.strip() if create_delete_summary else "Không có", inline=True)
    embeds.append(summary_embed)

    # --- Create Detail Embeds (paginated) ---
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
             log_details_str = ""
             for log_entry in log_batch:
                 actor = await get_user_name(log_entry.get('user_id'))
                 action = log_entry.get('action_type', 'unknown')
                 target_str = ""
                 target_id = log_entry.get('target_id')
                 if target_id:
                      target_name = None
                      extra = log_entry.get('extra_data')
                      target_obj = None
                      try: target_id_int = int(target_id)
                      except: target_id_int = None

                      if target_id_int:
                           if action.startswith("member_") or action == 'kick' or action == 'ban' or action == 'unban': target_obj = await get_user_name(target_id_int) # Includes mod actions
                           elif action.startswith("role_"): target_obj = guild.get_role(target_id_int)
                           elif action.startswith("channel_") or action.startswith("thread_"): target_obj = guild.get_channel_or_thread(target_id_int)
                           elif action.startswith("invite_"): target_obj = f"Invite (`{target_id}`)" # Invite code might be target
                           elif action.startswith("webhook_"): target_obj = f"Webhook (`{target_id}`)" # Often ID is target

                      # Try extracting name from changes if object not found
                      if not target_obj and extra and isinstance(extra, dict):
                           changes = extra # Use the direct 'extra_data'/'changes'
                           target_name_after = changes.get('after', {}).get('name')
                           target_name_before = changes.get('before', {}).get('name')
                           # Handle role/channel names possibly nested
                           if isinstance(target_name_after, dict) and 'name' in target_name_after: target_name_after = target_name_after['name']
                           if isinstance(target_name_before, dict) and 'name' in target_name_before: target_name_before = target_name_before['name']

                           if isinstance(target_name_after, str): target_name = target_name_after
                           elif isinstance(target_name_before, str): target_name = target_name_before


                      if isinstance(target_obj, (discord.Member, discord.User, discord.Role, discord.abc.GuildChannel, discord.Thread)):
                          target_str = f" -> {target_obj.mention}" if hasattr(target_obj, 'mention') else f" -> `{getattr(target_obj, 'name', target_id)}`"
                      elif isinstance(target_obj, str): # Handle pre-formatted targets (like Invite)
                          target_str = f" -> {target_obj}"
                      elif target_name: target_str = f" -> `{target_name}` (ID:`{target_id}`)"
                      else: target_str = f" -> ID: `{target_id}`"

                 reason = log_entry.get('reason')
                 reason_str = f" | Lý do: *{reason}*" if reason else ""
                 created_at_dt = log_entry.get('created_at')
                 time_str = format_discord_time(created_at_dt, 'R') if isinstance(created_at_dt, datetime.datetime) else "N/A"
                 log_details_str += f"**[{action}]** {actor}{target_str} ({time_str}){reason_str}\n"

             detail_embed.description = log_details_str if log_details_str else "Không có log trong phần này."
             if len(detail_embed.description) > 4000: detail_embed.description = detail_embed.description[:4000] + "\n... (quá dài)"
             embeds.append(detail_embed)

    return embeds

# --- create_permission_audit_embeds ---
# (No significant changes needed)
async def create_permission_audit_embeds(
    permission_results: Dict[str, List[Dict[str, Any]]],
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds tóm tắt kết quả phân tích quyền."""
    embeds = []
    e = lambda name: get_emoji(name, bot)
    if not permission_results:
        return embeds

    # --- Embed 1: Roles có quyền Admin ---
    roles_admin = permission_results.get("roles_with_admin", [])
    admin_embed = discord.Embed(
        title=f"{e('shield')}{e('crown')} Roles có quyền Administrator",
        color=discord.Color.red()
    )
    if roles_admin:
        admin_list_str = ""
        roles_admin.sort(key=lambda r: r.get('position', 0), reverse=True) # Sắp xếp theo vị trí
        for role_info in roles_admin[:PERMISSION_AUDIT_ITEMS_PER_EMBED]: # Giới hạn hiển thị
            admin_list_str += f"- <@&{role_info['id']}> (`{role_info['name']}`) - Pos: {role_info['position']} ({e('members')} {role_info.get('member_count', 'N/A')})\n"
        if len(roles_admin) > PERMISSION_AUDIT_ITEMS_PER_EMBED:
             admin_list_str += f"\n... và {len(roles_admin) - PERMISSION_AUDIT_ITEMS_PER_EMBED} role khác."
        admin_embed.description = admin_list_str
        if len(admin_embed.description) > 4000: admin_embed.description = admin_embed.description[:4000] + "\n... (quá dài)"
    else:
        admin_embed.description = f"{e('success')} Không tìm thấy role nào (ngoài owner) có quyền Administrator."
        admin_embed.color = discord.Color.green()
    embeds.append(admin_embed)

    # --- Embed 2: Quyền @everyone Nguy hiểm ---
    risky_everyone = permission_results.get("risky_everyone_overwrites", [])
    everyone_embed = discord.Embed(
        title=f"{e('error')} Kênh có quyền @everyone Tiềm ẩn Rủi ro",
        color=discord.Color.orange()
    )
    if risky_everyone:
        everyone_list_str = ""
        channels_affected = defaultdict(list)
        for item in risky_everyone:
             channel_mention = f"<#{item['channel_id']}>"
             perm_list = ", ".join(f"`{p}`" for p in item['permissions'])
             # <<< MODIFIED: Use channel_type_emoji if available >>>
             type_emoji = item.get('channel_type_emoji', '❓')
             channels_affected[channel_mention].append(f"{type_emoji} {perm_list}") # Prepend emoji

        items_shown = 0
        for channel_mention, perms_list in channels_affected.items():
            if items_shown >= PERMISSION_AUDIT_ITEMS_PER_EMBED: break
            everyone_list_str += f"**{channel_mention}**:\n"
            for perms_with_emoji in perms_list: # Usually just 1 item per channel
                 everyone_list_str += f"  └ {perms_with_emoji}\n"
            items_shown += 1

        if len(risky_everyone) > items_shown:
             everyone_list_str += f"\n... và {len(risky_everyone) - items_shown} mục khác." # Clarified 'items'
        everyone_embed.description = everyone_list_str
        if len(everyone_embed.description) > 4000: everyone_embed.description = everyone_embed.description[:4000] + "\n... (quá dài)"
    else:
        everyone_embed.description = f"{e('success')} Không tìm thấy kênh nào có quyền @everyone nguy hiểm được cấp."
        everyone_embed.color = discord.Color.green()
    embeds.append(everyone_embed)

    # --- Embed 3: Roles Khác có Quyền Nguy hiểm ---
    other_risky = permission_results.get("other_risky_role_perms", [])
    other_embed = discord.Embed(
        title=f"{e('warning')} Roles Khác (Không phải Admin/Bot) có Quyền Rủi ro",
        color=discord.Color.gold()
    )
    if other_risky:
        other_list_str = ""
        other_risky.sort(key=lambda r: r.get('position', 0), reverse=True)
        for role_info in other_risky[:PERMISSION_AUDIT_ITEMS_PER_EMBED]: # Giới hạn
            perm_list = ", ".join(f"`{p}`" for p in role_info['permissions'])
            other_list_str += f"**<@&{role_info['role_id']}> (`{role_info['role_name']}`)** - Pos: {role_info['position']}\n"
            other_list_str += f"  └ Quyền: {perm_list} ({e('members')} {role_info.get('member_count', 'N/A')})\n"
        if len(other_risky) > PERMISSION_AUDIT_ITEMS_PER_EMBED:
             other_list_str += f"\n... và {len(other_risky) - PERMISSION_AUDIT_ITEMS_PER_EMBED} role khác."
        other_embed.description = other_list_str
        if len(other_embed.description) > 4000: other_embed.description = other_embed.description[:4000] + "\n... (quá dài)"
    else:
         other_embed.description = f"{e('success')} Không tìm thấy role không phải admin/bot nào có các quyền nguy hiểm được liệt kê."
         other_embed.color = discord.Color.green()
    embeds.append(other_embed)

    return embeds

# --- create_keyword_analysis_embeds ---
# (No significant changes needed)
async def create_keyword_analysis_embeds(
    keyword_counts: Counter,
    channel_keyword_counts: Dict[int, Counter],
    thread_keyword_counts: Dict[int, Counter],
    user_keyword_counts: Dict[int, Counter], # Đã lọc bot ở bot.py
    guild: discord.Guild,
    bot: discord.Client,
    target_keywords: List[str]
) -> List[discord.Embed]:
    """Tạo embeds tóm tắt kết quả phân tích từ khóa."""
    embeds = []
    e = lambda name: get_emoji(name, bot)

    if not keyword_counts: return embeds

    # --- Embed 1: Tổng quan Từ khóa ---
    kw_overall_embed = discord.Embed(
        title=f"{e('hashtag')} Phân tích Từ khóa",
        description=f"Đếm số lần xuất hiện của **{len(target_keywords)}** từ khóa (không phân biệt hoa thường).",
        color=discord.Color.blue()
    )
    kw_summary_str = ""
    sorted_keywords = sorted(keyword_counts.items(), key=lambda item: item[1], reverse=True)
    for keyword, count in sorted_keywords[:15]: # Giới hạn hiển thị top 15
         kw_summary_str += f"- `{keyword}`: **{count:,}** lần\n"
    if len(sorted_keywords) > 15: kw_summary_str += f"- ... và {len(sorted_keywords)-15} từ khóa khác.\n"
    if not kw_summary_str: kw_summary_str = "Không tìm thấy từ khóa nào."

    kw_overall_embed.add_field(name="Tổng số lần xuất hiện", value=kw_summary_str, inline=False)
    embeds.append(kw_overall_embed)

    # --- Embed 2: Top Kênh/Luồng chứa Từ khóa ---
    kw_channel_embed = discord.Embed(
        title=f"{e('text_channel')}/{e('thread')} Top Kênh/Luồng theo Từ khóa",
        color=discord.Color.green()
    )
    channel_kw_ranking = []
    all_location_counts = {**channel_keyword_counts, **thread_keyword_counts} # Gộp dict kênh và luồng
    for loc_id, counts in all_location_counts.items():
        total_count = sum(counts.values())
        if total_count > 0:
             location_obj = guild.get_channel_or_thread(loc_id)
             loc_mention = location_obj.mention if location_obj else f"`ID:{loc_id}`"
             # <<< MODIFIED: Include type emoji >>>
             loc_type_emoji = utils.get_channel_type_emoji(location_obj, bot) if location_obj else "❓"
             channel_kw_ranking.append({"mention": loc_mention, "total": total_count, "details": dict(counts), "emoji": loc_type_emoji})

    channel_kw_ranking.sort(key=lambda x: x['total'], reverse=True)
    channel_rank_str = ""
    for i, item in enumerate(channel_kw_ranking[:KEYWORD_RANKING_LIMIT]):
        details = ", ".join(f"`{kw}`: {c:,}" for kw, c in item['details'].items())
        if len(details) > 150: details = details[:150] + "..."
        # <<< MODIFIED: Add type emoji >>>
        channel_rank_str += f"**{i+1}. {item['emoji']} {item['mention']}** ({item['total']:,} tổng)\n   └ {details}\n"
    if not channel_rank_str: channel_rank_str = "Không có kênh/luồng nào chứa từ khóa."
    if len(channel_kw_ranking) > KEYWORD_RANKING_LIMIT: channel_rank_str += f"\n... và {len(channel_kw_ranking) - KEYWORD_RANKING_LIMIT} kênh/luồng khác."
    kw_channel_embed.description = channel_rank_str
    if len(kw_channel_embed.description) > 4000: kw_channel_embed.description = kw_channel_embed.description[:4000] + "\n... (quá dài)"
    embeds.append(kw_channel_embed)

    # --- Embed 3: Top User dùng Từ khóa (user_keyword_counts đã lọc bot) ---
    kw_user_embed = discord.Embed(
        title=f"{e('members')} Top User theo Từ khóa (Đã lọc Bot)",
        color=discord.Color.orange()
    )
    user_total_counts = {uid: sum(counts.values()) for uid, counts in user_keyword_counts.items()}
    sorted_user_ids = sorted(user_total_counts, key=user_total_counts.get, reverse=True)

    limit_user = 15 # Top 15 user
    user_rank_str = ""
    rank = 1
    user_ids_to_fetch = sorted_user_ids[:limit_user]
    fetch_tasks = [fetch_user_data(guild, uid, bot_ref=bot) for uid in user_ids_to_fetch]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    fetched_users: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    for idx, result in enumerate(results):
        uid = user_ids_to_fetch[idx]
        if isinstance(result, (discord.User, discord.Member)): fetched_users[uid] = result
        else: fetched_users[uid] = None
        if isinstance(result, Exception): # Log fetch errors
            log.warning(f"Lỗi fetch user {uid} cho keyword embed: {result}")

    for user_id in user_ids_to_fetch:
        user_obj = fetched_users.get(user_id)
        user_mention = user_obj.mention if user_obj else f"`{user_id}`"
        user_display = f" (`{user_obj.display_name}`)" if user_obj else ""
        total_count = user_total_counts[user_id]
        details = ", ".join(f"`{kw}`: {c:,}" for kw, c in user_keyword_counts[user_id].items())
        if len(details) > 150: details = details[:150] + "..."
        user_rank_str += f"**`#{rank:02d}`**. {user_mention}{user_display} ({total_count:,} tổng)\n   └ {details}\n" # Added rank format
        rank += 1

    if not user_rank_str: user_rank_str = "Không có user nào sử dụng từ khóa."
    if len(sorted_user_ids) > limit_user: user_rank_str += f"\n... và {len(sorted_user_ids) - limit_user} user khác."

    kw_user_embed.description = user_rank_str
    if len(kw_user_embed.description) > 4000: kw_user_embed.description = kw_user_embed.description[:4000] + "\n... (quá dài)"
    embeds.append(kw_user_embed)

    return embeds

# --- create_error_embed ---
# (No significant changes needed)
async def create_error_embed(scan_errors: List[str], bot: discord.Client) -> Optional[discord.Embed]:
    """Tạo embed tóm tắt lỗi quét."""
    if not scan_errors:
        return None
    e = lambda name: get_emoji(name, bot)

    error_embed = discord.Embed(
        title=f"{e('error')} Tóm tắt Lỗi và Cảnh báo Khi Quét",
        color=discord.Color.dark_red(),
        timestamp=discord.utils.utcnow()
    )
    errors_per_page = 20
    num_error_pages = math.ceil(len(scan_errors) / errors_per_page)
    current_page_errors = scan_errors[:errors_per_page]
    error_text = "\n".join([f"- {err}" for err in current_page_errors])

    if len(scan_errors) > errors_per_page:
        error_text += f"\n... và {len(scan_errors) - errors_per_page} lỗi/cảnh báo khác."
        error_embed.set_footer(text=f"Trang 1/{num_error_pages} | Tổng cộng {len(scan_errors)} lỗi/cảnh báo.")
    if len(error_text) > 4000:
         error_text = error_text[:4000] + "\n... (nội dung lỗi quá dài)"
    error_embed.description = error_text if error_text else "Không có lỗi nào được ghi nhận."
    return error_embed


# --- create_role_change_stats_embeds ---
# (No significant changes needed)
async def create_role_change_stats_embeds(
    role_change_stats: Dict[str, Dict[str, Counter]],
    guild: discord.Guild,
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds cho thống kê số lần role được cấp/hủy bởi moderator."""
    embeds = []
    e = lambda name: get_emoji(name, bot)
    if not role_change_stats:
        return embeds

    num_role_stat_embeds = math.ceil(len(role_change_stats) / ROLES_STATS_PER_EMBED)
    if num_role_stat_embeds == 0: return embeds

    all_mod_ids = set()
    for stats in role_change_stats.values():
        all_mod_ids.update(stats["added"].keys())
        all_mod_ids.update(stats["removed"].keys())
    mod_fetch_tasks = [fetch_user_data(guild, mod_id, bot_ref=bot) for mod_id in all_mod_ids]
    mod_results = await asyncio.gather(*mod_fetch_tasks, return_exceptions=True)
    mod_user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    for idx, result in enumerate(mod_results):
        mod_id = list(all_mod_ids)[idx]
        if isinstance(result, (discord.User, discord.Member)): mod_user_cache[mod_id] = result
        else: mod_user_cache[mod_id] = None
        if isinstance(result, Exception): log.warning(f"Lỗi fetch mod {mod_id} cho role stats (by mod): {result}")

    sorted_role_ids = sorted(role_change_stats.keys(), key=lambda rid: sum(role_change_stats[rid]['added'].values()) + sum(role_change_stats[rid]['removed'].values()), reverse=True)

    for i in range(num_role_stat_embeds):
        start_index = i * ROLES_STATS_PER_EMBED
        end_index = start_index + ROLES_STATS_PER_EMBED
        role_batch_ids = sorted_role_ids[start_index:end_index]

        embed = discord.Embed(
            title=f"{e('role')}{e('stats')} Thống kê Cấp/Hủy Role (Bởi Mod - Phần {i + 1}/{num_role_stat_embeds})",
            description="*Dựa trên dữ liệu Audit Log đã quét.*",
            color=discord.Color.magenta()
        )
        field_count = 0
        for role_id_str in role_batch_ids:
            if field_count >= 24: # Discord limit is 25 fields
                 log.warning(f"Đạt giới hạn field ({field_count}) cho embed thống kê role (by mod) trang {i+1}")
                 break

            try: role_id_int = int(role_id_str)
            except ValueError: continue

            role = guild.get_role(role_id_int)
            role_mention = role.mention if role else f"`{role_id_str}` (Không tìm thấy?)"
            role_name = f"`{role.name}`" if role else "`N/A`"

            added_counter = role_change_stats[role_id_str]['added']
            removed_counter = role_change_stats[role_id_str]['removed']
            total_added = sum(added_counter.values())
            total_removed = sum(removed_counter.values())

            if total_added == 0 and total_removed == 0: continue

            value_str = ""
            if total_added > 0:
                value_str += f"**Được cấp:** {total_added:,} lần"
                top_adder_ids = added_counter.most_common(1)
                if top_adder_ids:
                    adder_id, add_count = top_adder_ids[0]
                    adder_obj = mod_user_cache.get(adder_id)
                    adder_name = adder_obj.mention if adder_obj else f"`{adder_id}`"
                    value_str += f" (Top: {adder_name}: {add_count:,})\n" # Shortened text
                else: value_str += "\n"
            if total_removed > 0:
                value_str += f"**Bị hủy:** {total_removed:,} lần"
                top_remover_ids = removed_counter.most_common(1)
                if top_remover_ids:
                     remover_id, remove_count = top_remover_ids[0]
                     remover_obj = mod_user_cache.get(remover_id)
                     remover_name = remover_obj.mention if remover_obj else f"`{remover_id}`"
                     value_str += f" (Top: {remover_name}: {remove_count:,})\n" # Shortened text
                else: value_str += "\n"

            if value_str:
                 if len(value_str) > 1020: value_str = value_str[:1020] + "...)"
                 try:
                    embed.add_field(name=f"{role_mention} {role_name}", value=value_str.strip(), inline=False)
                    field_count += 1
                 except Exception as field_err:
                    log.error(f"Lỗi thêm field cho role {role_id_str} (by mod): {field_err}")

        if not embed.fields:
             embed.description = "Không có dữ liệu thống kê thay đổi role (bởi mod) cho phần này."
        elif len(embed) > 5900:
             log.warning(f"Embed thống kê role (by mod) trang {i+1} quá dài ({len(embed)} chars). Có thể bị cắt.")

        embeds.append(embed)

    return embeds

# --- create_user_role_change_embeds ---
# (No significant changes needed)
async def create_user_role_change_embeds(
    user_role_changes: Dict[int, Dict[str, Dict[str, int]]],
    guild: discord.Guild,
    bot: discord.Client
) -> List[discord.Embed]:
    """Tạo embeds cho thống kê role được cấp/hủy cho từng user."""
    embeds = []
    e = lambda name: get_emoji(name, bot)
    if not user_role_changes:
        return embeds

    sorted_user_ids = sorted(
        user_role_changes.keys(),
        key=lambda uid: sum(stats["added"] + stats["removed"] for stats in user_role_changes[uid].values()),
        reverse=True
    )

    num_user_stat_embeds = math.ceil(len(sorted_user_ids) / USER_ROLE_STATS_PER_EMBED)
    if num_user_stat_embeds == 0: return embeds

    user_fetch_tasks = [fetch_user_data(guild, uid, bot_ref=bot) for uid in sorted_user_ids]
    user_results = await asyncio.gather(*user_fetch_tasks, return_exceptions=True)
    user_cache: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    for idx, result in enumerate(user_results):
        user_id = sorted_user_ids[idx]
        if isinstance(result, (discord.User, discord.Member)): user_cache[user_id] = result
        else: user_cache[user_id] = None
        if isinstance(result, Exception): log.warning(f"Lỗi fetch user {user_id} cho user role stats: {result}")

    for i in range(num_user_stat_embeds):
        start_index = i * USER_ROLE_STATS_PER_EMBED
        end_index = start_index + USER_ROLE_STATS_PER_EMBED
        user_batch_ids = sorted_user_ids[start_index:end_index]

        embed = discord.Embed(
            title=f"{e('members')}{e('role')} Thống kê Cấp/Hủy Role (Cho User - Phần {i + 1}/{num_user_stat_embeds})",
            description="*Dựa trên dữ liệu Audit Log đã quét. Sắp xếp theo tổng số thay đổi.*",
            color=discord.Color.dark_magenta() # Màu khác
        )
        field_count = 0
        for user_id in user_batch_ids:
            if field_count >= 24: # Discord limit 25
                 log.warning(f"Đạt giới hạn field ({field_count}) cho embed thống kê role (user) trang {i+1}")
                 break

            user_obj = user_cache.get(user_id)
            user_mention = user_obj.mention if user_obj else f"`{user_id}` (Không tìm thấy?)"
            user_display = f" (`{user_obj.display_name}`)" if user_obj else ""

            user_stats = user_role_changes[user_id]
            total_added = sum(stats["added"] for stats in user_stats.values())
            total_removed = sum(stats["removed"] for stats in user_stats.values())

            if total_added == 0 and total_removed == 0: continue

            value_str = ""
            sorted_roles_for_user = sorted(
                user_stats.items(),
                key=lambda item: item[1]["added"] + item[1]["removed"],
                reverse=True
            )
            roles_shown = 0
            max_roles_per_user = 3 # Chỉ hiển thị top 3 role thay đổi nhiều nhất cho mỗi user
            for role_id_str, changes in sorted_roles_for_user:
                if roles_shown >= max_roles_per_user: break
                added_count = changes.get("added", 0)
                removed_count = changes.get("removed", 0)
                if added_count > 0 or removed_count > 0:
                    role = guild.get_role(int(role_id_str))
                    role_mention = role.mention if role else f"`{role_id_str}`"
                    parts = []
                    if added_count > 0: parts.append(f"+{added_count}")
                    if removed_count > 0: parts.append(f"-{removed_count}")
                    value_str += f"- {role_mention}: ({' / '.join(parts)})\n"
                    roles_shown += 1

            if len(sorted_roles_for_user) > roles_shown:
                 value_str += f"- ... và {len(sorted_roles_for_user) - roles_shown} role khác.\n"

            if value_str:
                if len(value_str) > 1020: value_str = value_str[:1020] + "...)"
                try:
                    embed.add_field(name=f"{user_mention}{user_display}", value=value_str.strip(), inline=False)
                    field_count += 1
                except Exception as field_err:
                    log.error(f"Lỗi thêm field cho user {user_id} (role stats): {field_err}")

        if not embed.fields:
             embed.description = "Không có dữ liệu thống kê thay đổi role (cho user) cho phần này."
        elif len(embed) > 5900:
             log.warning(f"Embed thống kê role (user) trang {i+1} quá dài ({len(embed)} chars). Có thể bị cắt.")

        embeds.append(embed)

    return embeds

# --- create_reaction_analysis_embed ---
# (No significant changes needed)
async def create_reaction_analysis_embed(
    reaction_emoji_counts: Counter,
    overall_total_reaction_count: int,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed cho phân tích biểu cảm."""
    e = lambda name: get_emoji(name, bot)
    if not reaction_emoji_counts:
        return None

    embed = discord.Embed(
        title=f"{e('reaction')} Phân tích Biểu cảm",
        description=(
            f"Tổng cộng **{overall_total_reaction_count:,}** biểu cảm đã được quét.\n"
            f"*Lưu ý: Chỉ đếm số lượt thả, không đếm người thả do giới hạn hiệu năng.*"
        ),
        color=discord.Color.yellow()
    )

    top_emojis = reaction_emoji_counts.most_common(REACTIONS_PER_EMBED)
    emoji_list_str = ""
    for emoji_key, count in top_emojis:
        display_emoji = emoji_key
        # Try to resolve custom emoji string to actual emoji object
        if emoji_key.startswith('<') and emoji_key.endswith('>'):
            try:
                emoji_id_match = re.search(r':(\d+)>$', emoji_key)
                if emoji_id_match:
                    emoji_id = int(emoji_id_match.group(1))
                    found = discord.utils.get(bot.emojis, id=emoji_id)
                    if found: display_emoji = str(found)
            except: pass # Ignore parsing errors

        emoji_list_str += f"{display_emoji}: **{count:,}** lần\n"

    if len(reaction_emoji_counts) > REACTIONS_PER_EMBED:
        emoji_list_str += f"\n... và {len(reaction_emoji_counts) - REACTIONS_PER_EMBED} biểu cảm khác."

    embed.add_field(name="Top Biểu cảm Được sử dụng", value=emoji_list_str if emoji_list_str else "Không có", inline=False)

    if len(embed) > 5900:
        log.warning(f"Embed phân tích biểu cảm quá dài ({len(embed)} chars). Có thể bị cắt.")

    return embed

# --- create_top_inviters_embed ---
# (No significant changes needed)
async def create_top_inviters_embed(
    invite_usage_counts: Counter,
    guild: discord.Guild,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed cho top người mời dựa trên số lượt sử dụng invite."""
    e = lambda name: get_emoji(name, bot)
    if not invite_usage_counts:
        return None

    embed = discord.Embed(
        title=f"{e('award')}{e('invite')} Top Người Mời (Theo Lượt Sử dụng)",
        description="*Dựa trên số lượt sử dụng các lời mời đang hoạt động đã quét.*",
        color=discord.Color.dark_teal()
    )

    sorted_inviters = invite_usage_counts.most_common(TOP_INVITERS_LIMIT)

    desc_lines = []
    rank = 1
    inviter_ids_to_fetch = [uid for uid, count in sorted_inviters]
    fetch_tasks = [fetch_user_data(guild, uid, bot_ref=bot) for uid in inviter_ids_to_fetch]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    fetched_users: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    for idx, result in enumerate(results):
        uid = inviter_ids_to_fetch[idx]
        if isinstance(result, (discord.User, discord.Member)): fetched_users[uid] = result
        else: fetched_users[uid] = None
        if isinstance(result, Exception): log.warning(f"Lỗi fetch inviter {uid}: {result}")

    for inviter_id, usage_count in sorted_inviters:
        user_obj = fetched_users.get(inviter_id)
        user_mention = user_obj.mention if user_obj else f"`{inviter_id}` (Không tìm thấy?)"
        user_display = f" (`{user_obj.display_name}`)" if user_obj else ""

        desc_lines.append(f"**`#{rank:02d}`**. {user_mention}{user_display}**: {usage_count:,}** lượt sử dụng") # Added rank format and bold count
        rank += 1

    if len(invite_usage_counts) > TOP_INVITERS_LIMIT:
        desc_lines.append(f"\n... và {len(invite_usage_counts) - TOP_INVITERS_LIMIT} người mời khác.")

    embed.description += "\n\n" + "\n".join(desc_lines)
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    return embed


# --- Hàm Chung Tạo Embed Leaderboard ---
# (No significant changes needed)
async def create_generic_leaderboard_embed(
    counter_data: Counter,
    guild: discord.Guild,
    bot: discord.Client,
    title: str,
    item_name_singular: str,
    item_name_plural: str,
    limit: int,
    color: discord.Color = discord.Color.blue()
) -> Optional[discord.Embed]:
    """Hàm chung để tạo embed leaderboard."""
    e = lambda name: get_emoji(name, bot)
    if not counter_data:
        return None

    embed = discord.Embed(
        title=f"{e('award')} Top {limit} User - {title}",
        description=f"*Dựa trên số lượng {item_name_plural} đã quét. Đã lọc bot.*",
        color=color
    )

    sorted_users = counter_data.most_common(limit)

    desc_lines = []
    rank = 1
    user_ids_to_fetch = [uid for uid, count in sorted_users]
    fetch_tasks = [fetch_user_data(guild, uid, bot_ref=bot) for uid in user_ids_to_fetch]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    fetched_users: Dict[int, Optional[Union[discord.Member, discord.User]]] = {}
    for idx, result in enumerate(results):
        uid = user_ids_to_fetch[idx]
        if isinstance(result, (discord.User, discord.Member)): fetched_users[uid] = result
        else: fetched_users[uid] = None
        if isinstance(result, Exception): log.warning(f"Lỗi fetch user {uid} cho leaderboard '{title}': {result}")

    for user_id, count in sorted_users:
        user_obj = fetched_users.get(user_id)
        user_mention = user_obj.mention if user_obj else f"`{user_id}` (Không tìm thấy?)"
        user_display = f" (`{user_obj.display_name}`)" if user_obj else ""

        # <<< MODIFIED: Added rank format and bold count >>>
        desc_lines.append(f"**`#{rank:02d}`**. {user_mention}{user_display}**: {count:,}** {item_name_plural if count != 1 else item_name_singular}")
        rank += 1

    if len(counter_data) > limit:
        desc_lines.append(f"\n... và {len(counter_data) - limit} người dùng khác.")

    embed.description += "\n\n" + "\n".join(desc_lines)
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    return embed

# --- Hàm Tạo Embed Leaderboard Cụ thể (Gọi hàm chung) ---
# (No changes needed)
async def create_top_link_posters_embed(counts: Counter, guild: discord.Guild, bot: discord.Client) -> Optional[discord.Embed]:
    return await create_generic_leaderboard_embed(counts, guild, bot, f"{get_emoji('link', bot)} Gửi Link", "link", "links", TOP_LINK_USERS_LIMIT, discord.Color.dark_blue())

async def create_top_image_posters_embed(counts: Counter, guild: discord.Guild, bot: discord.Client) -> Optional[discord.Embed]:
    return await create_generic_leaderboard_embed(counts, guild, bot, f"{get_emoji('image', bot)} Gửi Ảnh", "ảnh", "ảnh", TOP_IMAGE_USERS_LIMIT, discord.Color.dark_green())

async def create_top_emoji_users_embed(counts: Counter, guild: discord.Guild, bot: discord.Client) -> Optional[discord.Embed]:
    return await create_generic_leaderboard_embed(counts, guild, bot, f"{get_emoji('reaction', bot)} Dùng Emoji", "emoji", "emojis", TOP_EMOJI_USERS_LIMIT, discord.Color.dark_gold())

async def create_top_sticker_users_embed(counts: Counter, guild: discord.Guild, bot: discord.Client) -> Optional[discord.Embed]:
    return await create_generic_leaderboard_embed(counts, guild, bot, f"{get_emoji('sticker', bot)} Dùng Sticker", "sticker", "stickers", TOP_STICKER_USERS_LIMIT, discord.Color.dark_purple())

# --- Hàm mới: Top Roles Granted ---
# (No significant changes needed)
async def create_top_roles_granted_embed(
    role_change_stats: Dict[str, Dict[str, Counter]],
    guild: discord.Guild,
    bot: discord.Client
) -> Optional[discord.Embed]:
    """Tạo embed cho top roles được cấp nhiều nhất."""
    e = lambda name: get_emoji(name, bot)
    if not role_change_stats:
        return None

    role_grant_counts = Counter()
    for role_id_str, stats in role_change_stats.items():
        role_grant_counts[role_id_str] = sum(stats['added'].values())

    # Filter out roles with zero grants
    filtered_grant_counts = Counter({role_id: count for role_id, count in role_grant_counts.items() if count > 0})
    if not filtered_grant_counts:
        return None

    embed = discord.Embed(
        title=f"{e('award')}{e('role')} Top Roles Được Cấp Nhiều Nhất",
        description="*Dựa trên số lần role được cấp (ADD) trong Audit Log đã quét.*",
        color=discord.Color.blue()
    )

    sorted_roles = filtered_grant_counts.most_common(TOP_ROLES_GRANTED_LIMIT)
    desc_lines = []
    rank = 1
    for role_id_str, count in sorted_roles:
        role = guild.get_role(int(role_id_str))
        role_mention = role.mention if role else f"`{role_id_str}` (Unknown)"
        desc_lines.append(f"**`#{rank:02d}`**. {role_mention}**: {count:,}** lần") # Added rank format & bold count
        rank += 1

    if len(filtered_grant_counts) > TOP_ROLES_GRANTED_LIMIT:
        desc_lines.append(f"\n... và {len(filtered_grant_counts) - TOP_ROLES_GRANTED_LIMIT} roles khác.")

    embed.description += "\n\n" + "\n".join(desc_lines)
    if len(embed.description) > 4000:
        embed.description = embed.description[:4000] + "\n... (quá dài)"

    return embed


# --- CSV Report Generation ---

# _write_csv_to_list remains the same

async def create_csv_report(
    server: discord.Guild,
    bot: discord.Client,
    server_info: Dict[str, Any],
    channel_details: List[Dict[str, Any]], # Now contains both text and voice processed details
    voice_channel_data: List[Dict[str, Any]], # Static info for voice/stage
    user_activity: Dict[int, Dict[str, Any]],
    all_roles: List[discord.Role],
    boosters: List[discord.Member],
    invites: List[discord.Invite],
    webhooks: List[discord.Webhook],
    integrations: List[discord.Integration],
    audit_logs: List[Dict[str, Any]],
    permission_results: Dict[str, List[Dict[str, Any]]],
    scan_end_time: datetime.datetime,
    *,
    files_list_ref: List[discord.File],
    reaction_emoji_counts: Optional[Counter] = None,
    invite_usage_counts: Optional[Counter] = None,
    user_link_counts: Optional[Counter] = None,
    user_image_counts: Optional[Counter] = None,
    user_emoji_counts: Optional[Counter] = None,
    user_sticker_counts: Optional[Counter] = None,
) -> None:
    """Tạo các file báo cáo CSV CHÍNH trong bộ nhớ và thêm vào list."""
    e_csv = lambda name: get_emoji(name, bot)

    # 1. Server Summary (No changes needed)
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo server_summary.csv...")
        s_headers = ["Metric", "Value"]
        s_rows = [
            ["Server Name", server.name], ["Server ID", server.id], ["Owner ID", server.owner_id],
            ["Owner Name", server.owner.name if server.owner else "N/A"],
            ["Created At", server.created_at.isoformat()], ["Total Members (Cache)", server.member_count],
            ["Real Users (Scan Start)", server_info.get('member_count_real', 'N/A')],
            ["Bots (Scan Start)", server_info.get('bot_count', 'N/A')], ["Boost Tier", server.premium_tier],
            ["Boost Count", server.premium_subscription_count], ["Verification Level", str(server.verification_level)],
            ["Explicit Content Filter", str(server.explicit_content_filter)], ["MFA Level", server.mfa_level],
            ["Default Notifications", str(server.default_notifications)],
            ["System Channel ID", server.system_channel.id if server.system_channel else "N/A"],
            ["Rules Channel ID", server.rules_channel.id if server.rules_channel else "N/A"],
            ["Public Updates Channel ID", server.public_updates_channel.id if server.public_updates_channel else "N/A"],
            ["AFK Channel ID", server.afk_channel.id if server.afk_channel else "N/A"],
            ["AFK Timeout (seconds)", server.afk_timeout],
            ["Total Text Channels (Scan Start)", server_info.get('text_channel_count', 'N/A')],
            ["Total Voice Channels (Scan Start)", server_info.get('voice_channel_count', 'N/A')],
            ["Total Categories (Scan Start)", server_info.get('category_count', 'N/A')],
            ["Total Stages (Scan Start)", server_info.get('stage_count', 'N/A')],
            ["Total Forums (Scan Start)", server_info.get('forum_count', 'N/A')],
            ["Total Roles (excl. @everyone)", len(all_roles)], ["Total Emojis", len(server.emojis)],
            ["Total Stickers", len(server.stickers)],
            ["Total Reactions Scanned", server_info.get('reaction_count_overall', 'N/A (Disabled)')]
        ]
        await _write_csv_to_list("server_summary.csv", s_headers, s_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo server_summary.csv: {ex}", exc_info=True)

    # 2. Scanned Channels & Threads Detail (Text & Voice)
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo scanned_channels_threads.csv...") # Renamed file
        # <<< MODIFIED: Added Channel Type, removed some N/A fields for threads >>>
        tc_headers = ["Item Type", "Channel Type", "ID", "Name", "Parent Channel ID", "Parent Channel Name", "Category ID", "Category Name", "Created At", "Is NSFW", "Slowmode (s)", "Topic", "Message Count (Scan)", "Reaction Count (Scan)", "Scan Duration (s)", "Top Chatter ID", "Top Chatter Name", "Top Chatter Msg Count", "Is Archived", "Is Locked", "Thread Owner ID", "Scan Type Note", "Error"]
        tc_rows = []
        for detail in channel_details:
             # Add channel row (Text or Voice)
             channel_type_str = detail.get("type", "unknown")
             is_voice = channel_type_str == str(discord.ChannelType.voice)

             top_chatter_str = detail.get('top_chatter', "")
             top_chatter_id = "N/A"; top_chatter_name = "N/A"; top_chatter_msg_count = 0
             try: # Safer parsing
                 mention_match = re.search(r'<@!?(\d+)>', top_chatter_str)
                 id_match = re.search(r'ID: `(\d+)`', top_chatter_str)
                 name_match = re.search(r'\(`(.*?)`\)', top_chatter_str) # Match name inside backticks and parens
                 count_match = re.search(r'- (\d{1,3}(?:,\d{3})*) tin', top_chatter_str) # Match count

                 if mention_match: top_chatter_id = mention_match.group(1)
                 elif id_match: top_chatter_id = id_match.group(1)

                 if name_match: top_chatter_name = name_match.group(1)
                 elif top_chatter_id != "N/A" and not mention_match: top_chatter_name = "(ID Only)"
                 elif mention_match: top_chatter_name = "(Mention Only)"

                 if count_match: top_chatter_msg_count = int(count_match.group(1).replace(',', ''))
             except Exception as parse_err:
                 log.debug(f"Lỗi parse top chatter string '{top_chatter_str}': {parse_err}")

             is_nsfw = detail.get('nsfw', '').startswith(e_csv('success')) if isinstance(detail.get('nsfw'), str) else False
             slowmode_val = parse_slowmode(detail.get('slowmode', '0')) if not is_voice else None # None for voice

             channel_row = [
                 "Channel", channel_type_str, # Added Channel Type
                 detail.get('id', 'N/A'), detail.get('name', 'N/A'),
                 None, None, # Parent info N/A for channels
                 detail.get('category_id', 'N/A'), detail.get('category', 'N/A'),
                 detail.get('created_at').isoformat() if detail.get('created_at') else 'N/A',
                 is_nsfw, slowmode_val, detail.get('topic', '') if not is_voice else None, # Topic None for voice
                 detail.get('message_count', 0),
                 detail.get('reaction_count', 0),
                 detail.get('duration', datetime.timedelta(0)).total_seconds(),
                 top_chatter_id, top_chatter_name, top_chatter_msg_count,
                 None, None, None, # Thread specific fields N/A
                 detail.get('scan_type_note', ''),
                 detail.get('error', '')
             ]
             tc_rows.append(channel_row)

             # Add thread rows (only if channel was text)
             if not is_voice and "threads_data" in detail:
                 for thread_data in detail.get("threads_data", []):
                     thread_row = [
                         "Thread", str(discord.ChannelType.public_thread), # Assume public/news thread type for CSV
                         thread_data.get('id', 'N/A'), thread_data.get('name', 'N/A'),
                         detail.get('id', 'N/A'), detail.get('name', 'N/A'), # Parent info
                         detail.get('category_id', 'N/A'), detail.get('category', 'N/A'), # Inherit category
                         thread_data.get('created_at'), # Already ISO format
                         None, None, None, # NSFW/Slow/Topic N/A for threads
                         thread_data.get('message_count', 0),
                         thread_data.get('reaction_count', 0),
                         thread_data.get('scan_duration_seconds', 0),
                         None, None, None, # Top chatter N/A for threads
                         thread_data.get('archived'), thread_data.get('locked'), thread_data.get('owner_id'),
                         thread_data.get('scan_type_note', ''), # Scan type for thread
                         thread_data.get('error', '')
                     ]
                     tc_rows.append(thread_row)
        await _write_csv_to_list("scanned_channels_threads.csv", tc_headers, tc_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo scanned_channels_threads.csv: {ex}", exc_info=True)

    # 3. Static Voice/Stage Channels Info
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo static_voice_stage_channels.csv...") # Renamed file
        vc_headers = ["ID", "Name", "Type", "Category ID", "Category Name", "Created At", "User Limit", "Bitrate (bps)"]
        vc_rows = []
        for vc in voice_channel_data: # Uses the static data list
             user_limit_val = vc.get('user_limit', 0) if isinstance(vc.get('user_limit'), int) else 0
             bitrate_val = parse_bitrate(vc.get('bitrate', '0'))
             vc_rows.append([
                 vc.get('id', 'N/A'), vc.get('name', 'N/A'), vc.get('type', 'unknown'), # Use type from static data
                 vc.get('category_id', 'N/A'), vc.get('category', 'N/A'),
                 vc.get('created_at').isoformat() if vc.get('created_at') else 'N/A',
                 user_limit_val, bitrate_val
             ])
        await _write_csv_to_list("static_voice_stage_channels.csv", vc_headers, vc_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo static_voice_stage_channels.csv: {ex}", exc_info=True)

    # 4. User Activity (No changes needed)
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo user_activity.csv...")
        ua_headers = ["User ID", "Is Bot", "Message Count", "Link Count", "Image Count", "Emoji Count", "Sticker Count", "First Seen UTC", "Last Seen UTC", "Activity Span (s)"]
        ua_rows = []
        for user_id, data in user_activity.items():
             first_seen = data.get('first_seen')
             last_seen = data.get('last_seen')
             activity_span_secs = 0
             if first_seen and last_seen and last_seen >= first_seen:
                  try:
                      first_aware = first_seen.astimezone(datetime.timezone.utc) if first_seen.tzinfo else first_seen.replace(tzinfo=datetime.timezone.utc)
                      last_aware = last_seen.astimezone(datetime.timezone.utc) if last_seen.tzinfo else last_seen.replace(tzinfo=datetime.timezone.utc)
                      if last_aware >= first_aware: activity_span_secs = (last_aware - first_aware).total_seconds()
                  except Exception: pass
             ua_rows.append([
                 user_id, data.get('is_bot', False), data.get('message_count', 0),
                 data.get('link_count', 0), data.get('image_count', 0),
                 data.get('emoji_count', 0), data.get('sticker_count', 0),
                 first_seen.isoformat() if first_seen else 'N/A',
                 last_seen.isoformat() if last_seen else 'N/A',
                 round(activity_span_secs, 2)
             ])
        await _write_csv_to_list("user_activity.csv", ua_headers, ua_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo user_activity.csv: {ex}", exc_info=True)

    # 5. Roles (No changes needed)
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo roles.csv...")
        r_headers = ["ID", "Name", "Position", "Color", "Is Hoisted", "Is Mentionable", "Is Bot Role", "Member Count (Scan End)", "Created At"]
        r_rows = []
        for role in all_roles:
             r_rows.append([
                 role.id, role.name, role.position, str(role.color),
                 role.hoist, role.mentionable, role.is_bot_managed(), len(role.members),
                 role.created_at.isoformat()
             ])
        await _write_csv_to_list("roles.csv", r_headers, r_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo roles.csv: {ex}", exc_info=True)

    # 6. Boosters (No changes needed)
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo boosters.csv...")
        b_headers = ["User ID", "Username", "Display Name", "Boost Start UTC", "Boost Duration (s)"]
        b_rows = []
        for member in boosters:
             boost_duration_secs = 0
             if member.premium_since:
                  try:
                      scan_end_time_aware = scan_end_time.astimezone(datetime.timezone.utc) if scan_end_time.tzinfo else scan_end_time.replace(tzinfo=datetime.timezone.utc)
                      premium_since_aware = member.premium_since.astimezone(datetime.timezone.utc) if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc)
                      if scan_end_time_aware >= premium_since_aware: boost_duration_secs = (scan_end_time_aware - premium_since_aware).total_seconds()
                  except Exception: pass
             b_rows.append([
                 member.id, member.name, member.display_name,
                 member.premium_since.isoformat() if member.premium_since else 'N/A',
                 round(boost_duration_secs, 2)
             ])
        await _write_csv_to_list("boosters.csv", b_headers, b_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo boosters.csv: {ex}", exc_info=True)

    # 7. Invites & Top Inviters (No changes needed)
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo invites.csv...")
        i_headers = ["Code", "Inviter ID", "Inviter Name", "Channel ID", "Channel Name", "Created At UTC", "Expires At UTC", "Uses", "Max Uses", "Is Temporary"]
        i_rows = []
        for inv in invites:
             inviter_id = inv.inviter.id if inv.inviter else 'N/A'
             inviter_name = inv.inviter.name if inv.inviter else 'N/A'
             channel_id = inv.channel.id if inv.channel else 'N/A'
             channel_name = inv.channel.name if inv.channel else 'N/A'
             created_at_iso = inv.created_at.isoformat() if inv.created_at else 'N/A'
             expires_at_iso = inv.expires_at.isoformat() if inv.expires_at else 'N/A'
             i_rows.append([
                 inv.code, inviter_id, inviter_name, channel_id, channel_name,
                 created_at_iso, expires_at_iso, inv.uses or 0, inv.max_uses or 0, inv.temporary
             ])
        await _write_csv_to_list("invites.csv", i_headers, i_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo invites.csv: {ex}", exc_info=True)

    if invite_usage_counts:
         try:
             log.info(f"{e_csv('csv_file')} Đang tạo top_inviters.csv...")
             ti_headers = ["Rank", "Inviter ID", "Total Uses"]
             ti_rows = []
             for rank, (inviter_id, count) in enumerate(invite_usage_counts.most_common(), 1):
                  ti_rows.append([rank, inviter_id, count])
             await _write_csv_to_list("top_inviters.csv", ti_headers, ti_rows, files_list_ref)
         except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo top_inviters.csv: {ex}", exc_info=True)

    # 8. Webhooks (No changes needed)
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo webhooks.csv...")
        wh_headers = ["ID", "Name", "Creator ID", "Creator Name", "Channel ID", "Channel Name", "Created At UTC"]
        wh_rows = []
        for wh in webhooks:
            channel = discord.utils.get(server.text_channels, id=wh.channel_id)
            channel_name = channel.name if channel else "N/A"
            wh_rows.append([
                wh.id, wh.name, wh.user.id if wh.user else 'N/A', wh.user.name if wh.user else 'N/A',
                wh.channel_id, channel_name,
                wh.created_at.isoformat() if wh.created_at else 'N/A'
            ])
        await _write_csv_to_list("webhooks.csv", wh_headers, wh_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo webhooks.csv: {ex}", exc_info=True)

    # 9. Integrations
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo integrations.csv...")
        int_headers = ["ID", "Name", "Type", "Enabled", "Syncing", "Role ID", "Role Name", "Expire Behaviour", "Expire Grace Period (s)", "Account ID", "Account Name"]
        int_rows = []
        for integ in integrations:
             integ_type = integ.type if isinstance(integ.type, str) else integ.type.name
             role_id = integ.role.id if hasattr(integ, 'role') and integ.role else 'N/A'
             role_name = integ.role.name if hasattr(integ, 'role') and integ.role else 'N/A'
             expire_behaviour = integ.expire_behaviour.name if hasattr(integ, 'expire_behaviour') and integ.expire_behaviour else 'N/A'
             grace_period = integ.expire_grace_period if hasattr(integ, 'expire_grace_period') and integ.expire_grace_period is not None else 'N/A'
             syncing = integ.syncing if hasattr(integ, 'syncing') else 'N/A'
             int_rows.append([
                 integ.id, integ.name, integ_type, integ.enabled, syncing,
                 role_id, role_name, expire_behaviour, grace_period,
                 integ.account.id if integ.account else 'N/A',
                 integ.account.name if integ.account else 'N/A'
             ])
        await _write_csv_to_list("integrations.csv", int_headers, int_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo integrations.csv: {ex}", exc_info=True) # [Dịch]

    # 10. Audit Log Detail
    if audit_logs:
        try:
            log.info(f"{e_csv('csv_file')} Đang tạo audit_log_detail.csv...") # [Dịch]
            al_headers = ["Log ID", "Timestamp UTC", "Action Type", "User ID", "Target ID", "Reason", "Extra Data (JSON)"]
            al_rows = []
            for log_entry in audit_logs:
                 # Safely serialize extra_data, default to str for unhandled types
                 extra_data_json = json.dumps(log_entry.get('extra_data'), ensure_ascii=False, default=str) if log_entry.get('extra_data') else ""
                 created_at_dt = log_entry.get('created_at')
                 created_at_iso = created_at_dt.isoformat() if isinstance(created_at_dt, datetime.datetime) else str(created_at_dt)
                 al_rows.append([
                     log_entry.get('log_id'), created_at_iso,
                     log_entry.get('action_type'), log_entry.get('user_id'),
                     log_entry.get('target_id'), log_entry.get('reason'),
                     extra_data_json
                 ])
            await _write_csv_to_list("audit_log_detail.csv", al_headers, al_rows, files_list_ref)
        except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo audit_log_detail.csv: {ex}", exc_info=True) # [Dịch]


    # 11-13: Permission Audit CSVs
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo permission_admin_roles.csv...") # [Dịch]
        pa_admin_headers = ["Role ID", "Role Name", "Position", "Member Count (Scan End)"]
        pa_admin_rows = []
        for role_info in permission_results.get("roles_with_admin", []):
             pa_admin_rows.append([
                 role_info.get('id'), role_info.get('name'), role_info.get('position'),
                 role_info.get('member_count', 0)
             ])
        await _write_csv_to_list("permission_admin_roles.csv", pa_admin_headers, pa_admin_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo permission_admin_roles.csv: {ex}", exc_info=True) # [Dịch]

    try:
        log.info(f"{e_csv('csv_file')} Đang tạo permission_risky_everyone.csv...") # [Dịch]
        # <<< MODIFIED: Removed Channel Type, less useful than Name/ID >>>
        pa_everyone_headers = ["Channel ID", "Channel Name", "Permission Name", "Permission Value"]
        pa_everyone_rows = []
        for item in permission_results.get("risky_everyone_overwrites", []):
             for perm_name, perm_value in item.get('permissions', {}).items():
                  pa_everyone_rows.append([
                      item.get('channel_id'), item.get('channel_name'),
                      perm_name, perm_value
                  ])
        await _write_csv_to_list("permission_risky_everyone.csv", pa_everyone_headers, pa_everyone_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo permission_risky_everyone.csv: {ex}", exc_info=True) # [Dịch]

    try:
        log.info(f"{e_csv('csv_file')} Đang tạo permission_other_risky_roles.csv...") # [Dịch]
        pa_other_headers = ["Role ID", "Role Name", "Position", "Member Count (Scan End)", "Risky Permission Name"]
        pa_other_rows = []
        for role_info in permission_results.get("other_risky_role_perms", []):
            for perm_name in role_info.get('permissions', {}):
                pa_other_rows.append([
                    role_info.get('role_id'), role_info.get('role_name'), role_info.get('position'),
                    role_info.get('member_count', 0), perm_name
                ])
        await _write_csv_to_list("permission_other_risky_roles.csv", pa_other_headers, pa_other_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo permission_other_risky_roles.csv: {ex}", exc_info=True) # [Dịch]

    # 14: Reaction Summary CSV
    if reaction_emoji_counts:
         try:
             log.info(f"{e_csv('csv_file')} Đang tạo reaction_summary.csv...")
             rs_headers = ["Emoji", "Count"]
             rs_rows = []
             for emoji_key, count in reaction_emoji_counts.most_common():
                  # Use the raw key (string representation) for CSV consistency
                  rs_rows.append([emoji_key, count])
             await _write_csv_to_list("reaction_summary.csv", rs_headers, rs_rows, files_list_ref)
         except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo reaction_summary.csv: {ex}", exc_info=True)

    # --- CSVs for New Leaderboards ---
    async def create_leaderboard_csv(counter: Optional[Counter], filename: str, item_name: str):
        """Helper to create leaderboard CSVs."""
        if counter:
            try:
                log.info(f"{e_csv('csv_file')} Đang tạo {filename}...")
                headers = ["Rank", "User ID", f"{item_name} Count"]
                rows = [[rank, uid, count] for rank, (uid, count) in enumerate(counter.most_common(), 1)]
                await _write_csv_to_list(filename, headers, rows, files_list_ref)
            except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo {filename}: {ex}", exc_info=True)

    await create_leaderboard_csv(user_link_counts, "top_link_users.csv", "Link")
    await create_leaderboard_csv(user_image_counts, "top_image_users.csv", "Image")
    await create_leaderboard_csv(user_emoji_counts, "top_emoji_users.csv", "Emoji")
    await create_leaderboard_csv(user_sticker_counts, "top_sticker_users.csv", "Sticker")

    log.info(f"{e_csv('success')} Hoàn thành tạo các file CSV chính, đã thêm vào list.") # [Dịch]


# --- Auxiliary CSV Generation Functions ---

async def _write_csv_to_list(filename: str, headers: List[str], data_rows: List[List[Any]], files_list_ref: List[discord.File]):
    """Writes data to a CSV in memory and appends it to the files list."""
    output = io.StringIO()
    # <<< MODIFIED: Use QUOTE_MINIMAL for cleaner output >>>
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(headers)
    for row in data_rows:
        sanitized_row = [sanitize_for_csv(cell) for cell in row]
        try:
            writer.writerow(sanitized_row)
        except Exception as csv_write_err:
             log.error(f"Lỗi ghi dòng CSV vào {filename}: {csv_write_err} | Dữ liệu dòng (partial): {sanitized_row[:5]}...")
             # Attempt to write an error row if writing fails
             try: writer.writerow(["ERROR_WRITING_ROW"] * len(headers))
             except: pass # Ignore errors during error writing

    output.seek(0)
    # <<< MODIFIED: Added BOM for Excel compatibility >>>
    bytes_output = io.BytesIO(b'\xef\xbb\xbf' + output.getvalue().encode('utf-8')) # Add UTF-8 BOM
    files_list_ref.append(discord.File(bytes_output, filename=filename))


# --- CSV Role Change Stats (Mod -> Role) ---
async def create_role_change_stats_csv(
    role_change_stats: Dict[str, Dict[str, Counter]],
    guild: discord.Guild,
    files_list_ref: List[discord.File],
    filename_suffix: str = "_by_mod" # Thêm suffix để phân biệt
):
    """Tạo file CSV cho thống kê cấp/hủy role bởi mod."""
    if not role_change_stats: return
    e_csv = lambda name: get_emoji(name, bot=None) # Bot ref might not be needed here

    filename = f"role_change_stats{filename_suffix}.csv"
    log.info(f"{e_csv('csv_file')} Đang tạo {filename}...") # [Dịch]
    headers = ["Role ID", "Role Name", "Change Type", "Moderator ID", "Count"]
    rows = []
    for role_id_str, stats in role_change_stats.items():
        try: role_id_int = int(role_id_str)
        except ValueError: role_name = "Invalid Role ID"; role_id_int = None
        else: role = guild.get_role(role_id_int); role_name = role.name if role else "Unknown/Deleted Role"

        # Added stats
        for mod_id, count in stats["added"].items():
            rows.append([role_id_str, role_name, "ADDED", mod_id, count])
        # Removed stats
        for mod_id, count in stats["removed"].items():
            rows.append([role_id_str, role_name, "REMOVED", mod_id, count])

    rows.sort(key=lambda x: (str(x[0]), x[2], str(x[3]))) # Sắp xếp
    try:
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo {filename}: {ex}", exc_info=True) # [Dịch]

# --- CSV Role Change Stats (User <- Role) ---
async def create_user_role_change_csv(
    user_role_changes: Dict[int, Dict[str, Dict[str, int]]],
    guild: discord.Guild,
    files_list_ref: List[discord.File]
):
    """Tạo file CSV cho thống kê role được cấp/hủy cho từng user."""
    if not user_role_changes: return
    e_csv = lambda name: get_emoji(name, bot=None)

    filename = "role_change_stats_for_user.csv"
    log.info(f"{e_csv('csv_file')} Đang tạo {filename}...") # [Dịch]
    headers = ["User ID", "Role ID", "Role Name", "Change Type", "Count"]
    rows = []

    for user_id, role_stats in user_role_changes.items():
        for role_id_str, changes in role_stats.items():
            try: role_id_int = int(role_id_str)
            except ValueError: role_name = "Invalid Role ID"
            else: role = guild.get_role(role_id_int); role_name = role.name if role else "Unknown/Deleted Role"

            added_count = changes.get("added", 0)
            removed_count = changes.get("removed", 0)

            if added_count > 0:
                rows.append([user_id, role_id_str, role_name, "ADDED", added_count])
            if removed_count > 0:
                rows.append([user_id, role_id_str, role_name, "REMOVED", removed_count])

    rows.sort(key=lambda x: (str(x[0]), str(x[1]), x[3])) # Sắp xếp theo user, role, type
    try:
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo {filename}: {ex}", exc_info=True) # [Dịch]

# --- CSV Top Roles Granted ---
async def create_top_roles_granted_csv(
    role_change_stats: Dict[str, Dict[str, Counter]],
    guild: discord.Guild,
    files_list_ref: List[discord.File]
):
    """Tạo file CSV cho top roles được cấp nhiều nhất."""
    if not role_change_stats: return
    e_csv = lambda name: get_emoji(name, bot=None)

    role_grant_counts = Counter()
    for role_id_str, stats in role_change_stats.items():
        role_grant_counts[role_id_str] = sum(stats['added'].values())

    # Filter out roles with zero grants before sorting
    filtered_grant_counts = Counter({role_id: count for role_id, count in role_grant_counts.items() if count > 0})
    if not filtered_grant_counts: return

    filename = "top_roles_granted.csv"
    log.info(f"{e_csv('csv_file')} Đang tạo {filename}...")
    headers = ["Rank", "Role ID", "Role Name", "Times Granted"]
    rows = []
    rank = 1
    # Use the filtered counter for sorting
    for role_id_str, count in filtered_grant_counts.most_common(TOP_ROLES_GRANTED_LIMIT * 2): # Lấy nhiều hơn cho CSV
         try: role_id_int = int(role_id_str)
         except ValueError: role_name = "Invalid Role ID"
         else: role = guild.get_role(role_id_int); role_name = role.name if role else "Unknown/Deleted Role"
         rows.append([rank, role_id_str, role_name, count])
         rank += 1

    try:
        await _write_csv_to_list(filename, headers, rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo {filename}: {ex}", exc_info=True)

# --- CSV Top Oldest Members ---
async def create_top_oldest_members_csv(
    oldest_members_data: List[Dict[str, Any]],
    files_list_ref: List[discord.File]
):
    """Tạo file CSV cho top thành viên lâu năm và thêm vào list."""
    if not oldest_members_data: return
    e_csv = lambda name: get_emoji(name, bot=None)

    log.info(f"{e_csv('csv_file')} Đang tạo top_oldest_members.csv...") # [Dịch]
    headers = ["Rank", "User ID", "Display Name", "Joined At UTC", "Time in Server (Days Approx)"]
    rows = []
    rank = 1
    now = discord.utils.utcnow()
    for data in oldest_members_data:
        joined_at = data.get('joined_at')
        days_in_server = "N/A"
        if isinstance(joined_at, datetime.datetime):
             try:
                 join_aware = joined_at.astimezone(datetime.timezone.utc) if joined_at.tzinfo else joined_at.replace(tzinfo=datetime.timezone.utc)
                 if now >= join_aware: days_in_server = (now - join_aware).days
             except: pass

        rows.append([
            rank, data.get('id', 'N/A'), data.get('display_name', 'N/A'),
            joined_at.isoformat() if isinstance(joined_at, datetime.datetime) else 'N/A',
            days_in_server
        ])
        rank += 1
    try:
        await _write_csv_to_list("top_oldest_members.csv", headers, rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo top_oldest_members.csv: {ex}", exc_info=True) # [Dịch]

# --- CSV Keyword Reports ---
async def create_keyword_csv_reports(
    keyword_counts: Counter,
    channel_keyword_counts: Dict[int, Counter],
    thread_keyword_counts: Dict[int, Counter],
    user_keyword_counts: Dict[int, Counter], # Đã lọc bot ở bot.py
    target_keywords: List[str],
    files_list_ref: List[discord.File] # Nhận list để append
) -> None: # Không trả về gì, chỉ append vào list
    """Tạo các file CSV liên quan đến phân tích từ khóa và thêm vào list."""
    if not target_keywords or not keyword_counts:
        log.info("Không có dữ liệu từ khóa để tạo CSV.") # [Dịch]
        return
    e_csv = lambda name: get_emoji(name, bot=None)

    # --- Keyword Overall Summary CSV ---
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo keyword_summary.csv...") # [Dịch]
        kw_sum_headers = ["Keyword", "Total Count"]
        kw_sum_rows = sorted(list(keyword_counts.items()), key=lambda item: item[1], reverse=True)
        await _write_csv_to_list("keyword_summary.csv", kw_sum_headers, kw_sum_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo keyword_summary.csv: {ex}", exc_info=True) # [Dịch]

    # --- Keyword by Location (Channel/Thread) CSV ---
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo keyword_by_location.csv...") # [Dịch]
        kw_loc_headers = ["Location ID (Channel/Thread)", "Keyword", "Count"]
        kw_loc_rows = []
        all_location_counts = {**channel_keyword_counts, **thread_keyword_counts}
        for loc_id, counts in all_location_counts.items():
            for keyword, count in counts.items():
                kw_loc_rows.append([loc_id, keyword, count])
        kw_loc_rows.sort(key=lambda x: (str(x[0]), x[1])) # Sắp xếp theo ID (str) rồi keyword
        await _write_csv_to_list("keyword_by_location.csv", kw_loc_headers, kw_loc_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo keyword_by_location.csv: {ex}", exc_info=True) # [Dịch]

    # --- Keyword by User CSV ---
    try:
        log.info(f"{e_csv('csv_file')} Đang tạo keyword_by_user.csv...") # [Dịch]
        kw_user_headers = ["User ID", "Keyword", "Count"]
        kw_user_rows = []
        for user_id, counts in user_keyword_counts.items(): # user_keyword_counts đã lọc bot
            for keyword, count in counts.items():
                kw_user_rows.append([user_id, keyword, count])
        kw_user_rows.sort(key=lambda x: (str(x[0]), x[1])) # Sắp xếp theo ID (str) rồi keyword
        await _write_csv_to_list("keyword_by_user.csv", kw_user_headers, kw_user_rows, files_list_ref)
    except Exception as ex: log.error(f"{e_csv('error')} LỖI tạo keyword_by_user.csv: {ex}", exc_info=True) # [Dịch]


# --- JSON Report Generation ---
async def create_json_report(
    server: discord.Guild,
    bot: discord.Client,
    server_info: Dict[str, Any],
    channel_details: List[Dict[str, Any]], # Contains processed text & voice channels
    voice_channel_data: List[Dict[str, Any]], # Contains static voice/stage info
    user_activity: Dict[int, Dict[str, Any]],
    all_roles: List[discord.Role],
    boosters: List[discord.Member],
    invites: List[discord.Invite],
    webhooks: List[discord.Webhook],
    integrations: List[discord.Integration],
    audit_logs: List[Dict[str, Any]],
    permission_results: Dict[str, List[Dict[str, Any]]],
    oldest_members_data: List[Dict[str, Any]],
    role_change_stats: Dict[str, Dict[str, Counter]],
    user_role_changes: Dict[int, Dict[str, Dict[str, int]]],
    scan_end_time: datetime.datetime,
    keyword_counts: Optional[Counter] = None,
    channel_keyword_counts: Optional[Dict[int, Counter]] = None,
    thread_keyword_counts: Optional[Dict[int, Counter]] = None,
    user_keyword_counts: Optional[Dict[int, Counter]] = None,
    target_keywords: Optional[List[str]] = None,
    reaction_emoji_counts: Optional[Counter] = None,
    invite_usage_counts: Optional[Counter] = None,
    user_link_counts: Optional[Counter] = None,
    user_image_counts: Optional[Counter] = None,
    user_emoji_counts: Optional[Counter] = None,
    user_sticker_counts: Optional[Counter] = None,
) -> Optional[discord.File]:
    """Tạo file báo cáo JSON trong bộ nhớ."""
    e_json = lambda name: get_emoji(name, bot)
    log.info(f"{e_json('json_file')} Đang tạo báo cáo JSON...")
    report_data = {
        "report_generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "server_info": {
            "id": str(server.id), "name": server.name, "owner_id": str(server.owner_id),
            "created_at_utc": server.created_at.isoformat(), "member_count_cache": server.member_count,
            "scan_start_users": server_info.get('member_count_real', None),
            "scan_start_bots": server_info.get('bot_count', None), "boost_tier": server.premium_tier,
            "boost_count": server.premium_subscription_count, "verification_level": str(server.verification_level),
            "explicit_content_filter": str(server.explicit_content_filter), "mfa_level": server.mfa_level,
            "default_notifications": str(server.default_notifications),
            "system_channel_id": str(server.system_channel.id) if server.system_channel else None,
            "rules_channel_id": str(server.rules_channel.id) if server.rules_channel else None,
            "public_updates_channel_id": str(server.public_updates_channel.id) if server.public_updates_channel else None,
            "afk_channel_id": str(server.afk_channel.id) if server.afk_channel else None,
            "afk_timeout_seconds": server.afk_timeout, "total_roles_scan": len(all_roles),
            "total_emojis_scan": len(server.emojis), "total_stickers_scan": len(server.stickers),
            "total_reactions_scanned": server_info.get('reaction_count_overall'),
            "features": server.features,
        },
        "scan_info": {
             "scan_end_time_utc": scan_end_time.isoformat(),
             # Add other scan metrics if needed
        },
        # <<< RENAMED Keys for clarity >>>
        "scanned_channels_and_threads": [],
        "static_voice_stage_channels": [],
        "roles": [],
        "user_activity": {}, # Key is User ID (string)
        "boosters": [],
        "invites": [],
        "webhooks": [],
        "integrations": [],
        "audit_logs": [], # Will be populated later
        "permission_audit": {},
        "top_oldest_members": [],
        "role_change_stats_by_mod": {},
        "role_change_stats_for_user": {},
        "keyword_analysis": None,
        "reaction_analysis": None,
        "invite_usage_by_inviter": None,
        "user_link_counts": None,
        "user_image_counts": None,
        "user_emoji_counts": None,
        "user_sticker_counts": None,
    }

    # Populate Scanned Channels & Threads (Text & Voice)
    for detail in channel_details: # Iterates through processed text and voice channels
         channel_type_str = detail.get("type", "unknown")
         is_voice = channel_type_str == str(discord.ChannelType.voice)

         channel_json = {
             "item_type": "channel",
             "channel_type": channel_type_str,
             "id": str(detail.get('id')), "name": detail.get('name'),
             "category_id": str(detail.get('category_id')) if detail.get('category_id') else None,
             "category_name": detail.get('category'),
             "created_at_utc": detail.get('created_at').isoformat() if detail.get('created_at') else None,
             "is_nsfw": detail.get('nsfw', '').startswith(e_json('success')) if isinstance(detail.get('nsfw'), str) else False,
             "slowmode_seconds": parse_slowmode(detail.get('slowmode', '0')) if not is_voice else None,
             "topic": detail.get('topic') if not is_voice else None,
             "message_count_scan": detail.get('message_count', 0),
             "reaction_count_scan": detail.get('reaction_count'),
             "scan_duration_seconds": detail.get('duration', datetime.timedelta(0)).total_seconds(),
             "top_chatter_info_text": detail.get('top_chatter'),
             "processed": detail.get("processed", False),
             "scan_type_note": detail.get("scan_type_note"),
             "error": detail.get('error'),
             "threads": [] # Only populated for text channels
         }
         # Populate threads only if it was a text channel
         if not is_voice and "threads_data" in detail:
             for thread_data in detail.get("threads_data", []):
                 channel_json["threads"].append({
                     "item_type": "thread",
                     "thread_type": str(discord.ChannelType.public_thread), # Assume for simplicity
                     "id": str(thread_data.get('id')), "name": thread_data.get('name'),
                     "owner_id": str(thread_data.get('owner_id')) if thread_data.get('owner_id') else None,
                     "created_at_utc": thread_data.get('created_at'), # Already ISO
                     "is_archived": thread_data.get('archived'), "is_locked": thread_data.get('locked'),
                     "message_count_scan": thread_data.get('message_count', 0),
                     "reaction_count_scan": thread_data.get('reaction_count'),
                     "scan_duration_seconds": thread_data.get('scan_duration_seconds', 0),
                     "scan_type_note": thread_data.get("scan_type_note"),
                     "error": thread_data.get('error')
                 })
         report_data["scanned_channels_and_threads"].append(channel_json)

    # Populate Static Voice/Stage Channels (Using the separate static list)
    for vc in voice_channel_data:
         report_data["static_voice_stage_channels"].append({
             "id": str(vc.get('id')), "name": vc.get('name'), "type": vc.get('type', 'unknown'),
             "category_id": str(vc.get('category_id')) if vc.get('category_id') else None,
             "category_name": vc.get('category'),
             "created_at_utc": vc.get('created_at').isoformat() if vc.get('created_at') else None,
             "user_limit": vc.get('user_limit') if isinstance(vc.get('user_limit'), int) else 0,
             "bitrate_bps": parse_bitrate(vc.get('bitrate', '0')),
         })

    # Populate Roles
    for role in all_roles:
        report_data["roles"].append({
            "id": str(role.id), "name": role.name, "position": role.position,
            "color_hex": str(role.color), "is_hoisted": role.hoist, "is_mentionable": role.mentionable,
            "is_bot_managed": role.is_bot_managed(),
            "member_count_scan_end": len(role.members),
            "created_at_utc": role.created_at.isoformat(),
        })

    # Populate User Activity
    for user_id, data in user_activity.items():
        first_seen = data.get('first_seen')
        last_seen = data.get('last_seen')
        activity_span_secs = 0
        if first_seen and last_seen and last_seen >= first_seen:
             try:
                 first_aware = first_seen.astimezone(datetime.timezone.utc) if first_seen.tzinfo else first_seen.replace(tzinfo=datetime.timezone.utc)
                 last_aware = last_seen.astimezone(datetime.timezone.utc) if last_seen.tzinfo else last_seen.replace(tzinfo=datetime.timezone.utc)
                 if last_aware >= first_aware: activity_span_secs = (last_aware - first_aware).total_seconds()
             except Exception: pass
        report_data["user_activity"][str(user_id)] = {
            "is_bot": data.get('is_bot', False),
            "message_count": data.get('message_count', 0),
            "link_count": data.get('link_count', 0),
            "image_count": data.get('image_count', 0),
            "emoji_count": data.get('emoji_count', 0),
            "sticker_count": data.get('sticker_count', 0),
            "first_seen_utc": first_seen.isoformat() if first_seen else None,
            "last_seen_utc": last_seen.isoformat() if last_seen else None,
            "activity_span_seconds": round(activity_span_secs, 2),
        }

    # Populate Boosters
    for member in boosters:
         boost_duration_secs = 0
         if member.premium_since:
              try:
                  scan_end_time_aware = scan_end_time.astimezone(datetime.timezone.utc) if scan_end_time.tzinfo else scan_end_time.replace(tzinfo=datetime.timezone.utc)
                  premium_since_aware = member.premium_since.astimezone(datetime.timezone.utc) if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc)
                  if scan_end_time_aware >= premium_since_aware: boost_duration_secs = (scan_end_time_aware - premium_since_aware).total_seconds()
              except Exception: pass
         report_data["boosters"].append({
             "user_id": str(member.id), "username": member.name, "display_name": member.display_name,
             "boost_start_utc": member.premium_since.isoformat() if member.premium_since else None,
             "boost_duration_seconds": round(boost_duration_secs, 2),
         })

    # Populate Invites
    for inv in invites:
         report_data["invites"].append({
            "code": inv.code, "inviter_id": str(inv.inviter.id) if inv.inviter else None,
            "channel_id": str(inv.channel.id) if inv.channel else None,
            "created_at_utc": inv.created_at.isoformat() if inv.created_at else None,
            "expires_at_utc": inv.expires_at.isoformat() if inv.expires_at else None,
            "uses": inv.uses or 0, "max_uses": inv.max_uses or 0, "is_temporary": inv.temporary,
         })

    # Populate Webhooks
    for wh in webhooks:
        report_data["webhooks"].append({
            "id": str(wh.id), "name": wh.name, "creator_id": str(wh.user.id) if wh.user else None,
            "channel_id": str(wh.channel_id),
            "created_at_utc": wh.created_at.isoformat() if wh.created_at else None,
        })

    # Populate Integrations
    for integ in integrations:
         integ_type = integ.type if isinstance(integ.type, str) else integ.type.name
         report_data["integrations"].append({
             "id": str(integ.id), "name": integ.name, "type": integ_type, "enabled": integ.enabled,
             "syncing": integ.syncing if hasattr(integ, 'syncing') else None,
             "role_id": str(integ.role.id) if hasattr(integ, 'role') and integ.role else None,
             "expire_behaviour": integ.expire_behaviour.name if hasattr(integ, 'expire_behaviour') and integ.expire_behaviour else None,
             "expire_grace_period_seconds": integ.expire_grace_period if hasattr(integ, 'expire_grace_period') is not None else None,
             "account_id": str(integ.account.id) if integ.account else None,
             "account_name": integ.account.name if integ.account else None,
         })

    # Populate Audit Logs (Use the data retrieved from DB)
    if audit_logs:
         # Assume audit_logs are already sorted if needed, or sort here
         # audit_logs.sort(key=lambda x: x.get('created_at', datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)), reverse=True)
         for log_entry in audit_logs:
              created_at_dt = log_entry.get('created_at')
              created_at_iso = created_at_dt.isoformat() if isinstance(created_at_dt, datetime.datetime) else str(created_at_dt)
              report_data["audit_logs"].append({
                  "log_id": str(log_entry.get('log_id')),
                  "created_at_utc": created_at_iso,
                  "action_type": log_entry.get('action_type'),
                  "user_id": str(log_entry.get('user_id')) if log_entry.get('user_id') else None,
                  "target_id": str(log_entry.get('target_id')) if log_entry.get('target_id') else None,
                  "reason": log_entry.get('reason'),
                  "changes": log_entry.get('extra_data') # Keep the dict from DB
              })

    # Populate Permission Audit
    report_data["permission_audit"] = permission_results # Already structured

    # Populate Top Oldest Members
    if oldest_members_data:
         for data in oldest_members_data:
             joined_at = data.get('joined_at')
             report_data["top_oldest_members"].append({
                 "user_id": str(data.get('id')),
                 "display_name": data.get('display_name'),
                 "joined_at_utc": joined_at.isoformat() if isinstance(joined_at, datetime.datetime) else None,
             })

    # Populate Role Change Stats (By Mod)
    if role_change_stats:
         for role_id_str, stats in role_change_stats.items():
              report_data["role_change_stats_by_mod"][role_id_str] = {
                  "added_by_mod": {str(mod_id): count for mod_id, count in stats["added"].items()},
                  "removed_by_mod": {str(mod_id): count for mod_id, count in stats["removed"].items()}
              }

    # Populate Role Change Stats (For User)
    if user_role_changes:
         for user_id, role_stats in user_role_changes.items():
              report_data["role_change_stats_for_user"][str(user_id)] = {
                  role_id_str: {
                      "added": changes.get("added", 0),
                      "removed": changes.get("removed", 0)
                  } for role_id_str, changes in role_stats.items()
              }

    # Populate Keyword Analysis
    if target_keywords and keyword_counts is not None:
         report_data["keyword_analysis"] = {
             "target_keywords": target_keywords,
             "overall_counts": dict(keyword_counts),
             "by_channel": {str(cid): dict(counts) for cid, counts in channel_keyword_counts.items()} if channel_keyword_counts else {},
             "by_thread": {str(tid): dict(counts) for tid, counts in thread_keyword_counts.items()} if thread_keyword_counts else {},
             "by_user": {str(uid): dict(counts) for uid, counts in user_keyword_counts.items()} if user_keyword_counts else {}
         }

    # Populate Reaction Analysis
    if reaction_emoji_counts:
         report_data["reaction_analysis"] = {
             "total_reactions_scanned": server_info.get('reaction_count_overall'),
             "emoji_counts": dict(reaction_emoji_counts) # Store as dict
         }

    # Populate Invite Usage
    if invite_usage_counts:
         report_data["invite_usage_by_inviter"] = {
             str(inviter_id): count for inviter_id, count in invite_usage_counts.items()
         }

    # Populate New Leaderboards
    if user_link_counts: report_data["user_link_counts"] = {str(k): v for k,v in user_link_counts.items()}
    if user_image_counts: report_data["user_image_counts"] = {str(k): v for k,v in user_image_counts.items()}
    if user_emoji_counts: report_data["user_emoji_counts"] = {str(k): v for k,v in user_emoji_counts.items()}
    if user_sticker_counts: report_data["user_sticker_counts"] = {str(k): v for k,v in user_sticker_counts.items()}


    # --- Convert to JSON string ---
    try:
        # Use default=str to handle potential non-serializable types like datetime
        json_string = json.dumps(report_data, indent=2, ensure_ascii=False, default=str)
        bytes_output = io.BytesIO(json_string.encode('utf-8'))
        return discord.File(bytes_output, filename="server_report.json")
    except TypeError as e:
        log.error(f"{e_json('error')} Không thể serialize dữ liệu báo cáo sang JSON: {e}", exc_info=True)
        try:
            # Attempt to find the problematic data section for logging
            problematic_part = {}
            for k, v in report_data.items():
                try: json.dumps({k: v}, default=str) # Test serialization per key
                except TypeError:
                    preview = repr(v) # Get representation
                    if len(preview) > 200: preview = preview[:200] + "..." # Truncate long previews
                    problematic_part[k] = f"Type: {type(v).__name__}, Preview: {preview}"
            log.error(f"Phần dữ liệu có thể gây lỗi JSON: {problematic_part}")
        except Exception as dump_err:
             log.error(f"Lỗi khi cố gắng tìm phần dữ liệu JSON lỗi: {dump_err}")
        return None
    except Exception as ex:
        log.error(f"{e_json('error')} LỖI tạo báo cáo JSON: {ex}", exc_info=True) # [Dịch]
        return None

# --- END OF FILE reporting.py ---

