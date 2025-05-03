# --- START OF FILE reporting/json_writer.py ---
import discord
import datetime
import io
import json
import logging
import collections
from typing import List, Dict, Any, Optional, Union, Set, Tuple
from collections import Counter, defaultdict

try:
    from .. import utils
    from .. import config
except ImportError:
    import utils
    import config

log = logging.getLogger(__name__)

def _default_serializer(obj):
    if isinstance(obj, datetime.datetime): return obj.isoformat()
    if isinstance(obj, datetime.timedelta): return obj.total_seconds()
    if isinstance(obj, (discord.Object, discord.abc.Snowflake)): return str(obj.id)
    if isinstance(obj, discord.Color): return str(obj)
    if isinstance(obj, discord.Permissions): return obj.value
    if isinstance(obj, (collections.Counter, collections.defaultdict)): return dict(obj)
    if isinstance(obj, set): return list(obj)
    try: return repr(obj)
    except Exception: return f"<Unserializable type: {type(obj).__name__}>"


async def create_json_report(
    server: discord.Guild, bot: discord.Client, server_info: Dict[str, Any],
    channel_details: List[Dict[str, Any]], voice_channel_static_data: List[Dict[str, Any]],
    user_activity: Dict[int, Dict[str, Any]], roles: List[discord.Role], boosters: List[discord.Member],
    invites: List[discord.Invite], webhooks: List[discord.Webhook], integrations: List[discord.Integration],
    audit_logs: List[Dict[str, Any]], scan_timestamp: datetime.datetime,
    oldest_members_data: Optional[List[Dict[str, Any]]] = None,
    user_thread_creation_counts: Optional[collections.Counter] = None,
    tracked_role_grant_counts: Optional[collections.Counter] = None,
    user_distinct_channel_counts: Optional[collections.Counter] = None,
    user_total_custom_emoji_content_counts: Optional[collections.Counter] = None,
    keyword_totals: Optional[collections.Counter] = None,
    keyword_by_channel: Optional[Dict[int, collections.Counter]] = None,
    keyword_by_thread: Optional[Dict[int, collections.Counter]] = None,
    keyword_by_user: Optional[Dict[int, collections.Counter]] = None,
    keywords_searched: Optional[List[str]] = None,
    filtered_reaction_emoji_counts: Optional[collections.Counter] = None,
    sticker_usage_counts: Optional[collections.Counter] = None,
    invite_usage_counts: Optional[collections.Counter] = None,
    user_link_counts: Optional[collections.Counter] = None,
    user_image_counts: Optional[collections.Counter] = None,
    user_emoji_counts: Optional[collections.Counter] = None,
    user_sticker_counts: Optional[collections.Counter] = None,
    user_mention_given_counts: Optional[collections.Counter] = None,
    user_mention_received_counts: Optional[collections.Counter] = None,
    user_reply_counts: Optional[collections.Counter] = None,
    user_reaction_received_counts: Optional[collections.Counter] = None,
    user_other_file_counts: Optional[collections.Counter] = None,
    user_most_active_channel: Optional[Dict[int, Tuple[int, int]]] = None, # <<< THÊM MỚI
) -> Optional[discord.File]:
    log.info("📄 Đang tạo báo cáo JSON...")
    report_data = {
        "report_metadata": {
            "report_generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "scan_end_time_utc": scan_timestamp.isoformat(),
            "bot_name": getattr(bot.user, 'name', 'Unknown Bot'),
            "bot_id": str(getattr(bot.user, 'id', 0)),
            "report_schema_version": "1.2", # Bump version
        },
        "server_info": {
            "id": str(server.id), "name": server.name, "owner_id": str(server.owner_id) if server.owner_id else None,
            "created_at_utc": server.created_at.isoformat(), "member_count_cache": server.member_count,
            "scan_start_users": server_info.get('member_count_real'), "scan_start_bots": server_info.get('bot_count'),
            "boost_tier": server.premium_tier, "boost_count": server.premium_subscription_count,
            "verification_level": str(server.verification_level), "explicit_content_filter": str(server.explicit_content_filter),
            "mfa_level": str(server.mfa_level), "default_notifications": str(server.default_notifications),
            "system_channel_id": str(server.system_channel.id) if server.system_channel else None,
            "rules_channel_id": str(server.rules_channel.id) if server.rules_channel else None,
            "public_updates_channel_id": str(server.public_updates_channel.id) if server.public_updates_channel else None,
            "afk_channel_id": str(server.afk_channel.id) if server.afk_channel else None,
            "afk_timeout_seconds": server.afk_timeout, "total_roles_scan": len(roles),
            "total_custom_emojis": len(server.emojis), "total_custom_stickers": len(server.stickers),
            "total_reactions_scanned_filtered": sum(filtered_reaction_emoji_counts.values()) if filtered_reaction_emoji_counts else None,
            "features": server.features,
        },
        "scanned_channels_and_threads": [],
        "static_voice_stage_channels": [],
        "roles": [],
        "user_activity_detail": {},
        "boosters": [],
        "invites": [],
        "webhooks": [],
        "integrations": [],
        "audit_logs": [],
        "leaderboards": {},
        "top_oldest_members": None,
        "tracked_role_grants": None,
        "keyword_analysis": None,
    }

    # --- Điền dữ liệu ---
    for detail in channel_details:
        channel_json = detail.copy(); channel_json['id'] = str(detail.get('id'))
        channel_json['category_id'] = str(detail.get('category_id')) if detail.get('category_id') else None
        channel_json['duration_seconds'] = detail.get('duration_seconds')
        channel_json.pop('duration', None); channel_json.pop('channel_obj', None)
        if 'threads_data' in channel_json and isinstance(channel_json['threads_data'], list):
            for thread in channel_json['threads_data']:
                thread['id'] = str(thread.get('id')); thread['owner_id'] = str(thread.get('owner_id')) if thread.get('owner_id') else None
                thread['scan_duration_seconds'] = thread.get('scan_duration_seconds'); thread.pop('scan_duration', None)
        report_data["scanned_channels_and_threads"].append(channel_json)

    for vc in voice_channel_static_data:
        report_data["static_voice_stage_channels"].append({"id": str(vc.get('id')), "name": vc.get('name'), "type": vc.get('type'), "category_id": str(vc.get('category_id')) if vc.get('category_id') else None, "category_name": vc.get('category'), "created_at_utc": vc.get('created_at'), "user_limit": vc.get('user_limit'), "bitrate_bps": utils.parse_bitrate(str(vc.get('bitrate', '0')))})

    report_data["roles"] = [{"id": str(r.id), "name": r.name, "position": r.position, "color_hex": str(r.color), "is_hoisted": r.hoist, "is_mentionable": r.mentionable, "is_bot_managed": r.is_bot_managed(), "member_count_scan_end": len(r.members), "created_at_utc": r.created_at, "permissions_value": r.permissions.value} for r in roles]

    for user_id, data in user_activity.items():
        most_active_data = user_most_active_channel.get(user_id) if user_most_active_channel else None # <<< LẤY DỮ LIỆU
        user_json_data = {
            "is_bot": data.get('is_bot'), "message_count": data.get('message_count'),
            "link_count": data.get('link_count'), "image_count": data.get('image_count'),
            "other_file_count": data.get('other_file_count'),
            "emoji_content_count": data.get('emoji_count'),
            "custom_emoji_server_content_count": sum(user_total_custom_emoji_content_counts.get(user_id, {}).values()) if user_total_custom_emoji_content_counts else 0,
            "sticker_sent_count": data.get('sticker_count'),
            "mention_given_count": data.get('mention_given_count'),
            "distinct_mention_given_count": len(data.get('distinct_mentions_set', set())),
            "mention_received_count": data.get('mention_received_count'),
            "reply_count": data.get('reply_count'), "reaction_received_count": data.get('reaction_received_count'),
            "distinct_channels_messaged": len(data.get('channels_messaged_in', set())),
            "most_active_location": { # <<< THÊM VÀO JSON
                "location_id": str(most_active_data[0]) if most_active_data else None,
                "message_count": most_active_data[1] if most_active_data else 0
            },
            "first_seen_utc": data.get('first_seen'), "last_seen_utc": data.get('last_seen'),
            "activity_span_seconds": data.get('activity_span_seconds'),
        }
        report_data["user_activity_detail"][str(user_id)] = user_json_data

    for member in boosters:
        boost_duration_secs = 0
        if member.premium_since:
            try:
                since_aware = member.premium_since.astimezone(datetime.timezone.utc) if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc); scan_aware = scan_timestamp.astimezone(datetime.timezone.utc) if scan_timestamp.tzinfo else scan_timestamp.replace(tzinfo=datetime.timezone.utc);
                if scan_aware >= since_aware: boost_duration_secs = (scan_aware - since_aware).total_seconds()
            except Exception: pass
        report_data["boosters"].append({"user_id": str(member.id), "username": member.name, "display_name": member.display_name, "boost_start_utc": member.premium_since, "boost_duration_seconds": round(boost_duration_secs, 2) if boost_duration_secs >= 0 else 0})

    report_data["invites"] = [{"code": inv.code, "inviter_id": str(inv.inviter.id) if inv.inviter else None, "channel_id": str(inv.channel.id) if inv.channel else None, "created_at_utc": inv.created_at, "expires_at_utc": inv.expires_at, "uses": inv.uses or 0, "max_uses": inv.max_uses or 0, "is_temporary": inv.temporary} for inv in invites]
    report_data["webhooks"] = [{"id": str(wh.id), "name": wh.name, "creator_id": str(wh.user.id) if wh.user else None, "channel_id": str(wh.channel_id), "created_at_utc": wh.created_at, "url": wh.url} for wh in webhooks]
    for integ in integrations: integ_type = integ.type if isinstance(integ.type, str) else integ.type.name; report_data["integrations"].append({"id": str(integ.id), "name": integ.name, "type": integ_type, "enabled": integ.enabled, "syncing": getattr(integ, 'syncing', None), "role_id": str(integ.role.id) if hasattr(integ, 'role') and integ.role else None, "expire_behaviour": getattr(integ.expire_behaviour, 'name', None) if hasattr(integ, 'expire_behaviour') else None, "expire_grace_period_seconds": getattr(integ, 'expire_grace_period', None), "account_id": str(integ.account.id) if integ.account else None, "account_name": integ.account.name if integ.account else None, "application_id": str(integ.application.id) if hasattr(integ, 'application') and integ.application else None})

    report_data["audit_logs"] = audit_logs

    lb = report_data["leaderboards"]
    lb["link_posters"] = user_link_counts; lb["image_posters"] = user_image_counts; lb["other_file_posters"] = user_other_file_counts
    lb["custom_emoji_server_content_users"] = user_total_custom_emoji_content_counts; lb["sticker_senders"] = user_sticker_counts
    lb["mention_givers"] = user_mention_given_counts; lb["mention_receivers"] = user_mention_received_counts
    lb["repliers"] = user_reply_counts; lb["reaction_receivers"] = user_reaction_received_counts
    lb["invite_creators_by_uses"] = invite_usage_counts; lb["filtered_emoji_reaction_usage"] = filtered_reaction_emoji_counts
    lb["sticker_id_usage"] = sticker_usage_counts; lb["thread_creators"] = user_thread_creation_counts
    lb["distinct_channel_users"] = user_distinct_channel_counts

    if oldest_members_data: report_data["top_oldest_members"] = [{"user_id": str(d.get('id')), "display_name": d.get('display_name'), "joined_at_utc": d.get('joined_at')} for d in oldest_members_data]
    if tracked_role_grant_counts:
        # Chuyển đổi Counter với key tuple thành cấu trúc dict lồng nhau nếu muốn
        grants_by_user = defaultdict(dict)
        for (user_id, role_id), count in tracked_role_grant_counts.items():
            grants_by_user[str(user_id)][str(role_id)] = count
        report_data["tracked_role_grants"] = grants_by_user
    else:
        report_data["tracked_role_grants"] = None
    if keywords_searched: report_data["keyword_analysis"] = {"keywords_searched": keywords_searched, "overall_counts": keyword_totals or {}, "by_channel": {str(k): v for k, v in keyword_by_channel.items()} if keyword_by_channel else {}, "by_thread": {str(k): v for k, v in keyword_by_thread.items()} if keyword_by_thread else {}, "by_user": {str(k): v for k, v in keyword_by_user.items()} if keyword_by_user else {}}

    try:
        json_string = json.dumps(report_data, indent=2, ensure_ascii=False, default=_default_serializer)
        bytes_output = io.BytesIO(json_string.encode('utf-8'))
        filename = f"shiromi_report_{server.id}_{scan_timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        return discord.File(bytes_output, filename=filename)
    except TypeError as type_err:
        log.error(f"‼️ LỖI TypeError khi tạo JSON: {type_err}", exc_info=True)
        try:
            problematic_part = {}
            for k, v in report_data.items():
                try: json.dumps({k: v}, default=_default_serializer)
                except TypeError: problematic_part[k] = f"Type: {type(v).__name__}, Value Sample: {repr(v)[:100]}"
            log.error(f"Phần dữ liệu có thể gây lỗi JSON (kiểm tra serializer): {problematic_part}")
        except Exception: pass
        return None
    except Exception as ex:
        log.error(f"‼️ LỖI không xác định khi tạo báo cáo JSON: {ex}", exc_info=True)
        return None

# --- END OF FILE reporting/json_writer.py ---