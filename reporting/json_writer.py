# --- START OF FILE reporting/json_writer.py ---
import discord
import datetime
import io
import json
import logging
import collections
from typing import List, Dict, Any, Optional, Union

# Relative import
try:
    from .. import utils
except ImportError:
    import utils

log = logging.getLogger(__name__)

# --- Hàm Helper Serialize ---
def _default_serializer(obj):
    """Hàm serializer tùy chỉnh cho json.dumps để xử lý các loại dữ liệu đặc biệt."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, datetime.timedelta):
        return obj.total_seconds()
    if isinstance(obj, (discord.Object, discord.abc.Snowflake)):
        return str(obj.id) # Chỉ lưu ID dưới dạng string
    if isinstance(obj, discord.Color):
        return str(obj) # Lưu mã hex màu
    if isinstance(obj, discord.Permissions):
        return obj.value # Lưu giá trị integer của permissions
    # Xử lý Counter thành dict
    if isinstance(obj, collections.Counter):
         return {str(k): v for k, v in obj.items()}
    # Xử lý defaultdict (cần cẩn thận hơn nếu giá trị phức tạp)
    if isinstance(obj, collections.defaultdict):
         return dict(obj) # Chuyển thành dict thường
    # Nếu không xử lý được, trả về repr hoặc báo lỗi
    try:
        return repr(obj) # Fallback cuối cùng là repr
    except Exception:
        return f"<Unserializable type: {type(obj).__name__}>"


# --- Hàm Chính Tạo Báo Cáo JSON ---
async def create_json_report(
    # Tham số giống create_csv_report
    server: discord.Guild, bot: discord.Client,
    server_info: Dict[str, Any],
    channel_details: List[Dict[str, Any]],
    voice_channel_static_data: List[Dict[str, Any]],
    user_activity: Dict[int, Dict[str, Any]],
    roles: List[discord.Role], boosters: List[discord.Member],
    invites: List[discord.Invite], webhooks: List[discord.Webhook], integrations: List[discord.Integration],
    audit_logs: List[Dict[str, Any]],
    permission_audit: Dict[str, List[Dict[str, Any]]],
    scan_timestamp: datetime.datetime,
    # Optional data
    oldest_members_data: Optional[List[Dict[str, Any]]] = None,
    role_change_stats: Optional[Dict[str, Dict[str, collections.Counter]]] = None,
    user_role_changes: Optional[Dict[int, Dict[str, Dict[str, int]]]] = None,
    user_thread_creation_counts: Optional[collections.Counter] = None,
    keyword_totals: Optional[collections.Counter] = None,
    keyword_by_channel: Optional[Dict[int, collections.Counter]] = None,
    keyword_by_thread: Optional[Dict[int, collections.Counter]] = None,
    keyword_by_user: Optional[Dict[int, collections.Counter]] = None,
    keywords_searched: Optional[List[str]] = None,
    # Counters
    reaction_emoji_counts: Optional[collections.Counter] = None,
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
) -> Optional[discord.File]:
    """
    Tạo file báo cáo JSON chứa tất cả dữ liệu quét và phân tích.
    Sử dụng serializer tùy chỉnh để xử lý các đối tượng Discord và datetime.
    """
    log.info("📄 Đang tạo báo cáo JSON...")
    report_data = {
        "report_metadata": {
            "report_generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "scan_end_time_utc": scan_timestamp.isoformat(),
            "bot_name": getattr(bot.user, 'name', 'Unknown Bot'),
            "bot_id": str(getattr(bot.user, 'id', 0)),
        },
        "server_info": {
            "id": str(server.id),
            "name": server.name,
            "owner_id": str(server.owner_id) if server.owner_id else None,
            "created_at_utc": server.created_at.isoformat(),
            "member_count_cache": server.member_count,
            "scan_start_users": server_info.get('member_count_real'),
            "scan_start_bots": server_info.get('bot_count'),
            "boost_tier": server.premium_tier,
            "boost_count": server.premium_subscription_count,
            "verification_level": str(server.verification_level),
            "explicit_content_filter": str(server.explicit_content_filter),
            "mfa_level": str(server.mfa_level),
            "default_notifications": str(server.default_notifications),
            "system_channel_id": str(server.system_channel.id) if server.system_channel else None,
            "rules_channel_id": str(server.rules_channel.id) if server.rules_channel else None,
            "public_updates_channel_id": str(server.public_updates_channel.id) if server.public_updates_channel else None,
            "afk_channel_id": str(server.afk_channel.id) if server.afk_channel else None,
            "afk_timeout_seconds": server.afk_timeout,
            "total_roles_scan": len(roles),
            "total_emojis_scan": len(server.emojis),
            "total_stickers_scan": len(server.stickers),
            "total_reactions_scanned": server_info.get('reaction_count_overall'),
            "features": server.features,
        },
        "scanned_channels_and_threads": [], # Sẽ điền bên dưới
        "static_voice_stage_channels": [], # Sẽ điền bên dưới
        "roles": [], # Sẽ điền bên dưới
        "user_activity_detail": {}, # Key là user_id (string)
        "boosters": [], # Sẽ điền bên dưới
        "invites": [], # Sẽ điền bên dưới
        "webhooks": [], # Sẽ điền bên dưới
        "integrations": [], # Sẽ điền bên dưới
        "audit_logs": [], # Sẽ điền bên dưới (dữ liệu từ DB)
        "permission_audit": permission_audit or {}, # Kết quả phân tích quyền
        "leaderboards": {}, # Chứa các counter leaderboard
        "top_oldest_members": None, # Sẽ điền bên dưới
        "role_change_stats": None, # Sẽ điền bên dưới (gồm by_mod và for_user)
        "keyword_analysis": None, # Sẽ điền bên dưới
    }

    # --- Điền dữ liệu vào cấu trúc report_data ---

    # Kênh và Luồng đã quét
    for detail in channel_details:
        # Tạo bản sao để tránh thay đổi dict gốc trong scan_data
        channel_json = detail.copy()
        # Chuyển đổi các giá trị cần thiết
        channel_json['id'] = str(detail.get('id'))
        channel_json['category_id'] = str(detail.get('category_id')) if detail.get('category_id') else None
        # Xóa các object không cần thiết hoặc khó serialize
        channel_json.pop('duration', None) # Đã có scan_duration_seconds
        channel_json.pop('channel_obj', None) # Không cần object kênh trong JSON

        if 'threads_data' in channel_json and isinstance(channel_json['threads_data'], list):
            for thread in channel_json['threads_data']:
                thread['id'] = str(thread.get('id'))
                thread['owner_id'] = str(thread.get('owner_id')) if thread.get('owner_id') else None
                # Chuyển đổi timedelta nếu có
                if 'scan_duration' in thread and isinstance(thread['scan_duration'], datetime.timedelta):
                     thread['scan_duration_seconds'] = thread['scan_duration'].total_seconds()
                     del thread['scan_duration']

        report_data["scanned_channels_and_threads"].append(channel_json)

    # Kênh Voice/Stage tĩnh
    for vc in voice_channel_static_data:
        report_data["static_voice_stage_channels"].append({
            "id": str(vc.get('id')),
            "name": vc.get('name'),
            "type": vc.get('type'),
            "category_id": str(vc.get('category_id')) if vc.get('category_id') else None,
            "category_name": vc.get('category'),
            "created_at_utc": vc.get('created_at'), # Sẽ được xử lý bởi serializer
            "user_limit": vc.get('user_limit'),
            "bitrate_bps": utils.parse_bitrate(str(vc.get('bitrate', '0'))), # Lưu dưới dạng số bps
        })

    # Roles
    report_data["roles"] = [{
        "id": str(role.id), "name": role.name, "position": role.position,
        "color_hex": str(role.color), "is_hoisted": role.hoist, "is_mentionable": role.mentionable,
        "is_bot_managed": role.is_bot_managed(),
        "member_count_scan_end": len(role.members), # Số member lúc quét xong
        "created_at_utc": role.created_at,
        "permissions_value": role.permissions.value,
    } for role in roles]

    # Hoạt động User
    for user_id, data in user_activity.items():
        # Tính span trước khi serialize
        first_seen = data.get('first_seen')
        last_seen = data.get('last_seen')
        activity_span_secs = 0
        if first_seen and last_seen and last_seen >= first_seen:
            try: activity_span_secs = (last_seen - first_seen).total_seconds()
            except Exception: pass

        # Tạo dict mới cho user, chỉ lấy các trường cần thiết
        user_json_data = {
            "is_bot": data.get('is_bot'),
            "message_count": data.get('message_count'),
            "link_count": data.get('link_count'),
            "image_count": data.get('image_count'),
            "emoji_content_count": data.get('emoji_count'),
            "sticker_sent_count": data.get('sticker_count'),
            "mention_given_count": data.get('mention_given_count'),
            "mention_received_count": data.get('mention_received_count'),
            "reply_count": data.get('reply_count'),
            "reaction_received_count": data.get('reaction_received_count'),
            "first_seen_utc": data.get('first_seen'), # Datetime object
            "last_seen_utc": data.get('last_seen'),   # Datetime object
            "activity_span_seconds": round(activity_span_secs, 2),
        }
        report_data["user_activity_detail"][str(user_id)] = user_json_data


    # Boosters
    for member in boosters:
        boost_duration_secs = 0
        if member.premium_since:
            try:
                since_aware = member.premium_since.astimezone(datetime.timezone.utc) if member.premium_since.tzinfo else member.premium_since.replace(tzinfo=datetime.timezone.utc)
                scan_aware = scan_timestamp.astimezone(datetime.timezone.utc) if scan_timestamp.tzinfo else scan_timestamp.replace(tzinfo=datetime.timezone.utc)
                if scan_aware >= since_aware:
                    boost_duration_secs = (scan_aware - since_aware).total_seconds()
            except Exception: pass
        report_data["boosters"].append({
            "user_id": str(member.id),
            "username": member.name,
            "display_name": member.display_name,
            "boost_start_utc": member.premium_since, # Datetime object
            "boost_duration_seconds": round(boost_duration_secs, 2) if boost_duration_secs >= 0 else 0,
        })

    # Invites
    report_data["invites"] = [{
        "code": inv.code,
        "inviter_id": str(inv.inviter.id) if inv.inviter else None,
        "channel_id": str(inv.channel.id) if inv.channel else None,
        "created_at_utc": inv.created_at, # Datetime object
        "expires_at_utc": inv.expires_at, # Datetime object or None
        "uses": inv.uses or 0,
        "max_uses": inv.max_uses or 0,
        "is_temporary": inv.temporary,
    } for inv in invites]

    # Webhooks
    report_data["webhooks"] = [{
        "id": str(wh.id),
        "name": wh.name,
        "creator_id": str(wh.user.id) if wh.user else None,
        "channel_id": str(wh.channel_id),
        "created_at_utc": wh.created_at, # Datetime object
        "url": wh.url # URL vẫn có thể hữu ích
    } for wh in webhooks]

    # Integrations
    for integ in integrations:
        integ_type = integ.type if isinstance(integ.type, str) else integ.type.name
        report_data["integrations"].append({
            "id": str(integ.id), "name": integ.name, "type": integ_type, "enabled": integ.enabled,
            "syncing": getattr(integ, 'syncing', None),
            "role_id": str(integ.role.id) if hasattr(integ, 'role') and integ.role else None,
            "expire_behaviour": getattr(integ.expire_behaviour, 'name', None) if hasattr(integ, 'expire_behaviour') else None,
            "expire_grace_period_seconds": getattr(integ, 'expire_grace_period', None),
            "account_id": str(integ.account.id) if integ.account else None,
            "account_name": integ.account.name if integ.account else None,
            # Thêm các trường khác nếu cần (ví dụ: application_id)
            "application_id": str(integ.application.id) if hasattr(integ, 'application') and integ.application else None,
        })

    # Audit Logs (đã là dict từ DB)
    # Chỉ cần chuyển đổi ID thành string nếu cần và đảm bảo datetime đúng
    report_data["audit_logs"] = audit_logs # Giả định audit_logs đã được xử lý đúng dạng

    # Leaderboards (Counter sẽ được xử lý bởi serializer)
    lb = report_data["leaderboards"]
    lb["link_posters"] = user_link_counts
    lb["image_posters"] = user_image_counts
    lb["emoji_content_users"] = user_emoji_counts
    lb["sticker_senders"] = user_sticker_counts
    lb["mention_givers"] = user_mention_given_counts
    lb["mention_receivers"] = user_mention_received_counts
    lb["repliers"] = user_reply_counts
    lb["reaction_receivers"] = user_reaction_received_counts
    lb["invite_creators_by_uses"] = invite_usage_counts
    lb["emoji_reaction_usage"] = reaction_emoji_counts
    lb["sticker_id_usage"] = sticker_usage_counts
    lb["thread_creators"] = user_thread_creation_counts

    # Dữ liệu Phụ trợ
    if oldest_members_data:
        report_data["top_oldest_members"] = [{
            "user_id": str(d.get('id')),
            "display_name": d.get('display_name'),
            "joined_at_utc": d.get('joined_at') # Datetime object
        } for d in oldest_members_data]

    if role_change_stats or user_role_changes:
        report_data["role_change_stats"] = {
            "by_moderator": role_change_stats or {}, # Counter sẽ được serialize
            "for_user": {str(uid): stats for uid, stats in user_role_changes.items()} if user_role_changes else {}
        }

    if keywords_searched:
        report_data["keyword_analysis"] = {
            "keywords_searched": keywords_searched,
            "overall_counts": keyword_totals or {}, # Counter
            "by_channel": {str(k): v for k, v in keyword_by_channel.items()} if keyword_by_channel else {}, # Counter
            "by_thread": {str(k): v for k, v in keyword_by_thread.items()} if keyword_by_thread else {}, # Counter
            "by_user": {str(k): v for k, v in keyword_by_user.items()} if keyword_by_user else {} # Counter
        }


    # --- Tạo file JSON ---
    try:
        json_string = json.dumps(
            report_data,
            indent=2, # Định dạng đẹp với thụt lề 2 spaces
            ensure_ascii=False, # Đảm bảo hiển thị đúng tiếng Việt
            default=_default_serializer # Sử dụng serializer tùy chỉnh
        )
        bytes_output = io.BytesIO(json_string.encode('utf-8'))
        # Đặt tên file rõ ràng hơn
        filename = f"shiromi_report_{server.id}_{scan_timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        return discord.File(bytes_output, filename=filename)
    except TypeError as type_err:
        log.error(f"‼️ LỖI TypeError khi tạo JSON: {type_err}", exc_info=True)
        # Cố gắng tìm ra phần dữ liệu gây lỗi
        try:
            problematic_part = {}
            for k, v in report_data.items():
                try:
                    json.dumps({k: v}, default=_default_serializer)
                except TypeError:
                    problematic_part[k] = f"Type: {type(v).__name__}, Value Sample: {repr(v)[:100]}"
            log.error(f"Phần dữ liệu có thể gây lỗi JSON (kiểm tra serializer): {problematic_part}")
        except Exception: pass
        return None
    except Exception as ex:
        log.error(f"‼️ LỖI không xác định khi tạo báo cáo JSON: {ex}", exc_info=True)
        return None

# --- END OF FILE reporting/json_writer.py ---