# --- START OF FILE database.py ---
import asyncpg
import os
import datetime
import json # Để xử lý JSONB
from typing import Optional, Dict, Any, List, Union
import logging
import discord # Cần cho type hint AuditLogEntry
import discord.enums # <<< Thêm import này để chắc chắn
import asyncio

# Cấu hình logging cơ bản (sẽ được RichHandler ghi đè nếu dùng trong bot.py)
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
pool: Optional[asyncpg.Pool] = None # Khởi tạo pool là None

async def connect_db() -> Optional[asyncpg.Pool]:
    """Thiết lập kết nối cơ sở dữ liệu."""
    global pool
    if pool is None and DATABASE_URL:
        try:
            log.info("Đang tạo nhóm kết nối cơ sở dữ liệu...")
            pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=2,
                max_size=10,
                init=__set_json_codec, # Đặt codec khi kết nối được tạo
                command_timeout=60
            )
            log.info("Đã thiết lập nhóm kết nối cơ sở dữ liệu.") # [Dịch]
            await setup_tables() # Đảm bảo bảng tồn tại sau khi kết nối
        except (asyncpg.exceptions.InvalidCatalogNameError, OSError, ConnectionRefusedError, asyncpg.exceptions.CannotConnectNowError) as e:
            log.critical(f"CRITICAL: Không thể kết nối tới cơ sở dữ liệu: {e}. Kiểm tra DATABASE_URL và trạng thái DB.") # [Dịch]
            pool = None
        except Exception as e:
            log.critical(f"CRITICAL: Lỗi không xác định trong quá trình kết nối DB: {e}", exc_info=True) # [Dịch]
            pool = None
    elif not DATABASE_URL:
        log.critical("CRITICAL: Biến môi trường DATABASE_URL chưa được đặt. Tính năng DB bị vô hiệu hóa.") # [Dịch]
        pool = None
    return pool

async def close_db():
    """Đóng nhóm kết nối cơ sở dữ liệu."""
    global pool
    if pool:
        log.info("Đang đóng nhóm kết nối cơ sở dữ liệu...")
        try:
            # Use asyncio.wait_for to apply a timeout to the close operation
            await asyncio.wait_for(pool.close(), timeout=10.0)
            log.info("Nhóm kết nối cơ sở dữ liệu đã được đóng.")
        except asyncio.TimeoutError:
            log.warning("Quá thời gian chờ đóng nhóm kết nối DB. Buộc đóng.")
            # Force close the pool if timeout occurs
            pool.terminate()
        except Exception as e:
            log.error(f"Lỗi khi đóng nhóm kết nối DB: {e}")
        finally:
            pool = None


async def __set_json_codec(conn):
    """Hàm trợ giúp để đặt codec JSON/JSONB cho asyncpg."""
    log.debug(f"Đang đặt codec JSON/JSONB cho kết nối {conn}")
    try:
        await conn.set_type_codec(
            'jsonb',
            encoder=lambda v: json.dumps(v, default=str), # Dùng default=str để xử lý datetime, etc.
            decoder=json.loads,
            schema='pg_catalog',
            format='text'
        )
        await conn.set_type_codec(
            'json',
            encoder=lambda v: json.dumps(v, default=str),
            decoder=json.loads,
            schema='pg_catalog',
            format='text'
        )
        log.debug(f"Đặt codec JSON/JSONB thành công cho kết nối {conn}")
    except Exception as e:
        log.error(f"Không thể đặt codec JSON cho kết nối {conn}: {e}", exc_info=True) # [Dịch]


async def setup_tables():
    """Tạo các bảng cần thiết nếu chúng chưa tồn tại."""
    if not pool:
        log.error("Không thể thiết lập bảng, nhóm DB không khả dụng.") # [Dịch]
        return
    log.info("Đang kiểm tra/tạo bảng cơ sở dữ liệu...")
    try:
        async with pool.acquire() as conn:

            await conn.execute("""
                 CREATE TABLE IF NOT EXISTS audit_log_cache (
                     log_id BIGINT PRIMARY KEY,
                     guild_id BIGINT NOT NULL,
                     user_id BIGINT,
                     target_id BIGINT,
                     action_type TEXT NOT NULL,
                     reason TEXT,
                     created_at TIMESTAMPTZ NOT NULL,
                     extra_data JSONB -- Sử dụng JSONB cho hiệu năng tốt hơn
                 );
                 CREATE INDEX IF NOT EXISTS idx_audit_log_guild_time ON audit_log_cache (guild_id, created_at DESC);
                 CREATE INDEX IF NOT EXISTS idx_audit_log_action_type ON audit_log_cache (guild_id, action_type);
                 CREATE INDEX IF NOT EXISTS idx_audit_log_target_id ON audit_log_cache (target_id); -- Index target_id nếu thường xuyên query
             """)
            # Bảng guild_metadata (lưu ID audit log cuối cùng)
            await conn.execute("""
                 CREATE TABLE IF NOT EXISTS guild_metadata (
                     guild_id BIGINT PRIMARY KEY,
                     last_audit_log_id BIGINT,
                     last_audit_scan_time TIMESTAMPTZ
                 );
             """)
            log.info("Kiểm tra/Tạo bảng cơ sở dữ liệu thành công (Bảng scan state đã bị loại bỏ).") # [Dịch]
    except Exception as e:
        log.error(f"Lỗi khi thiết lập bảng cơ sở dữ liệu: {e}", exc_info=True) # [Dịch]


def _serialize_value(value: Any) -> Any:
    """Hàm trợ giúp để serialize các giá trị riêng lẻ trong AuditLogChanges một cách an toàn cho JSON."""
    if isinstance(value, (str, int, bool, float)) or value is None:
        return value
    elif isinstance(value, discord.Role):
        return {'id': str(value.id), 'name': value.name, 'type': 'role'}
    elif isinstance(value, (discord.Member, discord.User)):
        # Lấy cả name và display_name
        display = getattr(value, 'display_name', value.name)
        return {'id': str(value.id), 'name': value.name, 'display_name': display, 'discriminator': value.discriminator, 'bot': value.bot, 'type': 'user'}
    elif isinstance(value, discord.abc.GuildChannel): # Bao gồm TextChannel, VoiceChannel, CategoryChannel, StageChannel, ForumChannel
        return {'id': str(value.id), 'name': value.name, 'type': str(value.type)}
    elif isinstance(value, discord.Thread):
         return {'id': str(value.id), 'name': value.name, 'parent_id': str(value.parent_id), 'type': str(value.type)}
    elif isinstance(value, discord.Invite): # Thêm xử lý Invite
        return {'code': value.code, 'inviter_id': str(value.inviter.id) if value.inviter else None, 'channel_id': str(value.channel.id) if value.channel else None, 'uses': value.uses, 'type': 'invite'}
    elif isinstance(value, discord.Asset):
        return {'url': str(value), 'key': value.key, 'type': 'asset'}
    elif isinstance(value, datetime.datetime):
        return {'iso': value.isoformat(), 'type': 'datetime'}
    elif isinstance(value, datetime.timedelta): # Xử lý timedelta
        return {'total_seconds': value.total_seconds(), 'type': 'timedelta'}
    elif isinstance(value, discord.Colour):
        return {'value': value.value, 'hex': str(value), 'type': 'color'}
    elif isinstance(value, discord.Permissions):
        # Lấy danh sách tên các quyền được bật
        perm_names = [name for name, enabled in iter(value) if enabled]
        return {'value': value.value, 'names': perm_names, 'type': 'permissions'}
    elif isinstance(value, list):
        # Xử lý đệ quy cho list
        return [_serialize_value(item) for item in value]
    elif isinstance(value, discord.enums.Enum): # Xử lý Enum tổng quát hơn
        try:
            # Cố gắng lấy name và value nếu có
            return {'name': value.name, 'value': value.value, 'type': type(value).__name__}
        except AttributeError:
            # Nếu không có name/value, dùng repr()
             return {'repr': repr(value), 'type': type(value).__name__}
    elif isinstance(value, discord.Object): # Xử lý discord.Object
        return {'id': str(value.id), 'type': 'object'}
    else:
        # Fallback cho các loại không xác định khác
        try:
            # Nếu là Snowflake, cố gắng lấy ID
            if isinstance(value, discord.abc.Snowflake):
                 return {'id': str(value.id), 'repr': repr(value), 'type': type(value).__name__}
            else:
                 # Trường hợp cuối cùng, chỉ dùng repr
                 return {'repr': repr(value), 'type': str(type(value).__name__)}
        except Exception:
            # Ghi log debug nếu có lỗi serialize không mong muốn
            log.debug(f"Không thể serialize đối tượng loại {type(value).__name__}: {value}")
            return {'repr': 'Unserializable Object', 'type': str(type(value).__name__)}

def _serialize_changes(changes: discord.AuditLogChanges) -> Optional[Dict[str, Any]]:
    """Serialize AuditLogChanges thành một dict tương thích JSON chi tiết."""
    if not changes:
        return None
    data = {'before': {}, 'after': {}}
    # Lấy tất cả các thuộc tính có thể có trong before/after một cách an toàn
    all_keys = set()
    if changes.before is not None:
        all_keys.update(k for k in dir(changes.before) if not k.startswith('_'))
    if changes.after is not None:
        all_keys.update(k for k in dir(changes.after) if not k.startswith('_'))

    keys_with_changes = set()
    for attr in all_keys:
        try:
            before_val = getattr(changes.before, attr, None) if changes.before is not None else None
            after_val = getattr(changes.after, attr, None) if changes.after is not None else None
            # Chỉ bao gồm nếu có sự thay đổi thực sự VÀ không phải là phương thức
            if before_val != after_val and not callable(before_val) and not callable(after_val):
                 data['before'][attr] = _serialize_value(before_val)
                 data['after'][attr] = _serialize_value(after_val)
                 keys_with_changes.add(attr)
        except Exception as e:
             # Ghi log nếu có lỗi khi truy cập hoặc so sánh thuộc tính
             log.debug(f"Lỗi khi serialize thuộc tính '{attr}' trong audit log changes: {e}")
             continue # Bỏ qua thuộc tính này

    # Chỉ trả về data nếu có ít nhất một thay đổi được ghi nhận
    return data if keys_with_changes else None


async def add_audit_log_entry(log_entry: discord.AuditLogEntry):
    """Thêm hoặc cập nhật một entry audit log trong cache."""
    if not pool or not log_entry:
        # [Dịch]
        # log.debug(f"Bỏ qua thêm entry audit log: Pool={pool is not None}, Entry={log_entry is not None}")
        return
    try:
        async with pool.acquire() as conn:
            user_id = log_entry.user.id if log_entry.user else None

            # --- Xử lý target_id linh hoạt ---
            target_id: Optional[int] = None
            target_obj = log_entry.target
            try:
                if isinstance(target_obj, discord.abc.Snowflake):
                    target_id = target_obj.id
                elif isinstance(target_obj, dict) and 'id' in target_obj:
                    target_id = int(target_obj['id'])
                elif isinstance(target_obj, str): # Ví dụ: Invite code trong invite_delete
                     # Thử chuyển invite code thành ID nếu là action invite_delete? Khó và không đáng tin cậy.
                     # Giữ target_id là None cho các trường hợp này.
                     log.debug(f"Audit log target là string: '{target_obj}' for action {log_entry.action}. target_id sẽ là NULL.")
                     target_id = None # Không lưu invite code vào target_id (INT)
                # Bỏ qua các loại khác không có ID rõ ràng
            except (ValueError, TypeError, AttributeError) as e:
                 log.warning(f"Không thể trích xuất ID từ audit log target type {type(target_obj)} (Value: {target_obj}, Action: {log_entry.action}): {e}")
                 target_id = None
            # --- Kết thúc xử lý target_id ---

            # Serialize changes bằng hàm đã cải thiện
            extra_data = _serialize_changes(log_entry.changes)

            query = """
                INSERT INTO audit_log_cache
                    (log_id, guild_id, user_id, target_id, action_type, reason, created_at, extra_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (log_id) DO UPDATE SET
                    guild_id = EXCLUDED.guild_id, user_id = EXCLUDED.user_id, target_id = EXCLUDED.target_id,
                    action_type = EXCLUDED.action_type, reason = EXCLUDED.reason, created_at = EXCLUDED.created_at,
                    extra_data = EXCLUDED.extra_data;
            """
            await conn.execute(
                query,
                log_entry.id, log_entry.guild.id, user_id, target_id, # Truyền target_id đã xử lý
                str(log_entry.action.name), log_entry.reason, log_entry.created_at,
                extra_data # Truyền dict đã serialize, asyncpg sẽ xử lý qua codec JSONB
            )
    except Exception as e:
        # [Dịch]
        log.error(f"Lỗi thêm entry audit log {log_entry.id} cho guild {log_entry.guild.id}: {e}", exc_info=False) # Giảm noise, chỉ log lỗi
        # Có thể log debug traceback nếu cần
        # log.debug(f"Traceback lỗi thêm audit log {log_entry.id}:", exc_info=True)


async def get_newest_audit_log_id_from_db(guild_id: int) -> Optional[int]:
    """Lấy ID của audit log mới nhất đã xử lý trước đó cho guild này từ guild_metadata."""
    if not pool: return None
    query = "SELECT last_audit_log_id FROM guild_metadata WHERE guild_id = $1"
    try:
        async with pool.acquire() as conn:
            result = await conn.fetchrow(query, guild_id)
            return result['last_audit_log_id'] if result and result['last_audit_log_id'] is not None else None
    except Exception as e:
        # [Dịch]
        log.error(f"Lỗi lấy ID audit log mới nhất cho guild {guild_id}: {e}", exc_info=False)
        return None

async def update_newest_audit_log_id(guild_id: int, newest_log_id: Optional[int]):
    """Cập nhật ID audit log mới nhất đã xử lý cho guild trong guild_metadata."""
    if not pool: return
    if newest_log_id is None:
         # [Dịch]
         log.debug(f"Bỏ qua cập nhật ID audit log mới nhất cho guild {guild_id} vì newest_log_id là None.")
         return

    now = datetime.datetime.now(datetime.timezone.utc)
    query = """
        INSERT INTO guild_metadata (guild_id, last_audit_log_id, last_audit_scan_time)
        VALUES ($1, $2, $3)
        ON CONFLICT (guild_id) DO UPDATE SET
            last_audit_log_id = EXCLUDED.last_audit_log_id,
            last_audit_scan_time = EXCLUDED.last_audit_scan_time;
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(query, guild_id, newest_log_id, now)
            log.info(f"Đã cập nhật ID audit log mới nhất cho guild {guild_id} thành {newest_log_id}") # [Dịch]
    except Exception as e:
        # [Dịch]
        log.error(f"Lỗi cập nhật ID audit log mới nhất cho guild {guild_id}: {e}", exc_info=False)


async def get_audit_logs_for_report(
    guild_id: int,
    limit: Optional[int] = 200, # Cho phép None để lấy tất cả
    action_filter: Optional[List[str]] = None,
    time_after: Optional[datetime.datetime] = None
) -> List[Dict[str, Any]]:
    """
    Truy xuất các audit log đã cache từ DB để báo cáo, với các bộ lọc tùy chọn.
    Trả về một list các dict đại diện cho các entry log.
    Nếu limit là None, sẽ cố gắng lấy tất cả các bản ghi phù hợp.
    """
    if not pool: return []
    query_base = """
        SELECT log_id, user_id, target_id, action_type, reason, created_at, extra_data
        FROM audit_log_cache
    """
    conditions = ["guild_id = $1"]
    params: List[Union[int, str, datetime.datetime]] = [guild_id]
    param_count = 1

    if time_after:
        param_count += 1
        conditions.append(f"created_at > ${param_count}")
        params.append(time_after)

    if action_filter:
        if len(action_filter) == 1:
             param_count += 1
             conditions.append(f"action_type = ${param_count}")
             params.append(action_filter[0])
        elif len(action_filter) > 1:
            # Dùng ANY với mảng placeholder
            placeholders = ', '.join(f'${i + param_count + 1}' for i in range(len(action_filter)))
            conditions.append(f"action_type = ANY(ARRAY[{placeholders}])")
            params.extend(action_filter)
            param_count += len(action_filter) # Cập nhật param_count chính xác

    where_clause = " AND ".join(conditions)
    query = f"{query_base} WHERE {where_clause}"

    # Thêm ORDER BY và LIMIT
    query += f" ORDER BY created_at DESC"
    if limit is not None:
         param_count += 1
         query += f" LIMIT ${param_count}"
         params.append(limit)

    try:
        async with pool.acquire() as conn:
            # Đặt timeout cho query này
            rows = await asyncio.wait_for(conn.fetch(query, *params), timeout=45.0)
            # Chuyển đổi Record thành dict, bao gồm cả JSONB extra_data đã được decode
            return [dict(row) for row in rows]
    except asyncio.TimeoutError:
        log.error(f"Timeout khi fetch audit logs cho báo cáo (Guild {guild_id}, Limit: {limit})")
        return []
    except Exception as e:
        # [Dịch]
        log.error(f"Lỗi fetch audit logs cho báo cáo (Guild {guild_id}, Limit: {limit}): {e}", exc_info=False)
        return []

# --- END OF FILE database.py ---