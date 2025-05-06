# --- START OF FILE database.py ---
import asyncpg
import os
import datetime
import json
from typing import Optional, Dict, Any, List, Union
import logging
import discord
import discord.enums
import asyncio

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
pool: Optional[asyncpg.Pool] = None

async def connect_db() -> Optional[asyncpg.Pool]:
    """Thiết lập kết nối cơ sở dữ liệu."""
    global pool
    if pool is not None:
        log.debug("Nhóm kết nối DB đã tồn tại.")
        return pool

    if not DATABASE_URL:
        log.critical("CRITICAL: Biến môi trường DATABASE_URL chưa được đặt.")
        return None

    try:
        log.info("Đang tạo nhóm kết nối cơ sở dữ liệu...")
        pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=2,
            max_size=10,
            init=__set_json_codec, # <<< Đảm bảo codec JSON được thiết lập
            command_timeout=60
        )
        log.info("Đã thiết lập nhóm kết nối cơ sở dữ liệu.")
        await setup_tables() # Tạo bảng sau khi kết nối thành công
    except (asyncpg.exceptions.InvalidCatalogNameError, OSError, ConnectionRefusedError, asyncpg.exceptions.CannotConnectNowError) as e:
        log.critical(f"CRITICAL: Không thể kết nối tới cơ sở dữ liệu: {e}.")
        pool = None
    except Exception as e:
        log.critical(f"CRITICAL: Lỗi không xác định trong quá trình kết nối DB: {e}", exc_info=True)
        pool = None

    return pool

async def close_db():
    """Đóng nhóm kết nối cơ sở dữ liệu."""
    global pool
    if pool:
        log.info("Đang đóng nhóm kết nối cơ sở dữ liệu...")
        try:
            await asyncio.wait_for(pool.close(), timeout=10.0)
            log.info("Nhóm kết nối cơ sở dữ liệu đã được đóng.")
        except asyncio.TimeoutError:
            log.warning("Quá thời gian chờ đóng nhóm kết nối DB. Buộc đóng.")
            pool.terminate() # Buộc đóng nếu timeout
        except Exception as e:
            log.error(f"Lỗi khi đóng nhóm kết nối DB: {e}")
        finally:
            pool = None # Đảm bảo pool được reset
    else:
        log.debug("Không có nhóm kết nối DB nào để đóng.")


async def __set_json_codec(conn):
    """Hàm trợ giúp để đặt codec JSON/JSONB cho asyncpg."""
    log.debug(f"Đang đặt codec JSON/JSONB cho kết nối {conn}")
    try:
        await conn.set_type_codec(
            'jsonb', encoder=lambda v: json.dumps(v, default=str), decoder=json.loads,
            schema='pg_catalog', format='text'
        )
        await conn.set_type_codec(
            'json', encoder=lambda v: json.dumps(v, default=str), decoder=json.loads,
            schema='pg_catalog', format='text'
        )
        log.debug(f"Đặt codec JSON/JSONB thành công cho kết nối {conn}")
    except Exception as e:
        log.error(f"Không thể đặt codec JSON cho kết nối {conn}: {e}", exc_info=True)

async def setup_tables():
    """Tạo các bảng cần thiết nếu chúng chưa tồn tại."""
    if not pool:
        log.error("Không thể thiết lập bảng, nhóm DB không khả dụng.")
        return

    log.info("Đang kiểm tra/tạo bảng cơ sở dữ liệu...")
    try:
        async with pool.acquire() as conn:
            # --- BẢNG AUDIT LOG ---
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_log_cache (
                    log_id BIGINT PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    user_id BIGINT,
                    target_id BIGINT,
                    action_type TEXT NOT NULL,
                    reason TEXT,
                    created_at TIMESTAMPTZ NOT NULL,
                    extra_data JSONB
                );
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_guild_time ON audit_log_cache (guild_id, created_at DESC);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_action_type ON audit_log_cache (guild_id, action_type);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_target_id ON audit_log_cache (target_id);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_user_time ON audit_log_cache (user_id, created_at DESC);")

            # --- BẢNG METADATA GUILD ---
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guild_metadata (
                    guild_id BIGINT PRIMARY KEY,
                    last_audit_log_id BIGINT,
                    last_audit_scan_time TIMESTAMPTZ
                );
            """)

            # --- BẢNG SCANS ---
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS scans (
                    scan_id BIGSERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    requested_by_user_id BIGINT,
                    start_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMPTZ,
                    status VARCHAR(20) NOT NULL DEFAULT 'running', -- 'running', 'completed', 'failed'
                    website_accessible BOOLEAN DEFAULT FALSE,
                    error_message TEXT
                );
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_scans_guild_status_end ON scans (guild_id, status, website_accessible, end_time DESC);")

            # --- BẢNG USER SCAN RESULTS ---
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_scan_results (
                    result_id BIGSERIAL PRIMARY KEY,
                    scan_id BIGINT NOT NULL REFERENCES scans(scan_id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL,
                    display_name_at_scan VARCHAR(100),
                    avatar_url_at_scan VARCHAR(255), -- ĐÃ THÊM CỘT NÀY
                    is_bot BOOLEAN,
                    message_count INTEGER DEFAULT 0,
                    reaction_received_count INTEGER DEFAULT 0,
                    reaction_given_count INTEGER DEFAULT 0,
                    reply_count INTEGER DEFAULT 0,
                    mention_given_count INTEGER DEFAULT 0,
                    mention_received_count INTEGER DEFAULT 0,
                    link_count INTEGER DEFAULT 0,
                    image_count INTEGER DEFAULT 0,
                    sticker_count INTEGER DEFAULT 0,
                    other_file_count INTEGER DEFAULT 0,
                    distinct_channels_count INTEGER DEFAULT 0,
                    first_seen_utc TIMESTAMPTZ,
                    last_seen_utc TIMESTAMPTZ,
                    activity_span_seconds FLOAT,
                    ranking_data JSONB,
                    achievement_data JSONB,
                    raw_dm_embed_data JSONB, -- Tùy chọn
                    UNIQUE (scan_id, user_id)
                );
            """)
            # Tạo index riêng biệt
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_scan_results_scan_user ON user_scan_results (scan_id, user_id);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_scan_results_scan_name ON user_scan_results (scan_id, display_name_at_scan);")

            log.info("Kiểm tra/Tạo bảng cơ sở dữ liệu thành công.")
    except Exception as e:
        log.error(f"Lỗi khi thiết lập bảng cơ sở dữ liệu: {e}", exc_info=True)
        raise e


# --- Các hàm Serialize (Giữ nguyên từ trước) ---
def _serialize_value(value: Any) -> Any:
    if isinstance(value, (str, int, bool, float)) or value is None:
        return value
    elif isinstance(value, discord.Role):
        return {'id': str(value.id), 'name': value.name, 'type': 'role'}
    elif isinstance(value, (discord.Member, discord.User)):
        display = getattr(value, 'display_name', value.name)
        return {
            'id': str(value.id), 'name': value.name,
            'display_name': display, 'discriminator': value.discriminator,
            'bot': value.bot, 'type': 'user'
        }
    elif isinstance(value, (discord.abc.GuildChannel, discord.Thread)):
         parent_id = getattr(value, 'parent_id', None)
         return {
             'id': str(value.id), 'name': value.name, 'type': str(value.type),
             'parent_id': str(parent_id) if parent_id else None
         }
    elif isinstance(value, discord.Invite):
        return {
            'code': value.code,
            'inviter_id': str(value.inviter.id) if value.inviter else None,
            'channel_id': str(value.channel.id) if value.channel else None,
            'uses': value.uses, 'type': 'invite'
        }
    elif isinstance(value, discord.Asset):
        return {'url': str(value), 'key': value.key, 'type': 'asset'}
    elif isinstance(value, datetime.datetime):
        try:
            if value.tzinfo is None:
                aware_dt = value.replace(tzinfo=datetime.timezone.utc)
            else:
                aware_dt = value.astimezone(datetime.timezone.utc)
            return {'iso_utc': aware_dt.isoformat(), 'type': 'datetime'}
        except Exception:
            return {'iso_naive': value.isoformat(), 'type': 'datetime_naive'}
    elif isinstance(value, datetime.timedelta):
        return {'total_seconds': value.total_seconds(), 'type': 'timedelta'}
    elif isinstance(value, discord.Colour):
        return {'value': value.value, 'hex': str(value), 'type': 'color'}
    elif isinstance(value, discord.Permissions):
        return {
            'value': value.value,
            'names': [name for name, enabled in iter(value) if enabled],
            'type': 'permissions'
        }
    elif isinstance(value, list):
        return [_serialize_value(item) for item in value]
    elif isinstance(value, discord.enums.Enum):
        try:
            return {'name': value.name, 'value': value.value, 'type': type(value).__name__}
        except AttributeError:
            return {'repr': repr(value), 'type': type(value).__name__}
    elif isinstance(value, discord.abc.Snowflake):
         obj_id = str(value.id)
         obj_name = getattr(value, 'name', None)
         repr_val = repr(value)
         data = {'id': obj_id, 'type': type(value).__name__, 'repr': repr_val}
         if obj_name: data['name'] = obj_name
         return data
    else:
        try: repr_val = repr(value)
        except Exception: repr_val = "<Unrepresentable Object>"
        log.debug(f"Không thể serialize đối tượng loại {type(value).__name__}: {repr_val[:100]}")
        return {'repr': repr_val, 'type': str(type(value).__name__)}

def _serialize_changes(changes: discord.AuditLogChanges) -> Optional[Dict[str, Any]]:
    if not changes:
        return None
    data = {'before': {}, 'after': {}}
    attributes_to_check = set()
    if hasattr(changes.before, '__slots__'): attributes_to_check.update(changes.before.__slots__)
    if hasattr(changes.after, '__slots__'): attributes_to_check.update(changes.after.__slots__)
    attributes_to_check.update(['name', 'id', 'type', 'roles', 'nick', 'mute', 'deaf', 'permissions', 'color', 'hoist', 'mentionable', 'topic', 'nsfw', 'bitrate', 'user_limit'])
    keys_with_changes = set()
    for attr in attributes_to_check:
        if attr.startswith('_'): continue
        try:
            before_val = getattr(changes.before, attr, discord.utils.MISSING) if changes.before else discord.utils.MISSING
            after_val = getattr(changes.after, attr, discord.utils.MISSING) if changes.after else discord.utils.MISSING
            if (before_val is not discord.utils.MISSING or after_val is not discord.utils.MISSING) and before_val != after_val:
                 is_callable = callable(before_val) or callable(after_val)
                 if not is_callable:
                    if before_val is not discord.utils.MISSING:
                        data['before'][attr] = _serialize_value(before_val)
                    if after_val is not discord.utils.MISSING:
                        data['after'][attr] = _serialize_value(after_val)
                    keys_with_changes.add(attr)
        except Exception as e:
            log.debug(f"Lỗi serialize thuộc tính '{attr}' trong audit log changes: {e}")
    return data if keys_with_changes else None


# --- Các hàm thao tác DB (Audit Log - Giữ nguyên từ trước) ---
async def add_audit_log_entry(log_entry: discord.AuditLogEntry):
    if not pool or not log_entry:
        log.warning("Bỏ qua add_audit_log_entry do pool DB hoặc log_entry không hợp lệ.")
        return
    try:
        async with pool.acquire() as conn:
            user_id = log_entry.user.id if log_entry.user else None
            target_id: Optional[int] = None
            target_obj = log_entry.target
            try:
                if isinstance(target_obj, discord.abc.Snowflake): target_id = target_obj.id
                elif isinstance(target_obj, dict) and 'id' in target_obj: target_id = int(target_obj['id'])
                elif isinstance(target_obj, str): log.debug(f"Audit log target là string: '{target_obj}' for action {log_entry.action}. target_id sẽ là NULL."); target_id = None
            except (ValueError, TypeError, AttributeError) as e:
                log.warning(f"Không thể trích xuất ID từ audit log target type {type(target_obj)} (Value: {target_obj}, Action: {log_entry.action}): {e}")
                target_id = None
            extra_data = _serialize_changes(log_entry.changes)
            query = """
                INSERT INTO audit_log_cache (log_id, guild_id, user_id, target_id, action_type, reason, created_at, extra_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (log_id) DO UPDATE SET
                    guild_id = EXCLUDED.guild_id, user_id = EXCLUDED.user_id, target_id = EXCLUDED.target_id,
                    action_type = EXCLUDED.action_type, reason = EXCLUDED.reason, created_at = EXCLUDED.created_at,
                    extra_data = EXCLUDED.extra_data; """
            created_at_aware = log_entry.created_at
            if created_at_aware.tzinfo is None: created_at_aware = created_at_aware.replace(tzinfo=datetime.timezone.utc)
            await conn.execute(query, log_entry.id, log_entry.guild.id, user_id, target_id, str(log_entry.action.name), log_entry.reason, created_at_aware, extra_data)
    except Exception as e:
        log.error(f"Lỗi thêm entry audit log {log_entry.id} cho guild {log_entry.guild.id}: {e}", exc_info=False)

async def get_newest_audit_log_id_from_db(guild_id: int) -> Optional[int]:
    if not pool: return None
    query = "SELECT last_audit_log_id FROM guild_metadata WHERE guild_id = $1"
    try:
        async with pool.acquire() as conn:
            result = await conn.fetchrow(query, guild_id)
            return result['last_audit_log_id'] if result and result['last_audit_log_id'] is not None else None
    except Exception as e:
        log.error(f"Lỗi lấy ID audit log mới nhất cho guild {guild_id}: {e}", exc_info=False)
        return None

async def update_newest_audit_log_id(guild_id: int, newest_log_id: Optional[int]):
    if not pool: return
    if newest_log_id is None:
        log.debug(f"Bỏ qua cập nhật ID audit log mới nhất cho guild {guild_id} vì newest_log_id là None.")
        return
    now = datetime.datetime.now(datetime.timezone.utc)
    query = """
        INSERT INTO guild_metadata (guild_id, last_audit_log_id, last_audit_scan_time) VALUES ($1, $2, $3)
        ON CONFLICT (guild_id) DO UPDATE SET
            last_audit_log_id = EXCLUDED.last_audit_log_id, last_audit_scan_time = EXCLUDED.last_audit_scan_time
        WHERE EXCLUDED.last_audit_log_id IS NOT NULL AND
              (guild_metadata.last_audit_log_id IS NULL OR EXCLUDED.last_audit_log_id > guild_metadata.last_audit_log_id); """
    try:
        async with pool.acquire() as conn:
            result = await conn.execute(query, guild_id, newest_log_id, now)
            if result and ('UPDATE 1' in result.upper() or 'INSERT 0 1' in result.upper()): log.info(f"Đã cập nhật ID audit log mới nhất cho guild {guild_id} thành {newest_log_id}")
            else: log.debug(f"ID audit log {newest_log_id} không mới hơn ID đã lưu cho guild {guild_id}. Bỏ qua cập nhật.")
    except Exception as e:
        log.error(f"Lỗi cập nhật ID audit log mới nhất cho guild {guild_id}: {e}", exc_info=False)

async def get_audit_logs_for_report(guild_id: int, limit: Optional[int] = 200, action_filter: Optional[List[Union[discord.AuditLogAction, str]]] = None, time_after: Optional[datetime.datetime] = None) -> List[Dict[str, Any]]:
    if not pool: return []
    query_base = "SELECT log_id, user_id, target_id, action_type, reason, created_at, extra_data FROM audit_log_cache"
    conditions = ["guild_id = $1"]; params: List[Any] = [guild_id]; param_count = 1
    if time_after:
        param_count += 1; time_after_aware = time_after
        if time_after_aware.tzinfo is None: time_after_aware = time_after_aware.replace(tzinfo=datetime.timezone.utc)
        conditions.append(f"created_at > ${param_count}"); params.append(time_after_aware)
    if action_filter:
        action_names = []
        for action in action_filter:
            if isinstance(action, discord.AuditLogAction): action_names.append(action.name)
            elif isinstance(action, str): action_names.append(action)
            else: log.warning(f"Loại action_filter không hợp lệ: {type(action)}, bỏ qua.")
        if action_names:
            placeholders = ', '.join(f'${i + param_count + 1}' for i in range(len(action_names)))
            conditions.append(f"action_type = ANY(ARRAY[{placeholders}]::text[])"); params.extend(action_names); param_count += len(action_names)
    where_clause = " AND ".join(conditions); query = f"{query_base} WHERE {where_clause}"
    query += f" ORDER BY created_at DESC"
    if limit is not None: param_count += 1; query += f" LIMIT ${param_count}"; params.append(limit)
    log.debug(f"Executing get_audit_logs query: {query} with params: {params}")
    try:
        async with pool.acquire() as conn: rows = await asyncio.wait_for(conn.fetch(query, *params), timeout=45.0); return [dict(row) for row in rows]
    except asyncio.TimeoutError: log.error(f"Timeout khi fetch audit logs cho báo cáo (Guild {guild_id}, Limit: {limit})"); return []
    except Exception as e: log.error(f"Lỗi fetch audit logs cho báo cáo (Guild {guild_id}, Limit: {limit}): {e}", exc_info=False); return []


# --- Các hàm thao tác DB (Scan Records & User Results - Mới thêm) ---
async def create_scan_record(guild_id: int, requested_by_user_id: Optional[int]) -> Optional[int]:
    """Tạo một bản ghi quét mới và trả về scan_id."""
    if not pool: return None
    query = """
        INSERT INTO scans (guild_id, requested_by_user_id, status)
        VALUES ($1, $2, 'running')
        RETURNING scan_id;
    """
    try:
        async with pool.acquire() as conn:
            result = await conn.fetchrow(query, guild_id, requested_by_user_id)
            if result and result['scan_id']:
                log.info(f"Đã tạo bản ghi quét mới scan_id: {result['scan_id']} cho guild {guild_id}")
                return result['scan_id']
            else:
                log.error("Không thể tạo bản ghi quét mới (không có ID trả về).")
                return None
    except Exception as e:
        log.error(f"Lỗi khi tạo bản ghi quét cho guild {guild_id}: {e}", exc_info=True)
        return None

async def save_user_scan_results(scan_id: int, user_results: List[Dict[str, Any]]):
    """Lưu hàng loạt kết quả của user vào database."""
    if not pool or not user_results: return
    # Đảm bảo tên cột avatar_url_at_scan có trong câu lệnh INSERT và UPDATE
    query = """
        INSERT INTO user_scan_results (
            scan_id, user_id, display_name_at_scan, avatar_url_at_scan, is_bot, message_count,
            reaction_received_count, reaction_given_count, reply_count,
            mention_given_count, mention_received_count, link_count, image_count,
            sticker_count, other_file_count, distinct_channels_count,
            first_seen_utc, last_seen_utc, activity_span_seconds,
            ranking_data, achievement_data
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
        )
        ON CONFLICT (scan_id, user_id) DO UPDATE SET
            display_name_at_scan = EXCLUDED.display_name_at_scan,
            avatar_url_at_scan = EXCLUDED.avatar_url_at_scan,
            is_bot = EXCLUDED.is_bot, message_count = EXCLUDED.message_count,
            reaction_received_count = EXCLUDED.reaction_received_count,
            reaction_given_count = EXCLUDED.reaction_given_count, reply_count = EXCLUDED.reply_count,
            mention_given_count = EXCLUDED.mention_given_count, mention_received_count = EXCLUDED.mention_received_count,
            link_count = EXCLUDED.link_count, image_count = EXCLUDED.image_count,
            sticker_count = EXCLUDED.sticker_count, other_file_count = EXCLUDED.other_file_count,
            distinct_channels_count = EXCLUDED.distinct_channels_count, first_seen_utc = EXCLUDED.first_seen_utc,
            last_seen_utc = EXCLUDED.last_seen_utc, activity_span_seconds = EXCLUDED.activity_span_seconds,
            ranking_data = EXCLUDED.ranking_data, achievement_data = EXCLUDED.achievement_data;
    """
    data_tuples = []
    for user_data in user_results:
        data_tuples.append((
            scan_id, user_data.get('user_id'), user_data.get('display_name_at_scan'),
            user_data.get('avatar_url_at_scan'), user_data.get('is_bot'),
            user_data.get('message_count', 0), user_data.get('reaction_received_count', 0),
            user_data.get('reaction_given_count', 0), user_data.get('reply_count', 0),
            user_data.get('mention_given_count', 0), user_data.get('mention_received_count', 0),
            user_data.get('link_count', 0), user_data.get('image_count', 0),
            user_data.get('sticker_count', 0), user_data.get('other_file_count', 0),
            user_data.get('distinct_channels_count', 0), user_data.get('first_seen_utc'),
            user_data.get('last_seen_utc'), user_data.get('activity_span_seconds'),
            user_data.get('ranking_data'), user_data.get('achievement_data')
        ))
    try:
        async with pool.acquire() as conn:
            async with conn.transaction(): await conn.executemany(query, data_tuples)
            log.info(f"Đã lưu {len(data_tuples)} kết quả user cho scan_id: {scan_id}")
    except Exception as e:
        log.error(f"Lỗi khi lưu hàng loạt kết quả user cho scan_id {scan_id}: {e}", exc_info=True)

async def update_scan_status(scan_id: int, status: str, end_time: Optional[datetime.datetime] = None, website_ready: Optional[bool] = None, error: Optional[str] = None):
    """Cập nhật trạng thái và thời gian kết thúc của một bản ghi quét."""
    if not pool or not scan_id: return
    updates = ["status = $2"]; params: List[Any] = [scan_id, status]; param_count = 2
    if end_time: param_count += 1; updates.append(f"end_time = ${param_count}"); params.append(end_time)
    if website_ready is not None: param_count += 1; updates.append(f"website_accessible = ${param_count}"); params.append(website_ready)
    if error: param_count += 1; updates.append(f"error_message = ${param_count}"); params.append(error)
    set_clause = ", ".join(updates); query = f"UPDATE scans SET {set_clause} WHERE scan_id = $1"
    try:
        async with pool.acquire() as conn:
            await conn.execute(query, *params)
            log.info(f"Đã cập nhật trạng thái scan_id {scan_id} thành '{status}'" + (" (Web Ready)" if website_ready else "") + (f" Error: {error[:50]}..." if error else ""))
    except Exception as e:
        log.error(f"Lỗi khi cập nhật trạng thái scan_id {scan_id}: {e}", exc_info=True)

# --- END OF FILE database.py ---