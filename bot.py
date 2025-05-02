# --- START OF FILE bot.py ---
import discord
from discord.ext import commands
import os
from dotenv import load_dotenv
import asyncio
import datetime
import time
from collections import Counter, defaultdict
import math
import traceback
import io
import sys
# <<< MODIFIED: Added Union for type hint >>>
from typing import List, Dict, Any, Optional, Union
import re # Để xóa mã màu của rich và đếm link/emoji

# Import modules
import utils
import reporting
import database

# Import logging and Rich + Queue Handler
import logging
from logging.handlers import QueueHandler # Sử dụng QueueHandler
import queue # Queue để chứa log record
import threading # Cho background thread xử lý log queue
from rich.logging import RichHandler
# from rich.console import Console # Tùy chọn

# --- Basic Checks ---
MIN_PYTHON = (3, 8)
if sys.version_info < MIN_PYTHON:
    sys.exit(f"Yêu cầu Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]} trở lên.")

# --- Custom Discord Logging Handler & Thread Logic ---

log_queue = queue.Queue()

class DiscordLogHandler(logging.Handler):
    """Handler này chỉ đưa log record vào Queue."""
    def __init__(self, level=logging.NOTSET):
        super().__init__(level=level)

    def emit(self, record: logging.LogRecord):
        # <<< ADDED: Explain Batching >>>
        # Note: Logs are intentionally queued and sent in batches.
        # Sending each log line individually would quickly hit Discord rate limits.
        log_queue.put(record)

discord_log_sender_thread: Optional[threading.Thread] = None
discord_log_thread_active = False
discord_target_thread: Optional[discord.Thread] = None
discord_log_buffer: List[str] = []
discord_log_sender_lock = threading.Lock()
_bot_ref_for_log_sending: Optional[discord.Client] = None

def strip_rich_markup(text: str) -> str:
    """Xóa các thẻ markup [color], [bold], etc. của Rich."""
    # Enhanced regex to handle more cases, including empty tags
    text = re.sub(r'\[(/?[a-zA-Z_]+(?:=[^\]]*)?)\]', '', text)
    text = re.sub(r'\[link=[^\]]+\](.*?)\[/link\]', r'\1', text)
    return text

# Improved log sending with better formatting potential
async def send_log_batch(log_lines: List[str], thread_id: int, is_final: bool = False):
    """Hàm Coroutine chạy trên event loop chính để gửi log vào thread."""
    global _bot_ref_for_log_sending
    if not _bot_ref_for_log_sending:
        print("[LỖI send_log_batch] Tham chiếu bot chưa sẵn sàng.")
        return

    try:
        thread = _bot_ref_for_log_sending.get_channel(thread_id)
        if not thread:
            try: thread = await _bot_ref_for_log_sending.fetch_channel(thread_id)
            except (discord.NotFound, discord.Forbidden): thread = None

        if not isinstance(thread, discord.Thread):
            print(f"[LỖI send_log_batch] Không tìm thấy thread ID {thread_id} hoặc không phải là thread.")
            with discord_log_sender_lock:
                global discord_target_thread
                discord_target_thread = None
            return

        if not log_lines and not is_final: return

        full_log_content = "\n".join(log_lines)
        if is_final:
             final_msg = "\n--- Kết thúc log ---"
             full_log_content += final_msg

        # <<< MODIFIED: Slightly smaller max_len for safety >>>
        max_len = 1900 # Giới hạn ký tự Discord trừ đi định dạng ```log và potential newline chars
        message_parts = []
        current_part = ""

        for line in full_log_content.splitlines(keepends=True):
            # <<< MODIFIED: Check length BEFORE appending if current_part is not empty >>>
            if current_part and len(current_part) + len(line) > max_len:
                 message_parts.append(current_part)
                 current_part = line
            # <<< MODIFIED: Handle lines longer than max_len >>>
            elif len(line) > max_len:
                 if current_part: message_parts.append(current_part) # Send previous part first
                 # Split the long line itself
                 for i in range(0, len(line), max_len):
                     message_parts.append(line[i:i+max_len])
                 current_part = "" # Reset current part after splitting long line
            else:
                 current_part += line

        if current_part: message_parts.append(current_part)

        for i, part in enumerate(message_parts):
            if not part.strip(): continue
            # Ensure content doesn't exceed Discord's limit even with code block formatting
            safe_part = part[:1990] # Extra safety trim
            content_to_send = f"```log\n{safe_part.strip()}\n```"
            try:
                await thread.send(content_to_send)
                # <<< MODIFIED: Slightly longer sleep >>>
                if i < len(message_parts) - 1 or is_final:
                     await asyncio.sleep(1.0) # Increase sleep slightly
            except discord.HTTPException as send_err:
                 print(f"[LỖI send_log_batch] HTTP {send_err.status} khi gửi phần {i+1} vào thread {thread_id}: {send_err.text}")
                 if send_err.status == 429: # Rate limited
                     retry_after = send_err.retry_after or 5.0
                     print(f"   -> Bị rate limit, chờ {retry_after:.2f}s...")
                     await asyncio.sleep(retry_after + 0.5) # Add small buffer
                 elif send_err.status >= 500: # Server error
                     print("   -> Lỗi server Discord, chờ 5s...")
                     await asyncio.sleep(5)
                 else: # Other HTTP errors (like Forbidden)
                     await asyncio.sleep(2) # Wait a bit before potentially trying again or stopping
            except Exception as send_e:
                 print(f"[LỖI send_log_batch] Lỗi không xác định khi gửi phần {i+1} vào thread {thread_id}: {send_e}")
                 await asyncio.sleep(1) # Wait a bit

        if is_final:
             try:
                 e_final = lambda n: utils.get_emoji(n, _bot_ref_for_log_sending)
                 await thread.send(f"{e_final('success')} Quét hoàn tất cho lệnh này. {e_final('lock')} Luồng này sẽ được lưu trữ và khóa.")
                 await asyncio.sleep(1)
                 await thread.edit(archived=True, locked=True)
             except discord.Forbidden:
                 print(f"[CẢNH BÁO send_log_batch] Thiếu quyền archive/lock thread {thread_id}.")
                 await thread.send(f"({e_final('error')} Không thể tự động lưu trữ/khóa luồng do thiếu quyền)")
             except discord.HTTPException as e:
                 print(f"[CẢNH BÁO send_log_batch] Lỗi HTTP khi archive/lock thread {thread_id}: {e.status} {e.text}")
             except Exception as final_err:
                 print(f"[LỖI send_log_batch] Lỗi khi gửi tin nhắn cuối/archive thread {thread_id}: {final_err}")

    except discord.NotFound:
         print(f"[LỖI send_log_batch] Không tìm thấy thread ID {thread_id} khi gửi.")
         with discord_log_sender_lock: discord_target_thread = None
    except discord.Forbidden:
         print(f"[LỖI send_log_batch] Thiếu quyền gửi tin nhắn vào thread {thread_id}.")
         with discord_log_sender_lock: discord_target_thread = None
    except discord.HTTPException as http_err:
         print(f"[LỖI send_log_batch] Lỗi HTTP {http_err.status} khi gửi log vào thread {thread_id}: {http_err.text}")
         await asyncio.sleep(2)
    except Exception as e:
         print(f"[LỖI send_log_batch] Lỗi không xác định khi gửi log vào thread {thread_id}: {e}")
         traceback.print_exc()

def discord_log_sender(bot_loop: asyncio.AbstractEventLoop):
    """Chạy trong thread riêng, xử lý log từ Queue và gửi lên event loop chính."""
    global discord_log_thread_active, discord_target_thread, discord_log_buffer

    print("[INFO Discord Logger Thread] Bắt đầu.")

    while discord_log_thread_active:
        send_buffer_now = False
        record = None
        try:
            # <<< MODIFIED: Shorter timeout for potentially faster batch sends >>>
            record = log_queue.get(block=True, timeout=1.5) # Reduced timeout
            if record is None:
                 print("[INFO Discord Logger Thread] Nhận tín hiệu dừng (None).")
                 discord_log_thread_active = False
                 send_buffer_now = True
            else:
                if not discord_log_thread_active: continue

                # Map log level to emoji (Ensuring it uses the global bot ref if available)
                level_emoji_map = {
                    logging.CRITICAL: utils.get_emoji('error', _bot_ref_for_log_sending),
                    logging.ERROR:    utils.get_emoji('error', _bot_ref_for_log_sending),
                    logging.WARNING:  utils.get_emoji('warning', _bot_ref_for_log_sending),
                    logging.INFO:     utils.get_emoji('info', _bot_ref_for_log_sending),
                    logging.DEBUG:    utils.get_emoji('hashtag', _bot_ref_for_log_sending),
                    logging.NOTSET:   utils.get_emoji('info', _bot_ref_for_log_sending),
                }
                log_emoji = level_emoji_map.get(record.levelno, utils.get_emoji('info', _bot_ref_for_log_sending))

                # <<< MODIFIED: More concise formatter for thread logs >>>
                temp_formatter = logging.Formatter('%(asctime)s [%(levelname)-.1s] %(name)s: %(message)s', datefmt='%H:%M:%S')
                try:
                    msg = temp_formatter.format(record)
                    msg_cleaned_no_markup = strip_rich_markup(msg)
                    # <<< CONFIRMED: Emoji is prepended here >>>
                    msg_with_emoji = f"{log_emoji} {msg_cleaned_no_markup}"
                except Exception as fmt_err:
                    msg_with_emoji = f"{utils.get_emoji('error', _bot_ref_for_log_sending)} Lỗi format log: {fmt_err} | Record: {record.getMessage()[:100]}"


                with discord_log_sender_lock:
                    if discord_target_thread:
                        discord_log_buffer.append(msg_with_emoji)
                        # <<< MODIFIED: Send more frequently based on line count OR char count >>>
                        if len(discord_log_buffer) >= 5 or len("\n".join(discord_log_buffer)) > 1000 : # Send smaller batches
                            send_buffer_now = True

        except queue.Empty:
            # <<< MODIFIED: Send buffer if not empty after timeout >>>
            with discord_log_sender_lock:
                if discord_target_thread and discord_log_buffer:
                     send_buffer_now = True # Send whatever is left after timeout
        except Exception as e:
            print(f"[LỖI Discord Logger Thread] Lỗi không xác định: {e}")
            traceback.print_exc()
            time.sleep(5)

        if send_buffer_now:
            with discord_log_sender_lock:
                 if discord_target_thread and discord_log_buffer:
                     messages_to_send = list(discord_log_buffer)
                     discord_log_buffer.clear()
                     # <<< ADDED: Log batch sending action >>>
                     print(f"[INFO Discord Logger Thread] Chuẩn bị gửi batch {len(messages_to_send)} dòng log...")
                     asyncio.run_coroutine_threadsafe(
                         send_log_batch(messages_to_send, discord_target_thread.id, is_final=(not discord_log_thread_active)),
                         bot_loop
                     )

        # <<< REMOVED: Redundant sleep conditions >>>
        # if not discord_log_thread_active and not discord_log_buffer:
        #      break
        # if record is None and not send_buffer_now:
        #      time.sleep(0.1)


    # --- Dọn dẹp cuối cùng khi thread thoát vòng lặp ---
    print("[INFO Discord Logger Thread] Dọn dẹp và kết thúc.")
    with discord_log_sender_lock:
        if discord_target_thread and discord_log_buffer: # Gửi nốt lần cuối nếu còn
            messages_to_send = list(discord_log_buffer)
            discord_log_buffer.clear()
            print(f"[INFO Discord Logger Thread] Gửi nốt {len(messages_to_send)} dòng log cuối cùng.")
            asyncio.run_coroutine_threadsafe(
                 send_log_batch(messages_to_send, discord_target_thread.id, is_final=True),
                 bot_loop
             )
        discord_target_thread = None
    print("[INFO Discord Logger Thread] Thread đã dừng.") # Added final confirmation

# --- Logging Setup using Rich ---
rich_handler = RichHandler(
    rich_tracebacks=True, markup=True, show_path=False, log_time_format="[%H:%M:%S]" # Ngắn gọn hơn
)
rich_handler.setLevel(logging.INFO) # Chỉ hiển thị INFO trở lên trên console

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG) # Đặt root level thành DEBUG để bắt mọi thứ

if root_logger.hasHandlers():
    print(f"[INFO] Xóa {len(root_logger.handlers)} handler(s) cũ khỏi root logger.")
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

# 1. Thêm RichHandler cho Console Output (chỉ INFO+)
root_logger.addHandler(rich_handler)

# 2. Thêm QueueHandler để đưa log vào Queue cho Discord (DEBUG+)
discord_queue_handler = QueueHandler(log_queue)
discord_queue_handler.setLevel(logging.DEBUG) # Gửi cả DEBUG vào queue
root_logger.addHandler(discord_queue_handler)

# Cấu hình logger discord.py và asyncpg
logging.getLogger("discord").setLevel(logging.INFO) # Giảm noise từ discord.py
logging.getLogger("asyncpg").setLevel(logging.WARNING)

log = logging.getLogger(__name__) # Logger chính của bot

# --- Cấu hình ---
load_dotenv()
BOT_TOKEN = os.getenv("DISCORD_TOKEN", "YOUR_BOT_TOKEN")
COMMAND_PREFIX = os.getenv("COMMAND_PREFIX", "!")
MIN_MESSAGE_COUNT_FOR_REPORT = int(os.getenv("MIN_MESSAGE_COUNT_FOR_REPORT", 100))
FINAL_STICKER_ID = os.getenv("FINAL_STICKER_ID")
if FINAL_STICKER_ID and FINAL_STICKER_ID.isdigit(): FINAL_STICKER_ID = int(FINAL_STICKER_ID)
else: FINAL_STICKER_ID = None
BOT_NAME = os.getenv("BOT_NAME", "Shiromi")
ENABLE_REACTION_SCAN = os.getenv("ENABLE_REACTION_SCAN", "False").lower() == "true"

# --- Thiết lập Bot và Intents ---
intents = discord.Intents.default()
intents.guilds = True; intents.messages = True; intents.members = True; intents.message_content = True
intents.presences = True; intents.invites = True; intents.guild_messages = True
# <<< MODIFIED: Ensure voice_states is True >>>
intents.voice_states = True # Needed for reading voice channel history
if ENABLE_REACTION_SCAN:
    intents.reactions = True
    log.info("[bold yellow]Đã bật Reaction Intent để quét biểu cảm.[/bold yellow]")

# Kiểm tra Intents quan trọng
if not all([intents.guilds, intents.members, intents.message_content]):
     log.critical("[bold red]LỖI:[/bold red] Thiếu các Privileged Intents quan trọng (Guilds, Members, Message Content)!")
     log.critical("[bold yellow]>>> Vui lòng bật chúng trong Discord Developer Portal! <<<[/bold yellow]"); sys.exit(1)
if ENABLE_REACTION_SCAN and not intents.reactions:
    log.critical("[bold red]LỖI:[/bold red] Đã bật ENABLE_REACTION_SCAN nhưng thiếu Reaction Intent!")
    log.critical("[bold yellow]>>> Vui lòng bật nó trong Discord Developer Portal! <<<[/bold yellow]"); sys.exit(1)
# <<< ADDED: Check voice_states if needed (though message_content should cover it) >>>
# Although message_content *should* be enough, let's log if voice_states is missing, just in case.
if not intents.voice_states:
    log.warning("[bold yellow]Cảnh báo:[/bold yellow] Voice States Intent đang tắt. Việc quét lịch sử kênh voice có thể bị ảnh hưởng nếu chỉ dựa vào Message Content.")


bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents, help_command=commands.DefaultHelpCommand())

# --- Setup Hook ---
async def setup_hook_logic():
    global _bot_ref_for_log_sending
    log.info("Đang chạy logic setup hook...")
    utils.set_bot_reference_for_emoji(bot)
    _bot_ref_for_log_sending = bot
    await database.connect_db()
    log.info("Logic setup hook đã hoàn thành.")

# --- Sự kiện khi Bot sẵn sàng ---
@bot.event
async def on_ready():
    global discord_log_sender_thread, discord_log_thread_active
    e = lambda n: utils.get_emoji(n, bot) # Emoji getter cho on_ready
    log.info(f'{e("success")} Đã đăng nhập với tư cách [bold cyan]{bot.user.name}[/] (ID: {bot.user.id})')
    log.info(f'discord.py [blue]{discord.__version__}[/], Python [green]{sys.version.split(" ")[0]}[/]')
    log.info(f'{e("members")} Đã kết nối tới [magenta]{len(bot.guilds)}[/] máy chủ.')
    try: log.info(f"{e('mention')} Đã tải [magenta]{len(bot.emojis)}[/] emoji tùy chỉnh.")
    except AttributeError: log.info("Không thể đếm emoji tùy chỉnh.")
    log.info('[dim]------[/dim]')
    required_emojis = ['success', 'error', 'loading', 'stats', 'role', 'thread', 'csv_file', 'json_file', 'warning', 'info', 'hashtag', 'voice_channel', 'text_channel'] # Added text_channel for consistency
    for name in required_emojis:
         emoji_val = utils.get_emoji(name) # Bot ref đã được set trong setup_hook
         is_default = emoji_val == utils.EMOJI_IDS.get(name, "❓") or ':123' in emoji_val or ':12345' in emoji_val
         log.info(f"  Emoji '{name}': {emoji_val}" + ("[dim] (Mặc định/Fallback)[/dim]" if is_default else " ([green]OK[/])"))
    log.info('[dim]------[/dim]')
    try:
        activity = discord.Activity(type=discord.ActivityType.watching, name=f"{BOT_NAME} | {COMMAND_PREFIX}help")
        await bot.change_presence(activity=activity, status=discord.Status.online)
        log.info(f"{e('online')} Trạng thái hoạt động đã được cập nhật.")
    except Exception as e_act: log.warning(f"Không thể đặt trạng thái hoạt động: {e_act}")

    # Khởi động thread xử lý log
    if discord_log_sender_thread is None or not discord_log_sender_thread.is_alive():
        discord_log_thread_active = True
        loop = asyncio.get_running_loop()
        discord_log_sender_thread = threading.Thread(target=discord_log_sender, args=(loop,), daemon=True)
        discord_log_sender_thread.start()
        log.info(f"{e('success')} [bold green]Thread gửi log Discord đã khởi động.[/bold green]")
    else:
         log.info("Thread gửi log Discord đã chạy.")

# --- Lệnh chính ---
@bot.command(
    name='serverdeepscan', aliases=['sds'],
    help=('Quét siêu sâu server (OWNER ONLY).\n'
          'Quét TOÀN BỘ lịch sử tin nhắn trong kênh text/voice, user, roles, channels, boosters, invites*, webhooks*, integrations*, audit logs*, phân tích quyền*, [reactions*].\n'
          '(* yêu cầu quyền, reactions yêu cầu ENABLE_REACTION_SCAN=True)\n'
          'Lưu ý: Quét toàn bộ có thể CHẬM và dùng nhiều API.\n'
          'Hỗ trợ xuất CSV/JSON, đếm từ khóa, gửi log vào thread.\n'
          'Tham số:\n'
          '  `export_csv=True/False`\n'
          '  `export_json=True/False`\n'
          '  `keywords="từ khóa 1, từ 2"`'),
    brief='Quét sâu toàn bộ server, tạo báo cáo, gửi log vào thread.'
)
@commands.is_owner()
@commands.cooldown(1, 7200, commands.BucketType.guild) # Cooldown 2 tiếng
@commands.guild_only()
async def server_deep_scan(ctx: commands.Context, export_csv: bool = False, export_json: bool = False, *, keywords: Optional[str] = None):
    """Thực hiện quét sâu toàn bộ server."""
    global discord_target_thread
    start_time_cmd = time.monotonic()
    overall_start_time = discord.utils.utcnow()
    e = lambda name: utils.get_emoji(name, bot) # Shortcut
    scan_errors: List[str] = []
    log_thread_this_command: Optional[discord.Thread] = None

    server = ctx.guild
    log.info(f"{e('loading')} Khởi tạo quét sâu TOÀN BỘ cho server: [bold cyan]{server.name}[/] ({server.id}) yêu cầu bởi [bold yellow]{ctx.author}[/] ({ctx.author.id})")
    if ENABLE_REACTION_SCAN:
        log.warning("[bold yellow]!!! Quét biểu cảm (Reaction Scan) đang BẬT. Quá trình quét có thể chậm hơn !!![/bold yellow]")

    # --- Tạo Thread Log ---
    initial_status_msg = await ctx.send(f"{e('loading')} Đang tạo luồng log...")
    try:
        thread_name = f"{e('stats')} S{server.id}-U{ctx.author.id}-{overall_start_time.strftime('%y%m%d-%H%M')}"
        # <<< MODIFIED: Request longer archive duration if possible >>>
        log_thread_this_command = await ctx.channel.create_thread(
            name=thread_name,
            type=discord.ChannelType.public_thread,
            auto_archive_duration=4320 # Request 3 days if possible (Nitro Level 1+) otherwise defaults
        )

        start_embed = discord.Embed(
            title=f"{e('loading')} Bắt đầu quét sâu TOÀN BỘ server: {server.name}",
            description=f"Yêu cầu bởi: {ctx.author.mention}\nThời gian bắt đầu: {utils.format_discord_time(overall_start_time, 'F')}",
            color=discord.Color.blue(),
            timestamp=overall_start_time
        )
        start_embed.add_field(name="Cấu hình", value=f"Export CSV: `{export_csv}`, Export JSON: `{export_json}`", inline=False)
        if keywords: start_embed.add_field(name="Keywords", value=f"`{keywords}`", inline=False)
        start_embed.set_footer(text="Log chi tiết sẽ được gửi vào đây...")
        await log_thread_this_command.send(embed=start_embed)

        with discord_log_sender_lock:
             discord_target_thread = log_thread_this_command
             discord_log_buffer.clear()

        log.info(f"{e('success')} Đã tạo luồng log: {log_thread_this_command.mention}")
        await initial_status_msg.edit(content=f"{e('success')} Đã tạo luồng log {log_thread_this_command.mention}. Bắt đầu quét...")

    except discord.Forbidden:
        log.error(f"{e('error')} Thiếu quyền tạo luồng (Manage Threads hoặc Create Public Threads). Sẽ không gửi log vào Discord.")
        scan_errors.append("Thiếu quyền tạo luồng log.")
        await initial_status_msg.edit(content=f"{e('error')} Không thể tạo luồng log. Log chi tiết chỉ có trên console.")
        with discord_log_sender_lock: discord_target_thread = None
    except discord.HTTPException as http_err:
        log.error(f"{e('error')} Lỗi HTTP khi tạo luồng log: {http_err.status} {http_err.text}")
        scan_errors.append(f"Lỗi mạng khi tạo luồng log ({http_err.status}).")
        await initial_status_msg.edit(content=f"{e('error')} Lỗi mạng khi tạo luồng log. Log chi tiết chỉ có trên console.")
        with discord_log_sender_lock: discord_target_thread = None
    except Exception as thread_err:
        log.error(f"{e('error')} Lỗi không xác định khi tạo luồng log: {thread_err}", exc_info=True)
        scan_errors.append(f"Lỗi không xác định khi tạo luồng log: {thread_err}")
        await initial_status_msg.edit(content=f"{e('error')} Lỗi không xác định khi tạo luồng log. Log chi tiết chỉ có trên console.")
        with discord_log_sender_lock: discord_target_thread = None


    # <<< BẮT ĐẦU KHỐI TRY...FINALLY CHÍNH >>>
    try:
        # --- Kiểm tra Database ---
        db_pool = await database.connect_db()
        if not db_pool:
            await ctx.send(f"{e('error')} Không thể kết nối đến database. Quét sâu không thể tiếp tục.", delete_after=20)
            ctx.command.reset_cooldown(ctx)
            raise ConnectionError("Không thể kết nối database.")
        log.info(f"{e('success')} Kết nối database đã được xác nhận.")

        # --- Xử lý Keywords ---
        target_keywords = []
        keyword_counts: Counter = Counter()
        channel_keyword_counts: Dict[int, Counter] = defaultdict(Counter)
        thread_keyword_counts: Dict[int, Counter] = defaultdict(Counter)
        user_keyword_counts: Dict[int, Counter] = defaultdict(Counter)
        if keywords:
             try:
                 target_keywords = [kw.strip().lower() for kw in keywords.split(',') if kw.strip()]
                 if not target_keywords:
                     log.warning("Tham số keywords được cung cấp nhưng không có từ khóa hợp lệ.")
                     scan_errors.append("Từ khóa trống hoặc không hợp lệ.")
                 else:
                     log.info(f"{e('hashtag')} Sẽ tìm kiếm các từ khóa: [bold blue]{', '.join(target_keywords)}[/]")
             except Exception as kw_err:
                 log.error(f"Lỗi khi xử lý tham số keywords: {kw_err}")
                 scan_errors.append(f"Lỗi xử lý từ khóa: {kw_err}")
                 target_keywords = []

        # --- Quyền Bot ---
        # <<< MODIFIED: Simplified required perms, read_message_history is key >>>
        required_perms_base = ["read_message_history", "embed_links", "view_channel", "attach_files"]
        if ENABLE_REACTION_SCAN: required_perms_base.append("read_message_history") # Already included, but explicit
        required_perms_threads = ["create_public_threads", "send_messages_in_threads"] # For logging
        required_perms_invites = ["manage_guild"]; required_perms_webhooks = ["manage_webhooks"]
        required_perms_integrations = ["manage_guild"]; required_perms_audit = ["view_audit_log"]
        bot_perms = server.me.guild_permissions
        missing_perms: List[str] = []
        for perm in required_perms_base:
            if not getattr(bot_perms, perm, False): missing_perms.append(perm)
        if missing_perms:
            perms_str = ', '.join(missing_perms)
            log.error(f"{e('error')} Bot thiếu quyền cơ bản: {perms_str}")
            await ctx.send(f"{e('error')} Bot thiếu quyền cơ bản cần thiết: `{perms_str}`", delete_after=30)
            ctx.command.reset_cooldown(ctx)
            raise commands.BotMissingPermissions(missing_perms)
        else:
             log.info(f"{e('success')} Quyền cơ bản (Read History, View Channel, Embed, Attach) được cấp.")

        can_create_log_thread = bot_perms.create_public_threads and bot_perms.send_messages_in_threads
        if not can_create_log_thread and log_thread_this_command:
             if not bot_perms.send_messages_in_threads:
                 scan_errors.append("Cảnh báo: Bot có thể không gửi được log vào luồng do thiếu quyền 'Send Messages in Threads'.")
                 log.warning("Bot thiếu quyền 'Send Messages in Threads', gửi log vào luồng có thể thất bại.")

        can_scan_invites = bot_perms.manage_guild; can_scan_webhooks = bot_perms.manage_webhooks
        can_scan_integrations = bot_perms.manage_guild; can_scan_audit_log = bot_perms.view_audit_log
        # <<< MODIFIED: Reaction scan check is simpler >>>
        can_scan_reactions = ENABLE_REACTION_SCAN and bot_perms.read_message_history
        if ENABLE_REACTION_SCAN and not can_scan_reactions:
             scan_errors.append("Bỏ qua quét Biểu cảm: Thiếu quyền 'Read Message History'.")
             log.warning("Bỏ qua quét Biểu cảm (thiếu Read Message History)")
        elif ENABLE_REACTION_SCAN:
             log.info(f"{e('reaction')} Quyền quét biểu cảm (Read Message History) được cấp.")

        can_scan_archived_threads = bot_perms.read_message_history or bot_perms.manage_threads
        if not can_scan_archived_threads:
             scan_errors.append("Thiếu quyền 'Read Message History' hoặc 'Manage Threads', sẽ bỏ qua luồng lưu trữ.")
             log.warning("Thiếu quyền xem luồng lưu trữ, sẽ bỏ qua.")
        if not can_scan_invites: scan_errors.append("Thiếu quyền 'Manage Server', sẽ bỏ qua lời mời."); log.warning("Bỏ qua lời mời (thiếu Manage Server)")
        if not can_scan_webhooks: scan_errors.append("Thiếu quyền 'Manage Webhooks', sẽ bỏ qua webhooks."); log.warning("Bỏ qua webhooks (thiếu Manage Webhooks)")
        if not can_scan_integrations: scan_errors.append("Thiếu quyền 'Manage Server', sẽ bỏ qua tích hợp."); log.warning("Bỏ qua tích hợp (thiếu Manage Server)")
        if not can_scan_audit_log: scan_errors.append("Thiếu quyền 'View Audit Log', sẽ bỏ qua Audit Log."); log.warning("Bỏ qua Audit Log (thiếu View Audit Log)")


        # --- Khởi tạo cấu trúc dữ liệu ---
        overall_total_message_count = 0; processed_channels_count = 0; processed_threads_count = 0
        skipped_channels_count = 0; skipped_threads_count = 0
        # <<< MODIFIED: Type hint includes VoiceChannel >>>
        accessible_channels: List[Union[discord.TextChannel, discord.VoiceChannel]] = []
        channel_details: List[Dict[str, Any]] = []
        user_activity: Dict[int, Dict[str, Any]] = defaultdict(lambda: {'first_seen': None, 'last_seen': None, 'message_count': 0, 'is_bot': False, 'link_count': 0, 'image_count': 0, 'emoji_count': 0, 'sticker_count': 0})
        invites_data: List[discord.Invite] = []; webhooks_data: List[discord.Webhook] = []
        integrations_data: List[discord.Integration] = []
        audit_log_entries_added = 0; audit_log_scan_duration = datetime.timedelta(0)
        permission_audit_results: Dict[str, List[Dict[str, Any]]] = {"roles_with_admin": [], "risky_everyone_overwrites": [], "other_risky_role_perms": []}
        role_change_stats: Dict[str, Dict[str, Counter]] = defaultdict(lambda: {"added": Counter(), "removed": Counter()})
        user_role_changes: Dict[int, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: {"added": 0, "removed": 0}))
        overall_total_reaction_count = 0
        reaction_emoji_counts: Counter = Counter()
        invite_usage_counts: Counter = Counter()
        oldest_members_data: List[Dict[str, Any]] = []
        user_link_counts: Counter = Counter()
        user_image_counts: Counter = Counter()
        user_emoji_counts: Counter = Counter()
        user_sticker_counts: Counter = Counter()

        # Lấy dữ liệu ban đầu từ cache
        current_members_list = list(server.members)
        initial_member_status_counts: Counter = Counter(str(m.status) for m in current_members_list)
        channel_counts: Counter = Counter(c.type for c in server.channels)
        all_roles_list: List[discord.Role] = sorted([r for r in server.roles if not r.is_default()], key=lambda r: r.position, reverse=True)

        # --- Lọc kênh Text & Voice & Kiểm tra quyền --- # <<< MODIFIED: Includes Voice >>>
        log.info(f"{e('info')} Đang lọc kênh text & voice trong [cyan]{server.name}[/]...")
        skipped_channels_count = 0
        # <<< MODIFIED: Include server.voice_channels in the list to iterate over >>>
        channels_to_scan = server.text_channels + server.voice_channels
        log.info(f"Tổng cộng {len(channels_to_scan)} kênh text/voice tiềm năng.")

        for channel in channels_to_scan:
            # <<< MODIFIED: Check specific types explicitly >>>
            if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
                 log.debug(f"Bỏ qua kênh '{channel.name}' ({channel.id}) loại {channel.type} - không phải Text hoặc Voice.")
                 continue # Skip categories, stages, forums etc for message scanning

            perms = channel.permissions_for(server.me)
            channel_type_emoji = utils.get_channel_type_emoji(channel, bot) # Get emoji for logging

            # <<< MODIFIED: Permissions check is now key >>>
            if perms.read_message_history and perms.view_channel:
                accessible_channels.append(channel)
                log.debug(f"Kênh {channel_type_emoji} '{channel.name}' ({channel.id}) có thể truy cập.")
            else:
                skipped_channels_count += 1
                reason = []
                if not perms.view_channel: reason.append("Thiếu View Channel")
                if not perms.read_message_history: reason.append("Thiếu Read History")
                reason_str = " và ".join(reason) if reason else "Lý do không xác định"
                scan_errors.append(f"Kênh {channel_type_emoji} #{channel.name}: Bỏ qua ({reason_str}).")
                log.warning(f"Bỏ qua kênh {channel_type_emoji} [yellow]#{channel.name}[/]: {reason_str}")
                # Add minimal detail for skipped channels
                channel_details.append({
                    "type": str(channel.type), # Store type as string
                    "name": channel.name,
                    "id": channel.id,
                    "created_at": channel.created_at,
                    "category": getattr(channel.category, 'name', "N/A"),
                    "category_id": getattr(channel.category, 'id', None),
                    "error": f"Bỏ qua do {reason_str}",
                    "processed": False,
                    "message_count": 0,
                    "reaction_count": 0
                 })


        log.info(f"Tìm thấy [green]{len(accessible_channels)}[/] kênh text/voice có thể quét, [yellow]{skipped_channels_count}[/] bị bỏ qua.")
        total_accessible_channels = len(accessible_channels)

        if total_accessible_channels == 0 and skipped_channels_count > 0:
             await initial_status_msg.edit(content=f"{e('error')} Không thể quét kênh text/voice nào do thiếu quyền.")
        elif total_accessible_channels == 0:
             await initial_status_msg.edit(content=f"{e('info')} Không tìm thấy kênh text/voice nào có thể truy cập.")

        if total_accessible_channels > 0:
            reaction_notice = f"\n{e('reaction')} Đang quét biểu cảm (có thể chậm)..." if can_scan_reactions else ""
            await initial_status_msg.edit(content=(
                f"{e('success')} {total_accessible_channels} kênh text/voice có thể quét.\n"
                f"{e('loading')} Bắt đầu quét TOÀN BỘ tin nhắn (Kênh & Luồng)...{reaction_notice}\n"
                f"{e('info')} Log chi tiết -> {log_thread_this_command.mention}" if log_thread_this_command else ""
            ))

        # --- Vòng lặp quét KÊNH và LUỒNG ---
        status_message = await ctx.send(f"{e('loading')} Chuẩn bị quét...")
        last_status_update_time = overall_start_time
        # <<< MODIFIED: Shorter update interval >>>
        update_interval_seconds = 30 # Update status more frequently

        # --- Regex cho đếm mới ---
        url_regex = re.compile(r'https?://\S+')
        emoji_regex = re.compile(r'<a?:[a-zA-Z0-9_]+:[0-9]+>|[\U00010000-\U0010ffff]') # Custom + Unicode

        current_channel_index = 0
        for channel in accessible_channels:
            current_channel_index += 1
            channel_message_count = 0
            channel_reaction_count = 0
            channel_scan_start_time = discord.utils.utcnow()
            channel_processed_flag = False
            channel_error: Optional[str] = None
            author_counter_channel: Counter = Counter()
            channel_threads_data = []
            current_channel_thread_count = 0
            current_channel_skipped_threads = 0
            channel_scan_type_note = "(Quét toàn bộ)" # REMOVED: Incremental logic

            # <<< MODIFIED: Determine channel type name/emoji explicitly >>>
            channel_type_emoji = utils.get_channel_type_emoji(channel, bot)
            channel_type_name = "Voice" if isinstance(channel, discord.VoiceChannel) else "Text"

            log.info(f"[bold]({current_channel_index}/{total_accessible_channels})[/bold] Đang quét kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/] ({channel.id})")
            log.info(f"  {e('stats')} {channel_scan_type_note} lịch sử kênh {channel_type_emoji} [cyan]#{channel.name}[/]")

            try:
                status_text = (
                    f"{e('loading')} Kênh: {channel_type_emoji} #{channel.name} ({current_channel_index}/{total_accessible_channels}) {channel_scan_type_note}\n"
                    f"{e('stats')} Kênh: 0 | {e('thread')} Luồng: ... | {e('stats')} Tổng: {overall_total_message_count:,}\n"
                    f"{e('members')} Users: {len(user_activity)} | {e('clock')} TG kênh: 0 giây"
                )
                try: await status_message.edit(content=status_text)
                except (discord.NotFound, discord.HTTPException):
                    log.warning("Tin nhắn trạng thái không tìm thấy, gửi lại.")
                    status_message = await ctx.send(status_text)
                last_status_update_time = discord.utils.utcnow()

                # REMOVED: Incremental logic, always scan full history
                # <<< MODIFIED: Added error handling for history iterator >>>
                try:
                    message_iterator = channel.history(limit=None)
                except discord.Forbidden:
                     # This specific Forbidden should ideally be caught by the initial permission check,
                     # but handle it here as a failsafe for the channel loop.
                     raise discord.Forbidden(None, f"Thiếu quyền read_message_history cho kênh {channel_type_emoji} #{channel.name} ngay cả sau khi kiểm tra ban đầu.") # Re-raise

                async for message in message_iterator:
                    timestamp = message.created_at
                    author_id = message.author.id
                    is_bot = message.author.bot

                    channel_message_count += 1
                    overall_total_message_count += 1

                    user_data = user_activity[author_id]
                    user_data['message_count'] += 1
                    if user_data['first_seen'] is None or timestamp < user_data['first_seen']: user_data['first_seen'] = timestamp
                    if user_data['last_seen'] is None or timestamp > user_data['last_seen']:
                        user_data['last_seen'] = timestamp
                    if is_bot: user_data['is_bot'] = True
                    else: author_counter_channel[author_id] += 1

                    # Đếm mới (chỉ cho user thật)
                    if not is_bot:
                        link_count = len(url_regex.findall(message.content))
                        if link_count > 0:
                            user_link_counts[author_id] += link_count
                            user_data['link_count'] += link_count
                        image_count = sum(1 for att in message.attachments if att.content_type and att.content_type.startswith('image/'))
                        if image_count > 0:
                             user_image_counts[author_id] += image_count
                             user_data['image_count'] += image_count
                        emoji_count = len(emoji_regex.findall(message.content))
                        if emoji_count > 0:
                             user_emoji_counts[author_id] += emoji_count
                             user_data['emoji_count'] += emoji_count
                        sticker_count = len(message.stickers)
                        if sticker_count > 0:
                             user_sticker_counts[author_id] += sticker_count
                             user_data['sticker_count'] += sticker_count

                    # Đếm Từ khóa
                    if target_keywords and message.content:
                         message_content_lower = message.content.lower()
                         for keyword in target_keywords:
                             count_in_msg = message_content_lower.count(keyword)
                             if count_in_msg > 0:
                                 keyword_counts[keyword] += count_in_msg
                                 channel_keyword_counts[channel.id][keyword] += count_in_msg
                                 if not is_bot:
                                     user_keyword_counts[author_id][keyword] += count_in_msg

                    # Đếm Reaction
                    if can_scan_reactions and message.reactions:
                        try:
                            current_message_reaction_count = 0
                            for reaction in message.reactions:
                                count = reaction.count
                                current_message_reaction_count += count
                                emoji_key = str(reaction.emoji)
                                reaction_emoji_counts[emoji_key] += count
                            channel_reaction_count += current_message_reaction_count
                            overall_total_reaction_count += current_message_reaction_count
                        except Exception as react_err:
                            log.warning(f"Lỗi xử lý reaction msg {message.id} kênh {channel.id}: {react_err}", exc_info=False)

                    # Cập nhật trạng thái định kỳ
                    now = discord.utils.utcnow()
                    if (now - last_status_update_time).total_seconds() > update_interval_seconds:
                        channel_elapsed = now - channel_scan_start_time
                        # Estimate thread count only for text channels
                        est_thread_count_str = f"~{len(channel.threads)}" if isinstance(channel, discord.TextChannel) else "N/A"
                        status_text = (
                           f"{e('loading')} Kênh: {channel_type_emoji} #{channel.name} ({current_channel_index}/{total_accessible_channels}) {channel_scan_type_note}\n"
                           f"{e('stats')} Kênh: {channel_message_count:,} | {e('thread')} Luồng: {est_thread_count_str} | {e('stats')} Tổng: {overall_total_message_count:,}\n"
                           f"{e('members')} Users: {len(user_activity)} | {e('clock')} TG kênh: {utils.format_timedelta(channel_elapsed)}"
                        )
                        if can_scan_reactions: status_text += f" | {e('reaction')} Reacts: {overall_total_reaction_count:,}"
                        try: await status_message.edit(content=status_text)
                        except discord.NotFound:
                            log.warning("Tin nhắn trạng thái không tìm thấy khi quét kênh, gửi lại.")
                            status_message = await ctx.send(status_text)
                        except discord.HTTPException as http_err:
                            log.warning(f"Cập nhật trạng thái thất bại (HTTP {http_err.status}) cho kênh #{channel.name}.")
                            await asyncio.sleep(5)
                        except Exception as e_stat:
                            log.error(f"Lỗi không xác định khi cập nhật trạng thái: {e_stat}", exc_info=True)
                        finally: last_status_update_time = now
                # --- Kết thúc vòng lặp quét tin nhắn KÊNH ---

                channel_scan_duration = discord.utils.utcnow() - channel_scan_start_time
                reaction_log_part = f", {channel_reaction_count:,} reactions" if can_scan_reactions else ""
                # REMOVED: Incremental logic
                log.info(f"  {e('success')} Hoàn thành quét toàn bộ kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/]: {channel_message_count:,} tin nhắn{reaction_log_part} trong [magenta]{utils.format_timedelta(channel_scan_duration)}[/].")

                # REMOVED: Database update logic for scan state

                # Thu thập chi tiết kênh
                log.info(f"  {e('info')} Đang thu thập chi tiết kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/]...")
                top_chatter_info = "Không có (hoặc chỉ bot)"
                top_chatter_roles = "N/A"
                if author_counter_channel:
                    try:
                        top_author_id, top_count = author_counter_channel.most_common(1)[0]
                        user = server.get_member(top_author_id)
                        if not user:
                             try: user = await server.fetch_member(top_author_id)
                             except (discord.NotFound, discord.HTTPException): user = await utils.fetch_user_data(None, top_author_id, bot_ref=bot)

                        if user:
                            # <<< MODIFIED: More concise top chatter info >>>
                            top_chatter_info = f"{user.mention} - {top_count:,} tin"
                            if isinstance(user, discord.Member):
                                role_mentions = [r.mention for r in user.roles if not r.is_default()]
                                roles_str = ", ".join(role_mentions) if role_mentions else "Không có role"
                                if len(roles_str) > 150: roles_str = roles_str[:150] + "..."
                                top_chatter_roles = roles_str
                            else: top_chatter_roles = "N/A (Không còn trong server)"
                        else: top_chatter_info = f"ID: `{top_author_id}` (Rời) - {top_count:,} tin" # Shorten 'left server'

                    except IndexError: log.warning(f"Lỗi IndexError khi lấy top chatter cho #{channel.name}. Counter: {author_counter_channel}"); top_chatter_info = f"{e('error')} Lỗi Index lấy top chatter"
                    except discord.NotFound: log.warning(f"Không tìm thấy member/user {top_author_id} khi lấy top chatter cho #{channel.name}."); top_chatter_info = f"ID: `{top_author_id}` (Rời) - {top_count:,} tin"
                    except discord.HTTPException as http_err: log.error(f"Lỗi HTTP ({http_err.status}) khi lấy top chatter cho #{channel.name}: {http_err.text}"); top_chatter_info = f"{e('error')} Lỗi HTTP lấy top chatter"
                    except Exception as chatter_err: log.error(f"Lỗi lấy top chatter cho #{channel.name}: {chatter_err}", exc_info=True); top_chatter_info = f"{e('error')} Lỗi lấy top chatter"


                first_messages_log: List[str] = []
                try:
                    # Fetch slightly more in case of initial system messages etc.
                    async for msg in channel.history(limit=reporting.FIRST_MESSAGES_LIMIT + 10, oldest_first=True):
                        author_display = f"{msg.author.display_name}" if msg.author else "Không rõ"
                        timestamp_str = msg.created_at.strftime('%d/%m/%y %H:%M UTC')
                        content_preview = msg.content[:reporting.FIRST_MESSAGES_CONTENT_PREVIEW].replace('`', "'").replace('\n', ' ')
                        if len(msg.content) > reporting.FIRST_MESSAGES_CONTENT_PREVIEW: content_preview += "..."
                        if not content_preview and msg.attachments: content_preview = "[File đính kèm]"
                        elif not content_preview and msg.embeds: content_preview = "[Embed]"
                        elif not content_preview and msg.stickers: content_preview = "[Sticker]"
                        elif not content_preview: content_preview = "[Nội dung trống]"
                        first_messages_log.append(f"[`{timestamp_str}`] **{author_display}**: {content_preview}")
                        if len(first_messages_log) >= reporting.FIRST_MESSAGES_LIMIT:
                            break
                    if not first_messages_log: first_messages_log.append("`[N/A]` Kênh trống hoặc không lấy được tin nhắn đầu.")
                except discord.Forbidden:
                    first_messages_log = [f"`[LỖI]` {e('error')} Thiếu quyền đọc lịch sử."]
                    channel_error = channel_error or "Thiếu quyền lấy tin nhắn đầu."
                except Exception as e_first:
                    log.error(f"Lỗi lấy tin nhắn đầu cho #{channel.name}: {e_first}")
                    first_messages_log = [f"`[LỖI]` {e('error')} Lỗi không xác định: {e_first}"]
                    channel_error = channel_error or f"Lỗi lấy tin nhắn đầu: {e_first}"

                # <<< MODIFIED: Get topic/nsfw/slowmode based on actual channel type >>>
                channel_topic = "N/A"
                channel_nsfw_str = "N/A"
                channel_slowmode_str = "N/A"

                if isinstance(channel, discord.TextChannel):
                     channel_topic = getattr(channel, 'topic', None)
                     if channel_topic is None: channel_topic = "Không có"
                     if len(channel_topic) > 150: channel_topic = channel_topic[:150] + "..."
                     channel_nsfw_str = f"{e('success')} Có" if channel.is_nsfw() else f"{e('error')} Không"
                     channel_slowmode_str = f"{channel.slowmode_delay} giây" if channel.slowmode_delay > 0 else "Không"
                elif isinstance(channel, discord.VoiceChannel):
                     channel_topic = "N/A (Voice Channel)"
                     channel_nsfw_str = f"{e('success')} Có" if channel.is_nsfw() else f"{e('error')} Không" # Voice channels can be NSFW
                     channel_slowmode_str = "N/A (Voice Channel)"


                channel_category = channel.category.name if channel.category else "Ngoài danh mục"
                channel_category_id = channel.category.id if channel.category else None

                # Lưu trữ thông tin kênh
                detail_entry = next((d for d in channel_details if d.get("id") == channel.id), None)
                update_data = {
                     "processed": True, "message_count": channel_message_count, "duration": channel_scan_duration,
                     "reaction_count": channel_reaction_count,
                     "topic": channel_topic, "nsfw": channel_nsfw_str, "slowmode": channel_slowmode_str,
                     "top_chatter": top_chatter_info, "top_chatter_roles": top_chatter_roles,
                     "first_messages_log": first_messages_log, "error": channel_error,
                     "scan_type_note": channel_scan_type_note, # Keep note, even if always "toàn bộ"
                     "threads_data": [] # Initialize threads data here
                }
                if detail_entry: detail_entry.update(update_data)
                else:
                    # Store channel type as string
                    channel_details.append({
                        "type": str(channel.type), "name": channel.name, "id": channel.id, "created_at": channel.created_at,
                        "category": channel_category, "category_id": channel_category_id, **update_data
                    })

                processed_channels_count += 1
                channel_processed_flag = True
                log.info(f"  {e('success')} Hoàn thành thu thập chi tiết kênh {channel_type_name} {channel_type_emoji} [cyan]#{channel.name}[/].")
                await asyncio.sleep(0.1) # Nghỉ ngắn

                # --- Quét Luồng (Threads) - ONLY FOR TEXT CHANNELS ---
                # <<< MODIFIED: Added check for TextChannel >>>
                if isinstance(channel, discord.TextChannel):
                    log.info(f"  {e('thread')} Đang kiểm tra luồng trong kênh text {channel_type_emoji} [cyan]#{channel.name}[/]...")
                    active_threads = channel.threads
                    archived_threads_iterator = None
                    if can_scan_archived_threads:
                         try:
                              log.info(f"    Đang fetch luồng lưu trữ cho #{channel.name} (giới hạn 200)...")
                              archived_threads_iterator = channel.archived_threads(limit=200)
                         except discord.Forbidden:
                              log.warning(f"    Thiếu quyền fetch luồng lưu trữ trong #{channel.name}.")
                              scan_errors.append(f"Luồng Kênh #{channel.name}: Thiếu quyền xem luồng lưu trữ.")
                         except discord.HTTPException as e_http:
                              log.warning(f"    Lỗi mạng fetch luồng lưu trữ trong #{channel.name}: {e_http.status}")
                              scan_errors.append(f"Luồng Kênh #{channel.name}: Lỗi mạng ({e_http.status}) khi lấy luồng lưu trữ.")
                         except Exception as e_arch:
                              log.error(f"    Lỗi không xác định fetch luồng lưu trữ trong #{channel.name}: {e_arch}", exc_info=True)
                              scan_errors.append(f"Luồng Kênh #{channel.name}: Lỗi không xác định khi lấy luồng lưu trữ: {e_arch}")
                    else:
                        log.warning(f"    Bỏ qua luồng lưu trữ trong #{channel.name} do thiếu quyền.")

                    all_threads_in_channel: List[discord.Thread] = list(active_threads)
                    if archived_threads_iterator:
                         processed_archived_count = 0
                         try:
                              async for thread in archived_threads_iterator:
                                  all_threads_in_channel.append(thread)
                                  processed_archived_count += 1
                              if processed_archived_count > 0:
                                  log.info(f"    Đã fetch {processed_archived_count} luồng lưu trữ cho #{channel.name}.")
                         except Exception as e_iter:
                              log.error(f"    Lỗi khi duyệt luồng lưu trữ trong #{channel.name}: {e_iter}", exc_info=True)
                              scan_errors.append(f"Luồng Kênh #{channel.name}: Lỗi khi duyệt luồng lưu trữ: {e_iter}")

                    total_threads_found = len(all_threads_in_channel)
                    if total_threads_found > 0:
                         log.info(f"  Tìm thấy {total_threads_found} luồng trong [cyan]#{channel.name}[/]. Bắt đầu quét...")
                         try: await status_message.edit(content=f"{e('loading')} Kênh: {channel_type_emoji} #{channel.name}\n{e('thread')} Đang quét luồng (0/{total_threads_found})...")
                         except: pass

                         thread_index = 0
                         for thread in all_threads_in_channel:
                              thread_index += 1
                              thread_message_count = 0
                              thread_reaction_count = 0
                              thread_scan_start_time = discord.utils.utcnow()
                              error_in_thread: Optional[str] = None
                              thread_scan_type_note = "(Toàn bộ)" # REMOVED: Incremental logic

                              thread_perms = thread.permissions_for(server.me)
                              if not thread_perms.view_channel or not thread_perms.read_message_history:
                                   reason = "Thiếu quyền View" if not thread_perms.view_channel else "Thiếu quyền Read History"
                                   log.warning(f"    Bỏ qua luồng '{thread.name}' ({thread.id}): {reason}.")
                                   scan_errors.append(f"Luồng '{thread.name}' ({thread.id}): Bỏ qua ({reason}).")
                                   skipped_threads_count += 1
                                   current_channel_skipped_threads += 1
                                   channel_threads_data.append({"id": thread.id, "name": thread.name, "archived": thread.archived, "error": f"Bỏ qua do {reason}", "message_count": 0, "reaction_count": 0})
                                   continue

                              log.info(f"    [bold]({thread_index}/{total_threads_found})[/bold] Đang quét luồng [magenta]'{thread.name}'[/] ({thread.id})...")
                              log.info(f"      {e('stats')} Quét toàn bộ lịch sử luồng '{thread.name}'")

                              try:
                                   try: await status_message.edit(content=f"{e('loading')} Kênh: {channel_type_emoji} #{channel.name}\n{e('thread')} Luồng: {thread_index}/{total_threads_found} {thread_scan_type_note}...")
                                   except: pass

                                   # REMOVED: Incremental logic
                                   thread_message_iterator = thread.history(limit=None)
                                   async for message in thread_message_iterator:
                                        timestamp = message.created_at
                                        author_id = message.author.id
                                        is_bot = message.author.bot
                                        thread_message_count += 1
                                        overall_total_message_count += 1

                                        user_data = user_activity[author_id]
                                        user_data['message_count'] += 1
                                        if user_data['first_seen'] is None or timestamp < user_data['first_seen']: user_data['first_seen'] = timestamp
                                        if user_data['last_seen'] is None or timestamp > user_data['last_seen']: user_data['last_seen'] = timestamp
                                        if is_bot: user_data['is_bot'] = True

                                        # Đếm mới (chỉ user thật)
                                        if not is_bot:
                                            link_count = len(url_regex.findall(message.content))
                                            if link_count > 0: user_link_counts[author_id] += link_count; user_data['link_count'] += link_count
                                            image_count = sum(1 for att in message.attachments if att.content_type and att.content_type.startswith('image/'))
                                            if image_count > 0: user_image_counts[author_id] += image_count; user_data['image_count'] += image_count
                                            emoji_count = len(emoji_regex.findall(message.content))
                                            if emoji_count > 0: user_emoji_counts[author_id] += emoji_count; user_data['emoji_count'] += emoji_count
                                            sticker_count = len(message.stickers)
                                            if sticker_count > 0: user_sticker_counts[author_id] += sticker_count; user_data['sticker_count'] += sticker_count

                                        # Đếm Từ khóa trong luồng
                                        if target_keywords and message.content:
                                             message_content_lower = message.content.lower()
                                             for keyword in target_keywords:
                                                 count_in_msg = message_content_lower.count(keyword)
                                                 if count_in_msg > 0:
                                                     keyword_counts[keyword] += count_in_msg
                                                     thread_keyword_counts[thread.id][keyword] += count_in_msg
                                                     if not is_bot:
                                                         user_keyword_counts[author_id][keyword] += count_in_msg

                                        # Đếm Reaction Luồng
                                        if can_scan_reactions and message.reactions:
                                            try:
                                                current_message_reaction_count = 0
                                                for reaction in message.reactions:
                                                    count = reaction.count
                                                    current_message_reaction_count += count
                                                    emoji_key = str(reaction.emoji)
                                                    reaction_emoji_counts[emoji_key] += count
                                                thread_reaction_count += current_message_reaction_count
                                                overall_total_reaction_count += current_message_reaction_count
                                            except Exception as react_err_thread:
                                                 log.warning(f"Lỗi xử lý reaction msg {message.id} luồng {thread.id}: {react_err_thread}", exc_info=False)

                                   # Kết thúc quét luồng này
                                   thread_scan_duration = discord.utils.utcnow() - thread_scan_start_time
                                   reaction_log_part_thread = f", {thread_reaction_count:,} reactions" if can_scan_reactions else ""
                                   log.info(f"      {e('success')} Hoàn thành quét toàn bộ luồng [magenta]'{thread.name}'[/]: {thread_message_count:,} tin nhắn{reaction_log_part_thread} trong [magenta]{utils.format_timedelta(thread_scan_duration)}[/].")
                                   processed_threads_count += 1
                                   current_channel_thread_count += 1

                                   # REMOVED: Database update logic for thread scan state

                              except discord.Forbidden as e_perm_thread:
                                   error_in_thread = f"Thiếu quyền nghiêm trọng khi quét luồng: {e_perm_thread.text}";
                                   log.error(f"    {e('error')} {error_in_thread} cho luồng '{thread.name}'");
                                   scan_errors.append(f"Luồng '{thread.name}': {error_in_thread}"); skipped_threads_count += 1; current_channel_skipped_threads += 1
                              except discord.HTTPException as e_http_thread:
                                   error_in_thread = f"Lỗi mạng nghiêm trọng luồng (HTTP {e_http_thread.status}): {e_http_thread.text}";
                                   log.error(f"    {e('error')} {error_in_thread} cho luồng '{thread.name}'");
                                   scan_errors.append(f"Luồng '{thread.name}': {error_in_thread}"); skipped_threads_count += 1; current_channel_skipped_threads += 1; await asyncio.sleep(3)
                              except Exception as e_unkn_thread:
                                   error_in_thread = f"Lỗi không xác định khi xử lý luồng: {e_unkn_thread}";
                                   log.error(f"    {e('error')} {error_in_thread} cho luồng '{thread.name}'", exc_info=True);
                                   scan_errors.append(f"Luồng '{thread.name}': {error_in_thread}"); skipped_threads_count += 1; current_channel_skipped_threads += 1

                              # Thêm thông tin luồng vào dữ liệu kênh
                              channel_threads_data.append({
                                  "id": thread.id, "name": thread.name,
                                  "owner_id": thread.owner_id,
                                  "created_at": thread.created_at.isoformat() if thread.created_at else None,
                                  "archived": thread.archived, "locked": thread.locked,
                                  "message_count": thread_message_count,
                                  "reaction_count": thread_reaction_count,
                                  "scan_duration_seconds": thread_scan_duration.total_seconds(),
                                  "scan_type_note": thread_scan_type_note,
                                  "error": error_in_thread
                              })
                              await asyncio.sleep(0.1) # Nghỉ ngắn giữa các luồng

                         log.info(f"  {e('success')} Hoàn thành quét {current_channel_thread_count} luồng trong kênh #{channel.name} ({current_channel_skipped_threads} bị bỏ qua).")
                         # Gắn dữ liệu luồng vào chi tiết kênh
                         # <<< MODIFIED: Find the correct detail entry before updating >>>
                         detail_entry_for_thread = next((d for d in channel_details if d.get("id") == channel.id), None)
                         if detail_entry_for_thread:
                             detail_entry_for_thread["threads_data"] = channel_threads_data
                         else:
                             log.warning(f"Không tìm thấy detail entry cho kênh #{channel.name} để gắn dữ liệu luồng.")

                    else:
                        log.info(f"  {e('info')} Không tìm thấy luồng hoặc không thể truy cập trong kênh text [cyan]#{channel.name}[/].")
                        # <<< MODIFIED: Ensure thread data list exists even if empty >>>
                        detail_entry_for_thread = next((d for d in channel_details if d.get("id") == channel.id), None)
                        if detail_entry_for_thread and "threads_data" not in detail_entry_for_thread:
                               detail_entry_for_thread["threads_data"] = []

                # --- End Thread Scan Block ---
                # <<< ADDED: Log if channel is Voice and thus skipped thread scan >>>
                elif isinstance(channel, discord.VoiceChannel):
                     log.info(f"  {e('thread')} Bỏ qua quét luồng cho kênh voice {channel_type_emoji} [cyan]#{channel.name}[/].")


            # Xử lý lỗi nghiêm trọng khi quét KÊNH
            except discord.Forbidden as e_perm:
                # <<< MODIFIED: Include channel type in error message >>>
                channel_error_msg = f"Bỏ qua kênh {channel_type_name} #{channel.name} - Thiếu quyền nghiêm trọng: {e_perm.text}"
                log.error(f"{e('error')} {channel_error_msg}")
                scan_errors.append(channel_error_msg)
                existing_detail = next((item for item in channel_details if item.get('id') == channel.id), None)
                if existing_detail:
                    existing_detail["error"] = (existing_detail.get("error", "") + f"\nFATAL SCAN ERROR: {channel_error_msg}").strip()
                    existing_detail["processed"] = False
                else:
                     channel_details.append({"type": str(channel.type), "name": channel.name, "id": channel.id, "error": f"FATAL SCAN ERROR: {channel_error_msg}", "processed": False, "message_count": channel_message_count, "reaction_count": channel_reaction_count})
                if channel_processed_flag: processed_channels_count -= 1
                # <<< MODIFIED: More reliable way to track skipped count >>>
                # Check if it wasn't already added as skipped before the fatal error
                if not any(d['id'] == channel.id for d in channel_details if not d.get('processed')):
                    skipped_channels_count += 1
                try: await status_message.edit(content=f"{e('error')} **Lỗi quyền {channel_type_name}:** #{channel.name}")
                except: pass
                await asyncio.sleep(2)
            except discord.HTTPException as e_http:
                 # <<< MODIFIED: Include channel type in error message >>>
                channel_error_msg = f"Lỗi mạng nghiêm trọng kênh {channel_type_name} #{channel.name} (HTTP {e_http.status}): {e_http.text}"
                log.error(f"{e('error')} {channel_error_msg}", exc_info=True)
                scan_errors.append(channel_error_msg)
                existing_detail = next((item for item in channel_details if item.get('id') == channel.id), None)
                error_prefix = "PARTIAL SCAN ERROR: " if channel_processed_flag else "FATAL SCAN ERROR: "
                if existing_detail:
                    existing_detail["error"] = (existing_detail.get("error", "") + f"\n{error_prefix}{channel_error_msg}").strip()
                else: channel_details.append({"type": str(channel.type), "name": channel.name, "id": channel.id, "error": f"{error_prefix}{channel_error_msg}", "message_count": channel_message_count, "reaction_count": channel_reaction_count, "processed": channel_processed_flag})
                try: await ctx.send(f"{e('error')} {channel_error_msg}. Dữ liệu kênh #{channel.name} có thể không đầy đủ.")
                except: pass
                await asyncio.sleep(3)
            except Exception as e_unkn:
                 # <<< MODIFIED: Include channel type in error message >>>
                channel_error_msg = f"Lỗi không xác định khi xử lý kênh {channel_type_name} #{channel.name}: {e_unkn}"
                log.error(f"{e('error')} {channel_error_msg}", exc_info=True)
                scan_errors.append(channel_error_msg)
                existing_detail = next((item for item in channel_details if item.get('id') == channel.id), None)
                error_prefix = "PARTIAL SCAN ERROR: " if channel_processed_flag else "FATAL SCAN ERROR: "
                if existing_detail: existing_detail["error"] = (existing_detail.get("error", "") + f"\n{error_prefix}{channel_error_msg}").strip()
                else: channel_details.append({"type": str(channel.type), "name": channel.name, "id": channel.id, "error": f"{error_prefix}{channel_error_msg}", "message_count": channel_message_count, "reaction_count": channel_reaction_count, "processed": channel_processed_flag})
                try: await ctx.send(f"{e('error')} {channel_error_msg}. Dữ liệu kênh #{channel.name} có thể không đầy đủ.")
                except: pass
                await asyncio.sleep(2)

        # --- Hoàn tất quét tất cả kênh và luồng ---
        overall_scan_end_time = discord.utils.utcnow()
        overall_duration = overall_scan_end_time - overall_start_time
        try: await status_message.delete()
        except: pass
        try: await initial_status_msg.edit(content=f"{e('success')} Hoàn tất quét dữ liệu. Đang xử lý và tạo báo cáo...")
        except (discord.NotFound, discord.HTTPException): pass

        log.info(f"\n--- [bold green]{e('stats')} Xử lý Dữ liệu & Tạo Báo cáo cho {server.name}[/bold green] ---")
        log.info(f"{e('clock')} Tổng thời gian quét: [bold magenta]{utils.format_timedelta(overall_duration, high_precision=True)}[/bold magenta]")
        # <<< MODIFIED: More accurate channel logging >>>
        log.info(f"{e('text_channel')}/{e('voice_channel')} Kênh Text/Voice đã xử lý: {processed_channels_count}, Bỏ qua: {skipped_channels_count}")
        log.info(f"{e('thread')} Luồng đã xử lý: {processed_threads_count}, Bỏ qua: {skipped_threads_count}")
        log.info(f"{e('stats')} Tổng tin nhắn đã quét (Kênh+Luồng): {overall_total_message_count:,}")
        if can_scan_reactions: log.info(f"{e('reaction')} Tổng biểu cảm đã quét: {overall_total_reaction_count:,}")
        log.info(f"{e('members')} Tổng Users có hoạt động: {len(user_activity)}")
        log.info(f"{e('link')} Tổng Links: {sum(user_link_counts.values()):,}, {e('image')} Images: {sum(user_image_counts.values()):,}, {e('reaction')} Emojis: {sum(user_emoji_counts.values()):,}, {e('sticker')} Stickers: {sum(user_sticker_counts.values()):,}")


        # --- Xử lý dữ liệu phụ trợ ---
        log.info(f"{e('loading')} Đang fetch dữ liệu phụ trợ (Roles, Boosters, Static VC Info, Invites, Webhooks, Integrations, Audit)...")
        boosters: List[discord.Member] = []
        # MODIFIED: This is now for *static* voice/stage channel info, not scanned messages
        voice_channel_static_data: List[Dict[str, Any]] = []
        skipped_voice_info_count = 0

        for member in current_members_list:
            if member.premium_since is not None: boosters.append(member)
        log.info(f"{e('boost')} Tìm thấy {len(boosters)} boosters (từ cache).")
        log.info(f"{e('role')} Tìm thấy {len(all_roles_list)} roles (từ cache).")

        # --- Collect static voice/stage channel info ---
        # <<< MODIFIED: Use existing server properties >>>
        static_voice_stage_channels = server.voice_channels + server.stage_channels
        log.info(f"Thu thập thông tin tĩnh cho {len(static_voice_stage_channels)} kênh voice/stage...")
        for vc in static_voice_stage_channels:
             if vc.permissions_for(server.me).view_channel:
                 voice_channel_static_data.append({
                     "channel_obj": vc, # Keep object ref if needed later
                     "name": vc.name, "id": vc.id, "type": str(vc.type), # Add type
                     "category": vc.category.name if vc.category else "Ngoài danh mục",
                     "category_id": vc.category.id if vc.category else None,
                     "user_limit": vc.user_limit if vc.user_limit > 0 else "Không giới hạn",
                     "bitrate": f"{vc.bitrate // 1000} kbps", "created_at": vc.created_at
                 })
             else:
                 skipped_voice_info_count += 1
                 type_emoji = utils.get_channel_type_emoji(vc, bot)
                 scan_errors.append(f"Thông tin Kênh Voice/Stage {type_emoji} #{vc.name}: Bỏ qua (Thiếu quyền View).")
                 log.warning(f"Bỏ qua lấy thông tin kênh {type_emoji} #{vc.name}: Thiếu quyền View.")
        log.info(f"{e('voice_channel')}{e('stage')} Tìm thấy thông tin tĩnh cho {len(voice_channel_static_data)} kênh voice/stage ({skipped_voice_info_count} bị bỏ qua).")

        if can_scan_invites:
            try:
                 invites_data = await server.invites()
                 log.info(f"{e('invite')} Đã fetch {len(invites_data)} lời mời đang hoạt động.")
                 for inv in invites_data:
                     if inv.inviter and inv.uses is not None:
                          invite_usage_counts[inv.inviter.id] += inv.uses
                 if invite_usage_counts: log.info(f"Đã tính toán số lượt sử dụng cho {len(invite_usage_counts)} người mời.")
            except discord.Forbidden: log.warning("Thiếu quyền 'Manage Server' cho invites.")
            except discord.HTTPException as e_http: log.error(f"Lỗi mạng fetch invites: {e_http.status}"); scan_errors.append(f"Lỗi mạng khi lấy lời mời ({e_http.status}).")
            except Exception as e_inv: log.error(f"Lỗi không xác định fetch invites: {e_inv}", exc_info=True); scan_errors.append(f"Lỗi không xác định khi lấy lời mời.")
        if can_scan_webhooks:
            try: webhooks_data = await server.webhooks(); log.info(f"{e('webhook')} Đã fetch {len(webhooks_data)} webhooks.")
            except discord.Forbidden: log.warning("Thiếu quyền 'Manage Webhooks'.")
            except discord.HTTPException as e_http: log.error(f"Lỗi mạng fetch webhooks: {e_http.status}"); scan_errors.append(f"Lỗi mạng khi lấy webhooks ({e_http.status}).")
            except Exception as e_wh: log.error(f"Lỗi không xác định fetch webhooks: {e_wh}", exc_info=True); scan_errors.append(f"Lỗi không xác định khi lấy webhooks.")
        if can_scan_integrations:
            try: integrations_data = await server.integrations(); log.info(f"{e('integration')} Đã fetch {len(integrations_data)} tích hợp.")
            except discord.Forbidden: log.warning("Thiếu quyền 'Manage Server' cho integrations.")
            except discord.HTTPException as e_http: log.error(f"Lỗi mạng fetch integrations: {e_http.status}"); scan_errors.append(f"Lỗi mạng khi lấy tích hợp ({e_http.status}).")
            except Exception as e_int: log.error(f"Lỗi không xác định fetch integrations: {e_int}", exc_info=True); scan_errors.append(f"Lỗi không xác định khi lấy tích hợp.")

        # --- Lấy Top Thành viên Lâu Năm ---
        log.info(f"{e('calendar')} Đang xác định thành viên lâu năm nhất...")
        try:
            human_members_with_join = [m for m in current_members_list if not m.bot and m.joined_at is not None]
            human_members_with_join.sort(key=lambda m: m.joined_at)
            limit_oldest = 30
            for member in human_members_with_join[:limit_oldest]:
                 oldest_members_data.append({
                     "id": member.id, "mention": member.mention, "display_name": member.display_name, "joined_at": member.joined_at
                 })
            log.info(f"Đã xác định top {len(oldest_members_data)} thành viên lâu năm nhất (đã lọc bot).")
        except Exception as oldest_err:
            log.error(f"Lỗi khi xác định thành viên lâu năm: {oldest_err}", exc_info=True)
            scan_errors.append(f"Lỗi lấy top thành viên lâu năm: {oldest_err}")

        # --- Phân tích Quyền Nâng Cao ---
        log.info(f"[bold]{e('shield')} Bắt đầu phân tích quyền nâng cao...[/bold]")
        perm_audit_start_time = time.monotonic()
        try:
            log.info("  Kiểm tra roles có quyền Administrator...")
            for role in all_roles_list:
                if role.permissions.administrator:
                     permission_audit_results["roles_with_admin"].append({"id": str(role.id), "name": role.name, "position": role.position, "member_count": len(role.members)})

            log.info("  Kiểm tra quyền @everyone trên các kênh...")
            everyone_role = server.default_role
            # <<< MODIFIED: Check permissions on ALL channels bot can see, not just processed ones >>>
            channels_to_check_perm_ids = set()
            # Add successfully scanned channels
            channels_to_check_perm_ids.update(c_detail['id'] for c_detail in channel_details if c_detail.get('processed'))
            # Add skipped channels (maybe they have risky perms even if history wasn't read)
            channels_to_check_perm_ids.update(c_detail['id'] for c_detail in channel_details if not c_detail.get('processed') and not c_detail.get('error','').startswith("Bỏ qua do")) # Exclude permission errors
            # Add static voice/stage channels
            channels_to_check_perm_ids.update(vc['id'] for vc in voice_channel_static_data)
            # Add categories, forums etc.
            channels_to_check_perm_ids.update(c.id for c in server.channels if c.permissions_for(server.me).view_channel)

            channels_to_check_perm_obj = [server.get_channel(cid) for cid in channels_to_check_perm_ids if server.get_channel(cid)]
            log.info(f"  Sẽ kiểm tra quyền @everyone trên {len(channels_to_check_perm_obj)} kênh có thể xem.")

            risky_everyone_perms = {'send_messages': True, 'send_messages_in_threads': True, 'manage_messages': True, 'manage_channels': True, 'manage_roles': True, 'manage_webhooks': True, 'manage_threads': True, 'mention_everyone': True, 'administrator': True, 'kick_members': True, 'ban_members': True, 'attach_files': True, 'embed_links': True} # Added attach/embed
            for channel in channels_to_check_perm_obj:
                 # <<< MODIFIED: Check if channel HAS overwrites attribute >>>
                 if not hasattr(channel, 'overwrites_for'): continue
                 try:
                     overwrites = channel.overwrites_for(everyone_role)
                     # <<< MODIFIED: Handle case where overwrites might be None >>>
                     if overwrites is None: continue
                     found_risky_perms = {p: getattr(overwrites, p) for p, v in risky_everyone_perms.items() if getattr(overwrites, p, None) is True} # Check for explicit True
                     if found_risky_perms:
                          channel_type_str = utils.get_channel_type_emoji(channel, bot)
                          permission_audit_results["risky_everyone_overwrites"].append({"channel_id": str(channel.id), "channel_name": channel.name, "channel_type_emoji": channel_type_str, "permissions": found_risky_perms})
                 except Exception as ch_perm_err:
                      log.warning(f"Lỗi kiểm tra quyền @everyone cho kênh #{channel.name}: {ch_perm_err}")
                      scan_errors.append(f"Lỗi kiểm tra quyền @everyone kênh #{channel.name}: {ch_perm_err}")

            log.info("  Kiểm tra các role khác có quyền nguy hiểm...")
            risky_general_perms = {'manage_guild', 'manage_roles', 'manage_channels', 'manage_webhooks', 'kick_members', 'ban_members', 'mention_everyone', 'moderate_members', 'view_audit_log'} # Added moderate, view_audit
            admin_role_ids = {str(r['id']) for r in permission_audit_results["roles_with_admin"]}
            for role in all_roles_list:
                 if str(role.id) in admin_role_ids: continue
                 is_bot_role = role.is_bot_managed()
                 if is_bot_role: continue

                 found_risky_general = {p: getattr(role.permissions, p) for p in risky_general_perms if getattr(role.permissions, p, False)}
                 if found_risky_general:
                     permission_audit_results["other_risky_role_perms"].append({"role_id": str(role.id), "role_name": role.name, "position": role.position, "member_count": len(role.members), "permissions": found_risky_general})

            perm_audit_duration = time.monotonic() - perm_audit_start_time
            log.info(f"{e('success')} Hoàn thành phân tích quyền nâng cao trong [magenta]{perm_audit_duration:.2f}[/] giây.")
        except Exception as perm_err:
            log.error(f"{e('error')} Lỗi trong quá trình phân tích quyền: {perm_err}", exc_info=True)
            scan_errors.append(f"Lỗi phân tích quyền: {perm_err}")

        # --- Quét Audit Log ---
        newest_processed_audit_log_id: Optional[int] = None
        audit_log_entries_added = 0
        if can_scan_audit_log:
            audit_scan_start_time = discord.utils.utcnow()
            log.info(f"[bold]{e('shield')} Bắt đầu quét Audit Log...[/bold]")
            try:
                last_scanned_log_id = await database.get_newest_audit_log_id_from_db(server.id)
                log.info(f"  ID audit log cuối cùng đã quét từ DB: {last_scanned_log_id}")
                # <<< MODIFIED: Track more actions >>>
                actions_to_track = [
                    discord.AuditLogAction.kick, discord.AuditLogAction.ban, discord.AuditLogAction.unban,
                    discord.AuditLogAction.member_role_update, discord.AuditLogAction.member_update,
                    discord.AuditLogAction.role_create, discord.AuditLogAction.role_delete, discord.AuditLogAction.role_update,
                    discord.AuditLogAction.channel_create, discord.AuditLogAction.channel_delete, discord.AuditLogAction.channel_update,
                    discord.AuditLogAction.invite_create, discord.AuditLogAction.invite_delete, discord.AuditLogAction.invite_update, # Added invite update
                    discord.AuditLogAction.webhook_create, discord.AuditLogAction.webhook_delete, discord.AuditLogAction.webhook_update, # Added webhook actions
                    discord.AuditLogAction.message_delete, discord.AuditLogAction.message_bulk_delete, # Added message delete
                    discord.AuditLogAction.thread_create, discord.AuditLogAction.thread_delete, discord.AuditLogAction.thread_update # Added thread actions
                ]
                fetch_limit = 1000; current_after_id = last_scanned_log_id; processed_in_this_scan = 0; max_iterations = 20

                for iteration in range(max_iterations):
                    log.info(f"  Đang fetch audit logs lần lặp {iteration+1} (sau ID: {current_after_id}, giới hạn: {fetch_limit})")
                    batch_start_time = time.monotonic()
                    logs_in_batch: List[discord.AuditLogEntry] = []
                    try:
                        async for entry in server.audit_logs(limit=fetch_limit, after=discord.Object(id=current_after_id) if current_after_id else None, oldest_first=True):
                            if entry.action in actions_to_track:
                                logs_in_batch.append(entry)
                    except discord.Forbidden: log.error("  Quét Audit Log thất bại: Thiếu quyền."); scan_errors.append("Lỗi quét Audit Log: Thiếu quyền."); break
                    except discord.HTTPException as http_err: log.error(f"  Quét Audit Log thất bại (HTTP {http_err.status}): {http_err.text}"); scan_errors.append(f"Lỗi mạng quét Audit Log ({http_err.status})."); await asyncio.sleep(5); continue
                    except Exception as fetch_err: log.error(f"  Lỗi fetch audit logs: {fetch_err}", exc_info=True); scan_errors.append(f"Lỗi quét Audit Log: {fetch_err}"); break

                    batch_fetch_duration = time.monotonic() - batch_start_time
                    log.info(f"  Đã fetch và lọc {len(logs_in_batch)} entry audit log trong {batch_fetch_duration:.2f}s.")
                    if not logs_in_batch: log.info("  Không tìm thấy entry audit log mới nào."); break

                    batch_newest_id = None
                    for entry in logs_in_batch:
                         await database.add_audit_log_entry(entry) # Lưu vào DB
                         audit_log_entries_added += 1
                         processed_in_this_scan += 1
                         if newest_processed_audit_log_id is None or entry.id > newest_processed_audit_log_id: newest_processed_audit_log_id = entry.id
                         if batch_newest_id is None or entry.id > batch_newest_id: batch_newest_id = entry.id

                    if len(logs_in_batch) >= fetch_limit and batch_newest_id:
                         current_after_id = batch_newest_id
                         log.info(f"  Batch đầy, tiếp tục fetch sau ID {current_after_id}...")
                         await asyncio.sleep(0.5)
                    else:
                         log.info("  Batch không đầy hoặc đã hết, kết thúc fetch audit log.")
                         break
                    if iteration == max_iterations - 1: log.warning("Đạt giới hạn fetch audit log."); scan_errors.append("Quét Audit Log dừng do giới hạn fetch.")

                if newest_processed_audit_log_id is not None and newest_processed_audit_log_id != last_scanned_log_id:
                     log.info(f"Đang cập nhật ID audit log mới nhất trong DB thành: {newest_processed_audit_log_id}")
                     await database.update_newest_audit_log_id(server.id, newest_processed_audit_log_id)
                elif newest_processed_audit_log_id is not None:
                     log.info("ID audit log mới nhất không đổi.")
                else:
                     log.info("Không có entry audit log mới được xử lý.")

                audit_log_scan_duration = discord.utils.utcnow() - audit_scan_start_time
                log.info(f"{e('success')} Hoàn thành quét Audit Log. Đã thêm {audit_log_entries_added} entry mới vào DB trong [magenta]{utils.format_timedelta(audit_log_scan_duration)}[/].")
            except Exception as audit_err:
                log.error(f"{e('error')} Lỗi xử lý Audit Log: {audit_err}", exc_info=True)
                scan_errors.append(f"Lỗi xử lý Audit Log: {audit_err}")
                audit_log_scan_duration = discord.utils.utcnow() - audit_scan_start_time
        else:
            log.info("Bỏ qua quét Audit Log do thiếu quyền.")


        # --- Phân tích Thống kê Role từ Audit Log ---
        log.info(f"{e('role')} Đang phân tích thống kê role từ Audit Log...")
        try:
            if can_scan_audit_log:
                 role_update_logs = await database.get_audit_logs_for_report(
                     server.id, limit=None, action_filter=["member_role_update"] # Lấy tất cả log role update
                 )
                 log.info(f"Đã fetch {len(role_update_logs)} entry 'member_role_update' từ DB để phân tích.")

                 for log_entry in role_update_logs:
                     mod_id = log_entry.get('user_id')
                     target_id = log_entry.get('target_id')
                     if not mod_id: continue
                     mod_id_int = int(mod_id)
                     target_id_int = int(target_id) if target_id else None

                     extra = log_entry.get('extra_data')
                     if not extra or not isinstance(extra, dict): continue

                     # --- Xử lý Role Thêm vào ---
                     added_roles_raw = []
                     changes = extra # Use the direct 'extra_data' which now represents 'changes'
                     # <<< MODIFIED: Use 'after' and 'before' keys directly within 'changes' >>>
                     after_data = changes.get('after', {})
                     before_data = changes.get('before', {})

                     if 'roles' in after_data and 'roles' in before_data:
                         before_ids = {str(r.get('id')) for r in before_data.get('roles',[]) if isinstance(r,dict)}
                         after_ids = {str(r.get('id')) for r in after_data.get('roles',[]) if isinstance(r,dict)}
                         added_role_ids = after_ids - before_ids
                         added_roles_raw = [{'id': rid, 'name': r.get('name')} for rid in added_role_ids for r in after_data.get('roles', []) if isinstance(r, dict) and str(r.get('id')) == rid]
                     elif '$add' in extra: # Check original 'extra' for fallback compatibility if needed
                          added_roles_raw = extra.get('$add', [])

                     for role_info in added_roles_raw:
                         if isinstance(role_info, dict) and 'id' in role_info:
                             role_id_str = str(role_info['id'])
                             role_change_stats[role_id_str]["added"][mod_id_int] += 1
                             if target_id_int: user_role_changes[target_id_int][role_id_str]["added"] += 1

                     # --- Xử lý Role Bị Xóa đi ---
                     removed_roles_raw = []
                     if 'roles' in after_data and 'roles' in before_data:
                         before_ids = {str(r.get('id')) for r in before_data.get('roles',[]) if isinstance(r,dict)}
                         after_ids = {str(r.get('id')) for r in after_data.get('roles',[]) if isinstance(r,dict)}
                         removed_role_ids = before_ids - after_ids
                         removed_roles_raw = [{'id': rid, 'name': r.get('name')} for rid in removed_role_ids for r in before_data.get('roles', []) if isinstance(r, dict) and str(r.get('id')) == rid]
                     elif '$remove' in extra: # Check original 'extra' for fallback compatibility
                          removed_roles_raw = extra.get('$remove', [])

                     for role_info in removed_roles_raw:
                         if isinstance(role_info, dict) and 'id' in role_info:
                             role_id_str = str(role_info['id'])
                             role_change_stats[role_id_str]["removed"][mod_id_int] += 1
                             if target_id_int: user_role_changes[target_id_int][role_id_str]["removed"] += 1
            else:
                 log.warning("Không thể phân tích thống kê role do thiếu quyền quét Audit Log.")
                 scan_errors.append("Bỏ qua thống kê cấp/hủy role (thiếu quyền Audit Log).")

        except Exception as role_stat_err:
            log.error(f"{e('error')} Lỗi khi phân tích thống kê role: {role_stat_err}", exc_info=True)
            scan_errors.append(f"Lỗi phân tích thống kê role: {role_stat_err}")


        await asyncio.sleep(0.2) # Nghỉ ngắn

        # --- Tạo và Gửi Báo cáo ---
        report_messages_sent = 0
        log.info(f"\n--- [bold green]{e('loading')} Đang Tạo Tất Cả Báo Cáo[/bold green] ---")

        async def send_report_embeds(embed_list: List[discord.Embed], context: commands.Context, type_name: str):
            nonlocal report_messages_sent
            if not embed_list:
                 # <<< MODIFIED: Only log missing data for non-leaderboard types that are expected >>>
                 expected_types = ["Kênh Text/Voice", "Kênh Voice Info", "Booster", "Role", "Hoạt động User", "Invite", "Webhook/Integration", "Top User Hoạt Động (Tin nhắn)", "Tóm tắt Audit Log", "Phân tích Quyền", "Phân tích Từ khóa", "Top Thành viên Lâu năm", "Thống kê Cấp/Hủy Role (Bởi Mod)", "Phân tích Biểu cảm", "Top Người Mời", "Thống kê Cấp/Hủy Role (Cho User)"]
                 if type_name in expected_types:
                     log.info(f"{e('info')} Không có dữ liệu cho báo cáo '{type_name}'.")
                     # Only send message for truly missing core data
                     core_missing = ["Kênh Text/Voice", "Role", "Hoạt động User", "Tóm tắt Server"]
                     if type_name in core_missing:
                         try: await context.send(f"{e('info')} {context.author.mention}, không có dữ liệu cho '{type_name}' hoặc đã lọc hết.", delete_after=30)
                         except (discord.HTTPException, discord.Forbidden): pass
                 return

            log.info(f"{e('loading')} Đang gửi {len(embed_list)} embed(s) '{type_name}'...")
            for embed in embed_list:
                try:
                     # <<< MODIFIED: Longer sleep between embeds >>>
                     await context.send(embed=embed)
                     report_messages_sent += 1
                     await asyncio.sleep(1.5) # Increase delay slightly
                except discord.HTTPException as send_err:
                    error_msg = f"Không thể gửi báo cáo '{type_name}' (Lỗi HTTP {send_err.status}): {send_err.text}"
                    log.error(f"{e('error')} {error_msg}")
                    scan_errors.append(error_msg)
                    if send_err.status != 403: # Don't spam if Forbidden
                        try: await context.send(f"{e('error')} {error_msg[:1900]}")
                        except: pass
                    if send_err.status == 429: await asyncio.sleep(send_err.retry_after or 5.0 + 0.5) # Add buffer
                    else: await asyncio.sleep(3)
                except Exception as send_e:
                    error_msg = f"Lỗi không xác định khi gửi báo cáo '{type_name}': {send_e}"
                    log.error(f"{e('error')} {error_msg}", exc_info=True)
                    scan_errors.append(error_msg)
                    try: await context.send(f"{e('error')} {error_msg[:1900]}")
                    except: pass
                    await asyncio.sleep(2)

        # --- Report Sending Order ---
        log.info(f"--- {e('stats')} Báo cáo Tổng quan & Kênh ---")
        # 1. Embed Tổng quan Server
        try:
            log.info("Đang tạo embed tóm tắt server...")
            server_info_for_report = {
                'member_count_real': len([m for m in current_members_list if not m.bot]),
                'bot_count': len([m for m in current_members_list if m.bot]),
                'text_channel_count': channel_counts.get(discord.ChannelType.text, 0),
                'voice_channel_count': channel_counts.get(discord.ChannelType.voice, 0),
                'category_count': channel_counts.get(discord.ChannelType.category, 0),
                'stage_count': channel_counts.get(discord.ChannelType.stage_voice, 0),
                'forum_count': channel_counts.get(discord.ChannelType.forum, 0),
                'reaction_count_overall': overall_total_reaction_count if can_scan_reactions else None
            }
            summary_embed = await reporting.create_summary_embed(
                server, bot, processed_channels_count, processed_threads_count,
                skipped_channels_count, skipped_threads_count, overall_total_message_count,
                len(user_activity), overall_duration, initial_member_status_counts,
                channel_counts, len(all_roles_list), overall_start_time, ctx,
                overall_total_reaction_count=server_info_for_report['reaction_count_overall']
            )
            await send_report_embeds([summary_embed], ctx, "Tóm tắt Server")
        except Exception as ex: error_msg = f"Lỗi khi tạo embed tóm tắt server: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg); await ctx.send(f"{e('error')} {error_msg}")

        # 2. Embed Chi tiết Kênh Text/Voice (Đã quét)
        channel_embeds = []
        # <<< MODIFIED: Include both text and voice channel details >>>
        processed_channel_details = [d for d in channel_details if d.get("processed")]
        log.info(f"Đang tạo embeds cho {len(processed_channel_details)} chi tiết kênh text/voice đã quét...")
        for detail in processed_channel_details:
             try:
                 # <<< CONFIRMED: create_text_channel_embed can handle voice based on 'type' >>>
                 embed = await reporting.create_text_channel_embed(detail, bot)
                 channel_embeds.append(embed)
             except Exception as ex: error_msg = f"Lỗi khi tạo embed cho kênh #{detail.get('name', detail.get('id', 'N/A'))}: {ex}"; log.error(f"{e('error')} {error_msg}", exc_info=True); scan_errors.append(error_msg); error_embed = discord.Embed(title=f"{e('error')} Lỗi tạo báo cáo kênh #{detail.get('name', 'N/A')}", description=error_msg, color=discord.Color.dark_red()); channel_embeds.append(error_embed)
        if channel_embeds: await send_report_embeds(channel_embeds, ctx, "Kênh Text/Voice")
        else: log.info("Không có embed kênh text/voice nào để gửi.")

        # 3. Embed Thông tin Kênh Voice/Stage (Tĩnh)
        if voice_channel_static_data:
            try:
                voice_info_embeds = await reporting.create_voice_channel_embeds(voice_channel_static_data, bot) # This func shows static info
                await send_report_embeds(voice_info_embeds, ctx, "Kênh Voice Info")
            except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed voice info: {ex}", exc_info=True); scan_errors.append(f"Lỗi embed voice info: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed voice info")
        else: log.info("Không có dữ liệu thông tin tĩnh kênh voice/stage để báo cáo.") # Modified log

        log.info(f"--- {e('role')} Báo cáo Roles & Boosters ---")
        # 4. Embed Roles
        if all_roles_list:
            try: role_embeds = await reporting.create_role_embeds(all_roles_list, bot); await send_report_embeds(role_embeds, ctx, "Role")
            except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed role: {ex}", exc_info=True); scan_errors.append(f"Lỗi embed role: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed role")
        else: log.info("Không có dữ liệu role để báo cáo.")

        # 5. Embed Boosters
        if boosters:
            try: booster_embeds = await reporting.create_booster_embeds(boosters, bot, overall_scan_end_time); await send_report_embeds(booster_embeds, ctx, "Booster")
            except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed booster: {ex}", exc_info=True); scan_errors.append(f"Lỗi embed booster: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed booster")
        else: log.info("Không có dữ liệu booster để báo cáo.")

        log.info(f"--- {e('members')} Báo cáo Hoạt động User & Leaderboards ---")
        # 6. Embed Top Active Users (Theo tin nhắn, lọc bot) - MODIFIED FORMAT
        try:
             # <<< MODIFIED: Call the potentially reformatted function >>>
             top_active_embed = await reporting.create_top_active_users_embed(user_activity, server, bot, user_role_changes)
             await send_report_embeds([top_active_embed] if top_active_embed else [], ctx, "Top User Hoạt Động (Tin nhắn)")
        except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed top active: {ex}", exc_info=True); scan_errors.append(f"Lỗi embed top active: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed top active")

        # 7. Embed Top Thành viên Lâu Năm
        try:
            log.info("Đang tạo embed top thành viên lâu năm...")
            oldest_embed = await reporting.create_top_oldest_members_embed(oldest_members_data, bot)
            await send_report_embeds([oldest_embed] if oldest_embed else [], ctx, "Top Thành viên Lâu năm")
        except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed top thành viên lâu năm: {ex}", exc_info=True); scan_errors.append(f"Lỗi tạo embed top lâu năm: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed top thành viên lâu năm")

        # 8. Embed Hoạt động User (Lọc bot)
        if user_activity:
            try: user_activity_embeds = await reporting.create_user_activity_embeds(user_activity, server, bot, MIN_MESSAGE_COUNT_FOR_REPORT, overall_start_time); await send_report_embeds(user_activity_embeds, ctx, "Hoạt động User (Lọc Bot)")
            except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed user activity: {ex}", exc_info=True); scan_errors.append(f"Lỗi embed user activity: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed user activity")
        else: log.info("Không có dữ liệu hoạt động user đạt ngưỡng tối thiểu.")

        # Leaderboard Embeds
        try:
            top_link_embed = await reporting.create_top_link_posters_embed(user_link_counts, server, bot)
            await send_report_embeds([top_link_embed] if top_link_embed else [], ctx, "Top Links")
        except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed top links: {ex}", exc_info=True); scan_errors.append(f"Lỗi embed top links: {ex}")

        try:
            top_image_embed = await reporting.create_top_image_posters_embed(user_image_counts, server, bot)
            await send_report_embeds([top_image_embed] if top_image_embed else [], ctx, "Top Images")
        except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed top images: {ex}", exc_info=True); scan_errors.append(f"Lỗi embed top images: {ex}")

        try:
            top_emoji_embed = await reporting.create_top_emoji_users_embed(user_emoji_counts, server, bot)
            await send_report_embeds([top_emoji_embed] if top_emoji_embed else [], ctx, "Top Emojis")
        except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed top emojis: {ex}", exc_info=True); scan_errors.append(f"Lỗi embed top emojis: {ex}")

        try:
            top_sticker_embed = await reporting.create_top_sticker_users_embed(user_sticker_counts, server, bot)
            await send_report_embeds([top_sticker_embed] if top_sticker_embed else [], ctx, "Top Stickers")
        except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed top stickers: {ex}", exc_info=True); scan_errors.append(f"Lỗi embed top stickers: {ex}")


        log.info(f"--- {e('invite')} Báo cáo Invites, Webhooks, Integrations ---")
        # 9. Embed Invites, Top Inviters, Webhooks, Integrations
        try:
            if invites_data: invite_embeds = await reporting.create_invite_embeds(invites_data, bot); await send_report_embeds(invite_embeds, ctx, "Invite")
            if invite_usage_counts:
                 top_inviter_embed = await reporting.create_top_inviters_embed(invite_usage_counts, server, bot)
                 await send_report_embeds([top_inviter_embed] if top_inviter_embed else [], ctx, "Top Người Mời (Lượt sử dụng)")
            if webhooks_data or integrations_data: webhook_integration_embeds = await reporting.create_webhook_integration_embeds(webhooks_data, integrations_data, bot); await send_report_embeds(webhook_integration_embeds, ctx, "Webhook/Integration")
        except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed inv/wh/int/inviter: {ex}", exc_info=True); scan_errors.append(f"Lỗi embed inv/wh/int/inviter: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed inv/wh/int/inviter")

        log.info(f"--- {e('shield')} Báo cáo Moderation & Admin ---")
        # 10. Embed Phân tích Quyền
        try: log.info("Đang tạo embed phân tích quyền..."); perm_audit_embeds = await reporting.create_permission_audit_embeds(permission_audit_results, bot); await send_report_embeds(perm_audit_embeds, ctx, "Phân tích Quyền")
        except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed phân tích quyền: {ex}", exc_info=True); scan_errors.append(f"Lỗi tạo embed phân tích quyền: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed phân tích quyền")

        # 11. Embed Audit Log Summary
        audit_logs_for_report_cached = []
        if can_scan_audit_log:
            try:
                # <<< MODIFIED: Fetch slightly more for reporting context >>>
                audit_logs_for_report_cached = await database.get_audit_logs_for_report(server.id, limit=150) # Increased limit for reporting
                log.info(f"Đã fetch {len(audit_logs_for_report_cached)} audit logs từ DB để báo cáo.")
                audit_log_embeds = await reporting.create_audit_log_summary_embeds(audit_logs_for_report_cached, server, bot) # Pass server object
                await send_report_embeds(audit_log_embeds, ctx, "Tóm tắt Audit Log")
            except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed Audit Log: {ex}", exc_info=True); scan_errors.append(f"Lỗi tạo embed Audit Log: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed Audit Log")

        # 12. Embed Thống kê Cấp/Hủy Role (Bởi Mod)
        try:
            log.info("Đang tạo embed thống kê cấp/hủy role (bởi mod)...")
            role_stat_embeds = await reporting.create_role_change_stats_embeds(role_change_stats, server, bot)
            await send_report_embeds(role_stat_embeds, ctx, "Thống kê Cấp/Hủy Role (Bởi Mod)")
        except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed thống kê role (mod): {ex}", exc_info=True); scan_errors.append(f"Lỗi tạo embed thống kê role (mod): {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed thống kê role (mod)")

        # 13. Embed Thống kê Cấp/Hủy Role (Cho User)
        try:
            log.info("Đang tạo embed thống kê cấp/hủy role (cho user)...")
            user_role_stat_embeds = await reporting.create_user_role_change_embeds(user_role_changes, server, bot)
            await send_report_embeds(user_role_stat_embeds, ctx, "Thống kê Cấp/Hủy Role (Cho User)")
        except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed thống kê role (user): {ex}", exc_info=True); scan_errors.append(f"Lỗi tạo embed thống kê role (user): {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed thống kê role (user)")

        # Embed Top Roles Granted
        try:
            log.info("Đang tạo embed top roles được cấp...")
            top_roles_embed = await reporting.create_top_roles_granted_embed(role_change_stats, server, bot)
            await send_report_embeds([top_roles_embed] if top_roles_embed else [], ctx, "Top Roles Granted")
        except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed top roles granted: {ex}", exc_info=True); scan_errors.append(f"Lỗi embed top roles granted: {ex}")


        log.info(f"--- {e('stats')} Báo cáo Phân tích ---")
        # 14. Embed Phân tích Từ khóa
        if target_keywords and keyword_counts:
             try:
                 log.info("Đang tạo embed phân tích từ khóa...")
                 keyword_embeds = await reporting.create_keyword_analysis_embeds(keyword_counts, channel_keyword_counts, thread_keyword_counts, user_keyword_counts, server, bot, target_keywords)
                 await send_report_embeds(keyword_embeds, ctx, "Phân tích Từ khóa")
             except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed phân tích từ khóa: {ex}", exc_info=True); scan_errors.append(f"Lỗi tạo embed phân tích từ khóa: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed phân tích từ khóa")

        # 15. Embed Phân tích Biểu cảm
        if can_scan_reactions and reaction_emoji_counts:
             try:
                 log.info("Đang tạo embed phân tích biểu cảm...")
                 reaction_embed = await reporting.create_reaction_analysis_embed(reaction_emoji_counts, overall_total_reaction_count, bot)
                 await send_report_embeds([reaction_embed] if reaction_embed else [], ctx, "Phân tích Biểu cảm")
             except Exception as ex: log.error(f"{e('error')} Lỗi tạo embed phân tích biểu cảm: {ex}", exc_info=True); scan_errors.append(f"Lỗi tạo embed phân tích biểu cảm: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo embed phân tích biểu cảm")

        # 16. Embed Tóm tắt Lỗi
        try: error_summary_embed = await reporting.create_error_embed(scan_errors, bot); await send_report_embeds([error_summary_embed] if error_summary_embed else [], ctx, "Tóm tắt Lỗi")
        except Exception as ex: log.error(f"Lỗi tạo embed tóm tắt lỗi: {ex}", exc_info=True); await ctx.send(f"{e('error')} Lỗi tạo embed lỗi")


        # --- Xuất file CSV và/hoặc JSON ---
        files_to_send: List[discord.File] = []
        if export_csv or export_json:
            log.info(f"\n--- [bold blue]{e('loading')} Đang tạo file xuất[/bold blue] ---")
            audit_logs_for_export = []
            if can_scan_audit_log:
                 # <<< MODIFIED: Check against the increased report limit >>>
                 if audit_logs_for_report_cached and len(audit_logs_for_report_cached) >= 150: # Check against the report limit
                      audit_logs_for_export = audit_logs_for_report_cached
                 else:
                      try:
                           audit_logs_for_export = await database.get_audit_logs_for_report(server.id, limit=5000)
                           log.info(f"{e('success')} Đã fetch {len(audit_logs_for_export)} audit logs từ DB để xuất file.")
                      except Exception as ex: log.error(f"{e('error')} Lỗi fetch audit logs để xuất file: {ex}", exc_info=True); scan_errors.append(f"Lỗi lấy audit log cho export: {ex}")

            if export_csv:
                log.info(f"{e('csv_file')} Đang tạo báo cáo CSV...")
                try:
                    # <<< MODIFIED: Pass processed text/voice details & static voice info >>>
                    await reporting.create_csv_report(
                        server, bot, server_info_for_report, processed_channel_details, # Pass processed text/voice details
                        voice_channel_static_data, # Pass static info for separate voice report
                        user_activity, all_roles_list, boosters,
                        invites_data, webhooks_data, integrations_data,
                        audit_logs_for_export, permission_audit_results, overall_scan_end_time,
                        files_list_ref=files_to_send,
                        reaction_emoji_counts=reaction_emoji_counts if can_scan_reactions else None,
                        invite_usage_counts=invite_usage_counts,
                        user_link_counts=user_link_counts,
                        user_image_counts=user_image_counts,
                        user_emoji_counts=user_emoji_counts,
                        user_sticker_counts=user_sticker_counts
                    )

                    # Create auxiliary CSVs
                    await reporting.create_top_oldest_members_csv(oldest_members_data, files_list_ref)
                    await reporting.create_role_change_stats_csv(role_change_stats, server, files_list_ref, filename_suffix="_by_mod")
                    await reporting.create_user_role_change_csv(user_role_changes, server, files_list_ref)
                    await reporting.create_top_roles_granted_csv(role_change_stats, server, files_list_ref) # CSV top roles granted
                    if target_keywords and keyword_counts:
                        await reporting.create_keyword_csv_reports(
                            keyword_counts, channel_keyword_counts, thread_keyword_counts, user_keyword_counts,
                            target_keywords, files_list_ref=files_to_send
                        )

                except Exception as ex: log.error(f"{e('error')} Lỗi nghiêm trọng khi tạo file CSV: {ex}", exc_info=True); scan_errors.append(f"Lỗi tạo CSV: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo CSV")

            if export_json:
                log.info(f"{e('json_file')} Đang tạo báo cáo JSON...")
                try:
                    # <<< MODIFIED: Pass processed text/voice details & static voice info >>>
                    json_file = await reporting.create_json_report(
                        server, bot, server_info_for_report, processed_channel_details, # Pass processed text/voice details
                        voice_channel_static_data, # Pass static info
                        user_activity, all_roles_list, boosters,
                        invites_data, webhooks_data, integrations_data,
                        audit_logs_for_export, permission_audit_results,
                        oldest_members_data,
                        role_change_stats,
                        user_role_changes,
                        overall_scan_end_time,
                        keyword_counts, channel_keyword_counts, thread_keyword_counts, user_keyword_counts, target_keywords,
                        reaction_emoji_counts=reaction_emoji_counts if can_scan_reactions else None,
                        invite_usage_counts=invite_usage_counts,
                        user_link_counts=user_link_counts,
                        user_image_counts=user_image_counts,
                        user_emoji_counts=user_emoji_counts,
                        user_sticker_counts=user_sticker_counts
                    )
                    if json_file: files_to_send.append(json_file)
                    else: log.warning("Tạo JSON không trả về file nào.")
                except Exception as ex: log.error(f"{e('error')} Lỗi nghiêm trọng khi tạo file JSON: {ex}", exc_info=True); scan_errors.append(f"Lỗi tạo JSON: {ex}"); await ctx.send(f"{e('error')} Lỗi tạo JSON")

            # Check file size before sending
            if files_to_send:
                total_size = 0; total_files = len(files_to_send)
                for f in files_to_send:
                     try:
                         if isinstance(f.fp, io.BytesIO): total_size += f.fp.tell()
                         elif hasattr(f.fp, 'name') and os.path.exists(f.fp.name): total_size += os.path.getsize(f.fp.name)
                         else: log.warning(f"Không thể lấy kích thước file {f.filename}, ước tính 5MB."); total_size += 5*1024*1024
                         f.reset() # Reset pointer after checking size
                     except Exception as size_err: log.warning(f"Không thể lấy kích thước file {f.filename}: {size_err}"); total_size += 5*1024*1024
                log.info(f"{e('info')} Tổng kích thước file xuất ({total_files} files): {total_size / (1024*1024):.2f} MB")
                if total_size >= 24.5 * 1024 * 1024:
                    await ctx.send(f"{e('error')} Tổng kích thước file xuất ({total_size / (1024*1024):.2f} MB) quá lớn (>= 25MB).")
                    scan_errors.append("Xuất file thất bại: quá lớn.")
                    for f in files_to_send: f.close()
                    files_to_send.clear()
            await asyncio.sleep(0.5)

        # --- Tin nhắn Kết thúc ---
        end_time_cmd = time.monotonic()
        total_cmd_duration_secs = end_time_cmd - start_time_cmd
        try: await initial_status_msg.delete()
        except: pass

        e_final = lambda name: utils.get_emoji(name, bot)
        final_message_lines = [
            f"{e_final('success')} **Đã Hoàn Thành Báo Cáo!**",
            f"{e_final('clock')} Tổng thời gian lệnh: **{utils.format_timedelta(datetime.timedelta(seconds=total_cmd_duration_secs), high_precision=True)}**",
            f"{e_final('stats')} Đã gửi **{report_messages_sent}** tin nhắn báo cáo."
        ]
        if log_thread_this_command:
             final_message_lines.append(f"{e('info')} Xem log chi tiết tại: {log_thread_this_command.mention}")

        if files_to_send:
            file_tags = []
            if any(f.filename.endswith('.csv') for f in files_to_send): file_tags.append(f"{e_final('csv_file')} CSV")
            if any(f.filename.endswith('.json') for f in files_to_send): file_tags.append(f"{e_final('json_file')} JSON")
            file_tags_str = " / ".join(file_tags) if file_tags else "file"
            final_message_lines.append(f"📎 Đính kèm **{len(files_to_send)}** {file_tags_str}.")
        elif export_csv or export_json:
             final_message_lines.append(f"{e_final('error')} Yêu cầu xuất file nhưng không thể tạo/gửi.")

        if scan_errors:
             final_message_lines.append(f"{e_final('warning')} Lưu ý: Có **{len(scan_errors)}** lỗi/cảnh báo.")

        final_message = "\n".join(final_message_lines)
        final_sticker_to_send: Optional[discord.Sticker] = None
        if FINAL_STICKER_ID:
            log.info(f"{e('loading')} Đang tìm sticker ID: {FINAL_STICKER_ID}")
            try:
                # <<< MODIFIED: Use fetch_sticker correctly >>>
                fetched_sticker = await bot.fetch_sticker(FINAL_STICKER_ID)
                if fetched_sticker and isinstance(fetched_sticker, discord.Sticker): # Ensure it's a Sticker object
                    final_sticker_to_send = fetched_sticker
                    log.info(f"{e('success')} Tìm thấy sticker: '{final_sticker_to_send.name}'")
                else:
                    log.warning(f"{e('warning')} fetch_sticker trả về None hoặc loại không đúng cho ID {FINAL_STICKER_ID}.")
            except discord.NotFound: log.warning(f"{e('error')} Không tìm thấy sticker ID {FINAL_STICKER_ID}.")
            except discord.HTTPException as fetch_err: log.warning(f"{e('error')} Lỗi mạng fetch sticker {FINAL_STICKER_ID}: {fetch_err.status}")
            except TypeError as sticker_type_err: log.error(f"{e('error')} Lỗi Type khi xử lý sticker {FINAL_STICKER_ID}: {sticker_type_err}", exc_info=True)
            except Exception as e_sticker: log.error(f"{e('error')} Lỗi không xác định fetch sticker {FINAL_STICKER_ID}: {e_sticker}", exc_info=True)
            if not final_sticker_to_send and f"Không tìm thấy sticker ID {FINAL_STICKER_ID}" not in scan_errors:
                scan_errors.append(f"Lỗi khi lấy sticker ID {FINAL_STICKER_ID}.")

        # Gửi tin nhắn cuối cùng và files
        try:
            kwargs_send = {"content": final_message, "allowed_mentions": discord.AllowedMentions.none()}
            if files_to_send: kwargs_send["files"] = files_to_send
            if final_sticker_to_send: kwargs_send["stickers"] = [final_sticker_to_send]
            await ctx.send(**kwargs_send)
            log.info(f"{e('success')} Đã gửi tin nhắn báo cáo cuối cùng thành công.")
        except discord.HTTPException as e_final:
            log.error(f"{e('error')} Không thể gửi tin nhắn/file cuối cùng (HTTP {e_final.status}): {e_final.text}", exc_info=True)
            # Attempt to send error message without attachments if file size might be the issue
            if e_final.status == 413 or "Request Entity Too Large" in e_final.text:
                try: await ctx.send(f"{final_message}\n\n{e('error')} **Lỗi:** Không thể gửi file đính kèm (quá lớn hoặc lỗi khác).")
                except: pass
        except Exception as e_final_unkn:
            log.error(f"{e('error')} Lỗi không xác định khi gửi tin nhắn/file cuối cùng: {e_final_unkn}", exc_info=True)
        finally:
            for f in files_to_send:
                 if hasattr(f, 'fp') and f.fp and not getattr(f.fp, 'closed', True):
                     try: f.close()
                     except Exception as close_err: log.debug(f"Non-critical error closing file {f.filename}: {close_err}")


        log.info(f"\n--- [bold green]{e('success')} Quét Sâu Toàn Bộ cho {server.name} ({server.id}) HOÀN TẤT[/bold green] ---")
        log.info(f"{e('clock')} Tổng thời gian thực thi lệnh: [bold magenta]{total_cmd_duration_secs:.2f}[/] giây")
        if scan_errors: log.warning(f"{e('warning')} Quét hoàn thành với [yellow]{len(scan_errors)}[/] lỗi/cảnh báo được ghi nhận.")

    # <<< KHỐI FINALLY CHÍNH >>>
    finally:
        log.info(f"[dim]{e('loading')} Bắt đầu dọn dẹp sau lệnh sds...[/dim]")
        # <<< MODIFIED: Ensure None is sent to queue only if thread exists >>>
        if discord_log_sender_thread:
            log_queue.put(None) # Signal the logging thread to finish

            if discord_log_sender_thread.is_alive():
                 log.info("Đang chờ Discord Logger Thread hoàn thành gửi log...")
                 discord_log_sender_thread.join(timeout=20.0)
                 if discord_log_sender_thread.is_alive():
                     log.warning("[bold yellow]Discord Logger Thread không dừng sau timeout![/bold yellow]")
                 else:
                     log.info("Discord Logger Thread đã dừng.")
        else:
            log.info("Discord Logger Thread không được khởi tạo hoặc đã dừng trước đó.")

        with discord_log_sender_lock:
             discord_target_thread = None # Clear the target thread ID
             discord_log_buffer.clear() # Clear any remaining logs in buffer

        log.info(f"[dim]{e('success')} Hoàn tất dọn dẹp sau lệnh sds.[/dim]")


# --- Xử lý lỗi chung ---
@bot.event
async def on_command_error(ctx: commands.Context, error):
    e = lambda name: utils.get_emoji(name, bot)
    if isinstance(error, commands.CommandNotFound): return

    original_error_info = ""
    if isinstance(error, commands.CommandInvokeError):
        original_error_info = f"\n   Original Error: {type(error.original).__name__}: {error.original}"

    log.error(f"{e('error')} Lỗi lệnh '[yellow]{ctx.command.name if ctx.command else 'Không rõ'}[/yellow]' bởi [yellow]{ctx.author}[/] ({ctx.author.id}) guild {ctx.guild.id if ctx.guild else 'DM'}: [bold red]{error}[/bold red]{original_error_info}", exc_info=isinstance(error, commands.CommandInvokeError))

    msg = None; reset_cd = False
    if isinstance(error, commands.MissingPermissions): msg = f"{e('error')} Bạn thiếu quyền: `{', '.join(error.missing_permissions)}`"
    elif isinstance(error, commands.BotMissingPermissions): msg = f"{e('error')} Bot thiếu quyền: `{', '.join(error.missing_permissions)}`"; reset_cd = True
    elif isinstance(error, commands.CheckFailure):
        if isinstance(error, commands.GuildOnly): msg = f"{e('error')} Lệnh này chỉ dùng trong server."
        elif isinstance(error, commands.NotOwner): msg = f"{e('error')} Chỉ chủ sở hữu bot mới dùng được."
        else: msg = f"{e('error')} Không đáp ứng điều kiện chạy lệnh."
        reset_cd = True
    elif isinstance(error, commands.CommandOnCooldown): msg = f"{e('clock')} Chờ {error.retry_after:.1f} giây."
    elif isinstance(error, commands.UserInputError): msg = f"{e('warning')} Sai cú pháp. Dùng `{COMMAND_PREFIX}help {ctx.command.qualified_name if ctx.command else ''}`."
    elif isinstance(error, commands.CommandInvokeError):
        original = error.original
        if isinstance(original, discord.Forbidden): msg = f"{e('error')} Lỗi quyền Bot: `{original.text}` (Code: {original.code})"
        elif isinstance(original, discord.HTTPException): msg = f"{e('error')} Lỗi mạng Discord (HTTP {original.status}): `{original.text}`"
        elif isinstance(original, (asyncpg.exceptions.PostgresError, ConnectionError)): msg = f"{e('error')} Lỗi cơ sở dữ liệu hoặc kết nối. Kiểm tra log."
        elif isinstance(original, NameError): msg = f"{e('error')} Lỗi lập trình (NameError). Kiểm tra log."
        # <<< ADDED: Handle potential AttributeError during scanning >>>
        elif isinstance(original, AttributeError): msg = f"{e('error')} Lỗi logic chương trình (AttributeError). Kiểm tra log."
        else: msg = f"{e('error')} Lỗi khi chạy lệnh! Xem log chi tiết."
    else: msg = f"{e('error')} Lỗi không xác định: {type(error).__name__}"

    if msg:
        try: await ctx.send(msg, delete_after=20)
        except discord.HTTPException: pass
    if reset_cd and ctx.command:
        ctx.command.reset_cooldown(ctx)


# --- Chạy Bot ---
async def main():
    global discord_log_thread_active
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN":
        log.critical("[bold red]LỖI NGHIÊM TRỌNG:[/bold red] Thiếu token bot!")
        sys.exit(1)
    if not os.getenv("DATABASE_URL"):
         log.critical("[bold red]LỖI NGHIÊM TRỌNG:[/bold red] Thiếu DATABASE_URL!")
         sys.exit(1)

    log.info("Đang cố gắng chạy bot...")
    try:
        async with bot:
            await setup_hook_logic() # Chạy setup hook trước start
            await bot.start(BOT_TOKEN)
    except (discord.LoginFailure, discord.PrivilegedIntentsRequired, KeyboardInterrupt, SystemExit) as e:
        if isinstance(e, discord.LoginFailure): log.critical("LỖI: Đăng nhập thất bại. Token không hợp lệ.")
        elif isinstance(e, discord.PrivilegedIntentsRequired): log.critical(f"LỖI: Thiếu Privileged Intents: {e}")
        elif isinstance(e, (KeyboardInterrupt, SystemExit)): log.info("Nhận tín hiệu tắt...")
        discord_log_thread_active = False
        # <<< MODIFIED: Only put None if thread exists >>>
        if discord_log_sender_thread:
            try: log_queue.put_nowait(None)
            except queue.Full: pass
            if discord_log_sender_thread.is_alive(): discord_log_sender_thread.join(timeout=5.0)
        sys.exit(1 if not isinstance(e, (KeyboardInterrupt, SystemExit)) else 0)
    except Exception as e:
        log.critical(f"LỖI NGHIÊM TRỌNG khi khởi động/chạy bot: {e}", exc_info=True)
        discord_log_thread_active = False
        if discord_log_sender_thread:
            try: log_queue.put_nowait(None)
            except queue.Full: pass
            if discord_log_sender_thread.is_alive(): discord_log_sender_thread.join(timeout=5.0)
        sys.exit(1)
    finally:
        log.info("Đang chạy dọn dẹp cuối cùng...")
        await database.close_db()
        discord_log_thread_active = False
        if discord_log_sender_thread:
            try: log_queue.put_nowait(None)
            except queue.Full: pass
            if discord_log_sender_thread.is_alive():
                log.info("Chờ Discord Logger Thread dừng trong finally...")
                discord_log_sender_thread.join(timeout=5.0)
        log.info("[bold blue]Bot đã tắt hoàn toàn.[/bold blue]")


if __name__ == "__main__":
    try:
        try: import uvloop; uvloop.install(); log.info("Sử dụng [bold green]uvloop[/bold green].")
        except ImportError: log.info("Sử dụng asyncio mặc định.")
        asyncio.run(main())
    except KeyboardInterrupt: log.info("Bắt được KeyboardInterrupt trong __main__, đang tắt...")
    except Exception as e: log.critical(f"Lỗi không xử lý được trong __main__: {e}", exc_info=True)

# --- END OF FILE bot.py ---