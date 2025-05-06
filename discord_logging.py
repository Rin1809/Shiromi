# --- START OF FILE discord_logging.py ---
import queue
import threading
import logging
import re
import asyncio
import discord
import traceback
import time
from typing import List, Optional, Dict, Any

import utils # Giả sử utils.py có các hàm get_emoji

# --- Globals cho Discord Logging ---
log_queue = queue.Queue(-1) # Queue không giới hạn kích thước
discord_log_sender_thread: Optional[threading.Thread] = None
discord_log_thread_active = False # Cờ trạng thái của thread
discord_target_thread: Optional[discord.Thread] = None # Thread đích để gửi log
discord_log_buffer: List[str] = [] # Buffer log trước khi gửi
discord_log_sender_lock = threading.Lock() # Lock để bảo vệ buffer và target thread
_bot_ref_for_log_sending: Optional[discord.Client] = None # Tham chiếu đến bot instance

log = logging.getLogger(__name__)

# --- Constants Cải thiện ---
LOG_BUFFER_MAX_LINES = 12
LOG_BUFFER_MAX_LENGTH = 1700
LOG_BUFFER_SEND_INTERVAL = 8.0
LOG_MESSAGE_PART_DELAY = 1.4
LOG_RETRY_AFTER_BUFFER = 0.75
# --- End Constants Cải thiện ---

# --- Hàm trợ giúp ---
def strip_rich_markup(text: str) -> str:
    """Xóa các thẻ markup [color], [bold], etc. của Rich."""
    text = re.sub(r'\[(/?[a-zA-Z_]+(?:=[^\]]*)?)\]', '', text)
    text = re.sub(r'\[link=[^\]]+\](.*?)\[/link\]', r'\1', text)
    return text

# --- Coroutine Gửi Log (Chạy trên Main Loop) ---
async def send_log_batch(log_lines: List[str], thread_id: Optional[int], is_final: bool = False):
    """Gửi một batch log vào thread Discord."""
    global _bot_ref_for_log_sending, discord_target_thread # discord_target_thread ở đây chỉ để debug/tham chiếu, không nên dùng để lấy id
    if not _bot_ref_for_log_sending:
        print("[LỖI send_log_batch] Tham chiếu bot chưa sẵn sàng.")
        return
    if not thread_id: # Nếu không có thread_id hợp lệ, không thể gửi
        log.error("send_log_batch được gọi với thread_id là None. Không thể gửi log.")
        return

    thread: Optional[discord.Thread] = None
    try:
        thread = _bot_ref_for_log_sending.get_channel(thread_id)
        if not thread:
            try:
                log.debug(f"Thread {thread_id} không có trong cache, đang fetch...")
                thread = await _bot_ref_for_log_sending.fetch_channel(thread_id)
                log.debug(f"Fetch thread {thread_id} thành công.")
            except discord.NotFound:
                log.error(f"Không tìm thấy thread ID {thread_id} khi fetch.")
                thread = None
            except discord.Forbidden:
                log.error(f"Thiếu quyền fetch thread ID {thread_id}.")
                thread = None
            except Exception as fetch_err:
                log.error(f"Lỗi không xác định khi fetch thread {thread_id}: {fetch_err}")
                thread = None

        if not isinstance(thread, discord.Thread):
            log.error(f"Đối tượng lấy được cho ID {thread_id} không phải là thread.")
            # Không xóa discord_target_thread ở đây vì nó được quản lý bởi lock ở nơi khác
            return

        if not log_lines and not is_final:
            return

        full_log_content = "\n".join(log_lines)
        if is_final:
            final_msg = "\n--- Kết thúc log ---"
            full_log_content += final_msg

        max_len = 1900
        message_parts = []
        current_part = ""

        for line in full_log_content.splitlines(keepends=True):
            if current_part and len(current_part) + len(line) > max_len:
                message_parts.append(current_part)
                current_part = line
            elif len(line) > max_len:
                if current_part:
                    message_parts.append(current_part)
                for i in range(0, len(line), max_len):
                    message_parts.append(line[i:i + max_len])
                current_part = ""
            else:
                current_part += line

        if current_part:
            message_parts.append(current_part)

        for i, part in enumerate(message_parts):
            if not part.strip(): continue
            safe_part = part[:max_len+50]
            content_to_send = f"```log\n{safe_part.strip()}\n```"
            try:
                await thread.send(content_to_send)
                if i < len(message_parts) - 1 or is_final:
                    await asyncio.sleep(LOG_MESSAGE_PART_DELAY)
            except discord.HTTPException as send_err:
                log.error(f"HTTP {send_err.status} khi gửi phần log vào thread {thread_id}: {send_err.text}")
                if send_err.status == 429:
                    retry_after = send_err.retry_after or 5.0
                    wait_time = retry_after + LOG_RETRY_AFTER_BUFFER
                    log.warning(f"   -> Bị rate limit, chờ {wait_time:.2f}s...")
                    await asyncio.sleep(wait_time)
                elif send_err.status >= 500:
                    log.warning("   -> Lỗi server Discord, chờ 5s...")
                    await asyncio.sleep(5)
                else:
                    await asyncio.sleep(2)
            except Exception as send_e:
                log.error(f"Lỗi không xác định khi gửi phần log vào thread {thread_id}: {send_e}", exc_info=True)
                await asyncio.sleep(1)

        if is_final:
            try:
                e_final = lambda n: utils.get_emoji(n, _bot_ref_for_log_sending)
                await thread.send(f"{e_final('success')} Quét hoàn tất cho lệnh này. {e_final('lock')} Luồng này sẽ được lưu trữ và khóa.")
                await asyncio.sleep(1)
                await thread.edit(archived=True, locked=True)
                log.info(f"Đã lưu trữ và khóa luồng log {thread_id}.")
            except discord.Forbidden:
                log.warning(f"Thiếu quyền archive/lock thread {thread_id}.")
                try:
                     await thread.send(f"({utils.get_emoji('error', _bot_ref_for_log_sending)} Không thể tự động lưu trữ/khóa luồng do thiếu quyền)")
                except Exception: pass
            except discord.HTTPException as e:
                log.warning(f"Lỗi HTTP khi archive/lock thread {thread_id}: {e.status} {e.text}")
            except Exception as final_err:
                log.error(f"Lỗi khi gửi tin nhắn cuối/archive thread {thread_id}: {final_err}", exc_info=True)

    except discord.NotFound:
        log.error(f"Không tìm thấy thread ID {thread_id} khi gửi.")
        # Không set discord_target_thread = None ở đây, để thread chính quản lý
    except discord.Forbidden:
        log.error(f"Thiếu quyền gửi tin nhắn vào thread {thread_id}.")
    except discord.HTTPException as http_err:
        log.error(f"Lỗi HTTP {http_err.status} khi gửi log vào thread {thread_id}: {http_err.text}")
        await asyncio.sleep(2)
    except Exception as e:
        log.error(f"Lỗi không xác định khi gửi log vào thread {thread_id}: {e}", exc_info=True)
        traceback.print_exc()


# --- Thread Gửi Log chạy nền ---
def discord_log_sender(bot_loop: asyncio.AbstractEventLoop):
    """Chạy trong thread riêng, xử lý log từ Queue và gửi lên event loop chính."""
    global discord_log_thread_active, discord_target_thread, discord_log_buffer, _bot_ref_for_log_sending

    log.info("[Discord Logger Thread] Bắt đầu.")
    last_send_time = time.monotonic()
    buffer_send_interval = LOG_BUFFER_SEND_INTERVAL

    while discord_log_thread_active:
        send_buffer_now = False
        record = None
        log_batch_to_send = []
        current_target_thread_id_for_batch: Optional[int] = None # Di chuyển ra ngoài try

        try:
            record = log_queue.get(block=True, timeout=1.0)

            if record is None:
                log.info("[Discord Logger Thread] Nhận tín hiệu dừng (None).")
                discord_log_thread_active = False # Sẽ gửi nốt buffer ở khối finally
                send_buffer_now = True # Đánh dấu để gửi buffer trước khi thoát hẳn vòng lặp
            else:
                if not discord_log_thread_active:
                    log_queue.task_done() # Đánh dấu record này cũng đã xong
                    continue

                with discord_log_sender_lock:
                    if not discord_target_thread:
                        log_queue.task_done()
                        continue
                    current_target_thread_id_for_batch = discord_target_thread.id # Lấy ID ở đây

                log_emoji = "➡️"
                level_emoji_map = {
                    logging.CRITICAL: utils.get_emoji('error', _bot_ref_for_log_sending),
                    logging.ERROR:    utils.get_emoji('error', _bot_ref_for_log_sending),
                    logging.WARNING:  utils.get_emoji('warning', _bot_ref_for_log_sending),
                    logging.INFO:     utils.get_emoji('info', _bot_ref_for_log_sending),
                    logging.DEBUG:    utils.get_emoji('hashtag', _bot_ref_for_log_sending),
                    logging.NOTSET:   utils.get_emoji('info', _bot_ref_for_log_sending),
                }
                log_emoji = level_emoji_map.get(record.levelno, utils.get_emoji('info', _bot_ref_for_log_sending))

                temp_formatter = logging.Formatter('%(asctime)s [%(levelname)-.1s] %(name)s: %(message)s', datefmt='%H:%M:%S')
                try:
                    msg = temp_formatter.format(record)
                    msg_cleaned_no_markup = strip_rich_markup(msg)
                    msg_with_emoji = f"{log_emoji} {msg_cleaned_no_markup}"
                except Exception as fmt_err:
                    error_emoji = utils.get_emoji('error', _bot_ref_for_log_sending) or "‼️"
                    msg_with_emoji = f"{error_emoji} Lỗi format log: {fmt_err} | Record: {record.getMessage()[:100]}"

                with discord_log_sender_lock:
                    if discord_target_thread: # Kiểm tra lại target
                        discord_log_buffer.append(msg_with_emoji)
                        buffer_len = len(discord_log_buffer)
                        buffer_char_len = len("\n".join(discord_log_buffer))
                        if buffer_len >= LOG_BUFFER_MAX_LINES or buffer_char_len > LOG_BUFFER_MAX_LENGTH:
                            send_buffer_now = True
            # Luôn đánh dấu record đã xử lý (dù có gửi hay không)
            log_queue.task_done()

        except queue.Empty:
            current_time = time.monotonic()
            with discord_log_sender_lock:
                if discord_target_thread and discord_log_buffer and (current_time - last_send_time >= buffer_send_interval):
                    send_buffer_now = True
                    current_target_thread_id_for_batch = discord_target_thread.id # Lấy ID nếu gửi do timeout
        except Exception as e:
            print(f"[LỖI Discord Logger Thread] Lỗi không xác định: {e}")
            traceback.print_exc()
            time.sleep(5)

        if send_buffer_now:
            temp_target_id = None # Biến tạm để giữ ID
            with discord_log_sender_lock:
                # Chỉ copy buffer và lấy ID nếu target vẫn còn và buffer không rỗng
                if discord_target_thread and discord_log_buffer:
                    log_batch_to_send = list(discord_log_buffer)
                    temp_target_id = discord_target_thread.id # Lấy ID trước khi clear buffer
                    discord_log_buffer.clear()
                    last_send_time = time.monotonic()
                else:
                    log_batch_to_send = [] # Đảm bảo batch rỗng

            if log_batch_to_send and temp_target_id is not None: # Kiểm tra cả ID
                log.debug(f"[Discord Logger Thread] Chuẩn bị gửi batch {len(log_batch_to_send)} dòng log...")
                asyncio.run_coroutine_threadsafe(
                    send_log_batch(log_batch_to_send, temp_target_id, is_final=(not discord_log_thread_active and record is None)), # is_final=True chỉ khi record là None
                    bot_loop
                )
                log_batch_to_send = [] # Xóa sau khi đã lên lịch

    # --- Dọn dẹp khi thread kết thúc (sau vòng lặp while) ---
    log.info("[Discord Logger Thread] Dọn dẹp và kết thúc.")
    final_batch_to_send = []
    target_thread_id_at_exit: Optional[int] = None
    with discord_log_sender_lock:
        if discord_target_thread and discord_log_buffer:
            final_batch_to_send = list(discord_log_buffer)
            target_thread_id_at_exit = discord_target_thread.id
            discord_log_buffer.clear()
        # discord_target_thread = None # Xóa target ở đây có thể hơi sớm nếu send_log_batch bị block

    if final_batch_to_send and target_thread_id_at_exit is not None:
        log.info(f"[Discord Logger Thread] Gửi nốt {len(final_batch_to_send)} dòng log cuối cùng.")
        # is_final phải là True ở đây
        future = asyncio.run_coroutine_threadsafe(
            send_log_batch(final_batch_to_send, target_thread_id_at_exit, is_final=True),
            bot_loop
        )
        try:
            future.result(timeout=10) # Chờ coroutine hoàn thành (có timeout)
        except asyncio.TimeoutError:
            log.warning("Timeout khi chờ gửi batch log cuối cùng.")
        except Exception as e_final_send:
            log.error(f"Lỗi khi gửi batch log cuối cùng: {e_final_send}")

    # Dọn dẹp target thread sau cùng
    with discord_log_sender_lock:
        discord_target_thread = None

    log.info("[Discord Logger Thread] Thread đã dừng.")


# --- Custom Log Handler ---
class DiscordLogHandler(logging.Handler):
    """Handler này đưa LogRecord vào Queue để thread khác xử lý."""
    def __init__(self, level=logging.NOTSET):
        super().__init__(level=level)

    def emit(self, record: logging.LogRecord):
        if not discord_log_thread_active and record is not None:
            return

        try:
            log_queue.put_nowait(record)
        except queue.Full:
            print("[CRITICAL LỖI DiscordLogHandler] Log queue đầy! Log bị mất.")
        except Exception as e:
            print(f"[LỖI DiscordLogHandler] Lỗi khi đưa log vào queue: {e}")


# --- Các hàm điều khiển thread logging ---
def setup_discord_logging(bot_client: discord.Client):
    """Thiết lập tham chiếu bot cho module logging."""
    global _bot_ref_for_log_sending
    _bot_ref_for_log_sending = bot_client
    log.info("Tham chiếu bot đã được thiết lập cho Discord logging.")

def start_discord_log_thread(loop: asyncio.AbstractEventLoop):
    """Khởi động thread gửi log Discord nếu chưa chạy."""
    global discord_log_sender_thread, discord_log_thread_active
    if discord_log_sender_thread is None or not discord_log_sender_thread.is_alive():
        log.info("Khởi động Discord Logger Thread...")
        discord_log_thread_active = True
        discord_log_sender_thread = threading.Thread(
            target=discord_log_sender,
            args=(loop,),
            daemon=True,
            name="DiscordLogSender"
        )
        discord_log_sender_thread.start()
        return True
    else:
        log.info("Discord Logger Thread đã chạy.")
        return False

def stop_discord_log_thread():
    """Gửi tín hiệu dừng và chờ thread gửi log Discord kết thúc."""
    global discord_log_thread_active, discord_log_sender_thread
    if not discord_log_thread_active and (discord_log_sender_thread is None or not discord_log_sender_thread.is_alive()):
        log.info("Discord Logger Thread không chạy hoặc đã dừng.")
        return

    log.info("Chuẩn bị dừng Discord Logger Thread...")
    # Không set discord_log_thread_active = False ở đây ngay lập tức
    # mà để tín hiệu None trong queue kích hoạt việc dừng

    try:
        log_queue.put_nowait(None) # Gửi tín hiệu dừng
        log.info("Đã gửi tín hiệu dừng (None) đến Discord Logger Thread.")
    except queue.Full:
        log.warning("Log queue đầy khi gửi tín hiệu dừng. Thread có thể không dừng ngay.")
        # Nếu queue đầy, việc set active = False ở đây có thể cần thiết để ép dừng
        discord_log_thread_active = False
    except Exception as e:
        log.error(f"Lỗi khi gửi tín hiệu dừng log: {e}")
        discord_log_thread_active = False # Ép dừng nếu lỗi

    if discord_log_sender_thread and discord_log_sender_thread.is_alive():
        log.info("Đang chờ Discord Logger Thread dừng (tối đa 10s)...")
        discord_log_sender_thread.join(timeout=10.0)
        if discord_log_sender_thread.is_alive():
            log.warning("Discord Logger Thread không dừng sau 10 giây! Ép dừng cờ active.")
            discord_log_thread_active = False # Đảm bảo cờ được set để thoát vòng lặp
        else:
            log.info("Discord Logger Thread đã dừng thành công.")
        discord_log_sender_thread = None

    # Đảm bảo cờ active cuối cùng là False
    discord_log_thread_active = False


def set_log_target_thread(thread: Optional[discord.Thread]):
    """Đặt thread đích cho việc gửi log (hoặc xóa target nếu thread=None)."""
    global discord_target_thread, discord_log_buffer, _bot_ref_for_log_sending
    with discord_log_sender_lock:
        if thread:
            if discord_target_thread is None or discord_target_thread.id != thread.id:
                log.info(f"Đặt thread log đích: {thread.mention} ({thread.id})")
                # Nếu có buffer cũ và target cũ, gửi nốt trước khi đổi target
                if discord_target_thread and discord_log_buffer:
                    log.debug(f"Gửi nốt {len(discord_log_buffer)} log từ target cũ {discord_target_thread.id}...")
                    log_batch_to_send_old = list(discord_log_buffer)
                    old_target_id = discord_target_thread.id
                    discord_log_buffer.clear()
                    if _bot_ref_for_log_sending:
                        asyncio.run_coroutine_threadsafe(
                            send_log_batch(log_batch_to_send_old, old_target_id, is_final=False),
                            _bot_ref_for_log_sending.loop
                        )
                discord_target_thread = thread
                discord_log_buffer.clear() # Luôn xóa buffer khi có target mới
            else:
                log.debug(f"Target log thread không đổi ({thread.id}).")
        else: # thread is None, xóa target
            if discord_target_thread is not None:
                log.info(f"Xóa thread log đích (trước đó là {discord_target_thread.id}).")
                # Gửi nốt buffer còn lại TRƯỚC KHI xóa target
                if discord_log_buffer and _bot_ref_for_log_sending:
                    log.debug(f"Gửi nốt {len(discord_log_buffer)} log buffer trước khi xóa target...")
                    log_batch_to_send_final_buffer = list(discord_log_buffer)
                    target_id_before_clear = discord_target_thread.id
                    discord_log_buffer.clear() # Xóa buffer
                    asyncio.run_coroutine_threadsafe(
                        send_log_batch(log_batch_to_send_final_buffer, target_id_before_clear, is_final=True), # is_final=True vì target sắp bị xóa
                        _bot_ref_for_log_sending.loop
                    )
                discord_target_thread = None # Xóa target

def get_log_target_thread() -> Optional[discord.Thread]:
    """Lấy thread log đích hiện tại."""
    with discord_log_sender_lock:
        return discord_target_thread

# --- END OF FILE discord_logging.py ---