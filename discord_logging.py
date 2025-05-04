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

import utils

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
LOG_BUFFER_MAX_LINES = 12     # Gửi khi đạt 12 dòng (thay vì 7)
LOG_BUFFER_MAX_LENGTH = 1700  # Gửi khi dài hơn 1700 ký tự (thay vì 1200)
LOG_BUFFER_SEND_INTERVAL = 8.0 # Gửi buffer ít nhất mỗi 8 giây (thay vì 5)
LOG_MESSAGE_PART_DELAY = 1.4  # Delay giữa các phần của cùng 1 lô log (thay vì 1.1)
LOG_RETRY_AFTER_BUFFER = 0.75 # Thêm 0.75s vào retry_after của Discord (thay vì 0.5)
# --- End Constants Cải thiện ---

# --- Hàm trợ giúp ---
def strip_rich_markup(text: str) -> str:
    """Xóa các thẻ markup [color], [bold], etc. của Rich."""
    # Xóa thẻ có dạng [tag] hoặc [/tag] hoặc [tag=value]
    text = re.sub(r'\[(/?[a-zA-Z_]+(?:=[^\]]*)?)\]', '', text)
    # Xóa thẻ link nhưng giữ lại nội dung
    text = re.sub(r'\[link=[^\]]+\](.*?)\[/link\]', r'\1', text)
    return text

# --- Coroutine Gửi Log (Chạy trên Main Loop) ---
async def send_log_batch(log_lines: List[str], thread_id: int, is_final: bool = False):
    """Gửi một batch log vào thread Discord."""
    global _bot_ref_for_log_sending, discord_target_thread
    if not _bot_ref_for_log_sending:
        print("[LỖI send_log_batch] Tham chiếu bot chưa sẵn sàng.")
        return

    thread: Optional[discord.Thread] = None
    try:
        # Lấy đối tượng thread, thử fetch nếu không có trong cache
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
            with discord_log_sender_lock:
                discord_target_thread = None # Xóa target nếu không hợp lệ
            return

        if not log_lines and not is_final:
            return # Không có gì để gửi

        full_log_content = "\n".join(log_lines)
        if is_final:
            final_msg = "\n--- Kết thúc log ---"
            full_log_content += final_msg

        # Chia log thành các phần nhỏ hơn để gửi (giới hạn Discord)
        max_len = 1900
        message_parts = []
        current_part = ""

        for line in full_log_content.splitlines(keepends=True):
            # Nếu thêm dòng mới vào part hiện tại sẽ quá dài
            if current_part and len(current_part) + len(line) > max_len:
                message_parts.append(current_part)
                current_part = line # Bắt đầu part mới với dòng này
            # Nếu bản thân dòng đã quá dài
            elif len(line) > max_len:
                if current_part: # Gửi part cũ trước nếu có
                    message_parts.append(current_part)
                # Chia dòng dài thành nhiều phần
                for i in range(0, len(line), max_len):
                    message_parts.append(line[i:i + max_len])
                current_part = "" # Reset part hiện tại
            # Nếu thêm dòng mới vẫn ổn
            else:
                current_part += line

        if current_part: # Gửi nốt phần còn lại
            message_parts.append(current_part)

        # Gửi từng phần với delay và xử lý lỗi
        for i, part in enumerate(message_parts):
            if not part.strip(): continue # Bỏ qua phần trống
            safe_part = part[:max_len+50] # Cắt an toàn lần nữa phòng ngừa
            content_to_send = f"```log\n{safe_part.strip()}\n```"
            try:
                await thread.send(content_to_send)
                # Delay giữa các tin nhắn để tránh flood/rate limit
                if i < len(message_parts) - 1 or is_final:
                    # <<< FIX: Sử dụng LOG_MESSAGE_PART_DELAY >>>
                    await asyncio.sleep(LOG_MESSAGE_PART_DELAY)
            except discord.HTTPException as send_err:
                log.error(f"HTTP {send_err.status} khi gửi phần log vào thread {thread_id}: {send_err.text}")
                if send_err.status == 429: # Rate limited
                    retry_after = send_err.retry_after or 5.0
                    # <<< FIX: Sử dụng LOG_RETRY_AFTER_BUFFER >>>
                    wait_time = retry_after + LOG_RETRY_AFTER_BUFFER
                    log.warning(f"   -> Bị rate limit, chờ {wait_time:.2f}s...")
                    await asyncio.sleep(wait_time)
                elif send_err.status >= 500: # Lỗi server Discord
                    log.warning("   -> Lỗi server Discord, chờ 5s...")
                    await asyncio.sleep(5)
                else: # Các lỗi HTTP khác (vd: 403 Forbidden nếu mất quyền giữa chừng)
                    await asyncio.sleep(2)
            except Exception as send_e:
                log.error(f"Lỗi không xác định khi gửi phần log vào thread {thread_id}: {send_e}", exc_info=True)
                await asyncio.sleep(1) # Chờ một chút trước khi thử lại (nếu có vòng lặp)

        # Hoàn tất và khóa thread nếu là log cuối cùng
        if is_final:
            try:
                e_final = lambda n: utils.get_emoji(n, _bot_ref_for_log_sending)
                await thread.send(f"{e_final('success')} Quét hoàn tất cho lệnh này. {e_final('lock')} Luồng này sẽ được lưu trữ và khóa.")
                await asyncio.sleep(1) # Chờ tin nhắn được gửi
                await thread.edit(archived=True, locked=True)
                log.info(f"Đã lưu trữ và khóa luồng log {thread_id}.")
            except discord.Forbidden:
                log.warning(f"Thiếu quyền archive/lock thread {thread_id}.")
                try: # Cố gắng thông báo lỗi trong thread
                     await thread.send(f"({utils.get_emoji('error', _bot_ref_for_log_sending)} Không thể tự động lưu trữ/khóa luồng do thiếu quyền)")
                except Exception: pass
            except discord.HTTPException as e:
                log.warning(f"Lỗi HTTP khi archive/lock thread {thread_id}: {e.status} {e.text}")
            except Exception as final_err:
                log.error(f"Lỗi khi gửi tin nhắn cuối/archive thread {thread_id}: {final_err}", exc_info=True)

    except discord.NotFound:
        log.error(f"Không tìm thấy thread ID {thread_id} khi gửi.")
        with discord_log_sender_lock: discord_target_thread = None
    except discord.Forbidden:
        log.error(f"Thiếu quyền gửi tin nhắn vào thread {thread_id}.")
        with discord_log_sender_lock: discord_target_thread = None
    except discord.HTTPException as http_err:
        log.error(f"Lỗi HTTP {http_err.status} khi gửi log vào thread {thread_id}: {http_err.text}")
        await asyncio.sleep(2) # Chờ nếu có lỗi HTTP chung
    except Exception as e:
        log.error(f"Lỗi không xác định khi gửi log vào thread {thread_id}: {e}", exc_info=True)
        traceback.print_exc()


# --- Thread Gửi Log chạy nền ---
def discord_log_sender(bot_loop: asyncio.AbstractEventLoop):
    """Chạy trong thread riêng, xử lý log từ Queue và gửi lên event loop chính."""
    global discord_log_thread_active, discord_target_thread, discord_log_buffer, _bot_ref_for_log_sending

    log.info("[Discord Logger Thread] Bắt đầu.")
    last_send_time = time.monotonic()
    # <<< FIX: Sử dụng constant đã định nghĩa >>>
    buffer_send_interval = LOG_BUFFER_SEND_INTERVAL

    while discord_log_thread_active:
        send_buffer_now = False
        record = None
        log_batch_to_send = []

        try:
            # Lấy log từ queue, timeout để kiểm tra định kỳ
            record = log_queue.get(block=True, timeout=1.0) # Timeout ngắn hơn để phản ứng nhanh hơn

            if record is None: # Tín hiệu dừng thread
                log.info("[Discord Logger Thread] Nhận tín hiệu dừng (None).")
                discord_log_thread_active = False
                send_buffer_now = True # Gửi nốt buffer trước khi thoát
            else:
                if not discord_log_thread_active: continue # Bỏ qua nếu đã nhận tín hiệu dừng

                # Chỉ xử lý nếu có thread đích được đặt
                with discord_log_sender_lock:
                    if not discord_target_thread:
                        # Nếu không có target, đánh dấu record đã xử lý để tránh đầy queue
                        log_queue.task_done()
                        continue

                # Lấy emoji (ưu tiên dùng utils.get_emoji)
                log_emoji = "➡️" # Fallback
                level_emoji_map = {
                    logging.CRITICAL: utils.get_emoji('error', _bot_ref_for_log_sending),
                    logging.ERROR:    utils.get_emoji('error', _bot_ref_for_log_sending),
                    logging.WARNING:  utils.get_emoji('warning', _bot_ref_for_log_sending),
                    logging.INFO:     utils.get_emoji('info', _bot_ref_for_log_sending),
                    logging.DEBUG:    utils.get_emoji('hashtag', _bot_ref_for_log_sending),
                    logging.NOTSET:   utils.get_emoji('info', _bot_ref_for_log_sending),
                }
                log_emoji = level_emoji_map.get(record.levelno, utils.get_emoji('info', _bot_ref_for_log_sending))

                # Định dạng log ngắn gọn
                temp_formatter = logging.Formatter('%(asctime)s [%(levelname)-.1s] %(name)s: %(message)s', datefmt='%H:%M:%S')
                try:
                    msg = temp_formatter.format(record)
                    msg_cleaned_no_markup = strip_rich_markup(msg) # Xóa markup Rich
                    msg_with_emoji = f"{log_emoji} {msg_cleaned_no_markup}"
                except Exception as fmt_err:
                    error_emoji = utils.get_emoji('error', _bot_ref_for_log_sending) or "‼️"
                    msg_with_emoji = f"{error_emoji} Lỗi format log: {fmt_err} | Record: {record.getMessage()[:100]}"

                # Thêm vào buffer
                with discord_log_sender_lock:
                    if discord_target_thread: # Kiểm tra lại target trước khi thêm
                        discord_log_buffer.append(msg_with_emoji)
                        # <<< FIX: Sử dụng constants mới để kiểm tra gửi buffer >>>
                        buffer_len = len(discord_log_buffer)
                        buffer_char_len = len("\n".join(discord_log_buffer))
                        if buffer_len >= LOG_BUFFER_MAX_LINES or buffer_char_len > LOG_BUFFER_MAX_LENGTH:
                            send_buffer_now = True
                        # <<< END FIX >>>

            # Đánh dấu record đã xử lý (dù có gửi hay không)
            log_queue.task_done()

        except queue.Empty:
            # Timeout, kiểm tra xem có cần gửi buffer không (dựa trên thời gian)
            current_time = time.monotonic()
            with discord_log_sender_lock:
                # <<< FIX: Sử dụng constant mới >>>
                if discord_target_thread and discord_log_buffer and (current_time - last_send_time >= buffer_send_interval):
                    send_buffer_now = True
                # <<< END FIX >>>
        except Exception as e:
            # Log lỗi của chính thread này (dùng print vì logger có thể lỗi)
            print(f"[LỖI Discord Logger Thread] Lỗi không xác định: {e}")
            traceback.print_exc()
            time.sleep(5) # Nghỉ 5 giây nếu có lỗi nghiêm trọng

        # Thực hiện gửi batch lên event loop chính nếu cần
        if send_buffer_now:
            with discord_log_sender_lock:
                if discord_target_thread and discord_log_buffer:
                    # Copy buffer để gửi và xóa buffer gốc
                    log_batch_to_send = list(discord_log_buffer)
                    discord_log_buffer.clear()
                    last_send_time = time.monotonic() # Cập nhật thời gian gửi cuối

            if log_batch_to_send: # Chỉ gửi nếu thực sự có log
                log.debug(f"[Discord Logger Thread] Chuẩn bị gửi batch {len(log_batch_to_send)} dòng log...")
                # Lên lịch coroutine trên main loop
                asyncio.run_coroutine_threadsafe(
                    send_log_batch(log_batch_to_send, discord_target_thread.id, is_final=(not discord_log_thread_active)),
                    bot_loop
                )

    # --- Dọn dẹp khi thread kết thúc ---
    log.info("[Discord Logger Thread] Dọn dẹp và kết thúc.")
    final_batch_to_send = []
    target_thread_id_at_exit: Optional[int] = None
    with discord_log_sender_lock:
        if discord_target_thread and discord_log_buffer: # Gửi nốt phần còn lại
            final_batch_to_send = list(discord_log_buffer)
            target_thread_id_at_exit = discord_target_thread.id
            discord_log_buffer.clear()
        discord_target_thread = None # Xóa target

    if final_batch_to_send and target_thread_id_at_exit:
        log.info(f"[Discord Logger Thread] Gửi nốt {len(final_batch_to_send)} dòng log cuối cùng.")
        asyncio.run_coroutine_threadsafe(
            send_log_batch(final_batch_to_send, target_thread_id_at_exit, is_final=True),
            bot_loop
        )

    log.info("[Discord Logger Thread] Thread đã dừng.")


# --- Custom Log Handler ---
class DiscordLogHandler(logging.Handler):
    """Handler này đưa LogRecord vào Queue để thread khác xử lý."""
    def __init__(self, level=logging.NOTSET):
        super().__init__(level=level)

    def emit(self, record: logging.LogRecord):
        # Chỉ đưa vào queue nếu thread đang (hoặc sẽ) chạy
        # record=None là tín hiệu dừng, vẫn cho vào queue
        if not discord_log_thread_active and record is not None:
            return # Bỏ qua nếu thread đã dừng hẳn

        try:
            # Dùng nowait để tránh block nếu queue đầy (không nên xảy ra với size -1)
            log_queue.put_nowait(record)
        except queue.Full:
            # Log lỗi này đặc biệt nghiêm trọng
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
            daemon=True, # Để thread tự thoát khi chương trình chính thoát
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
    discord_log_thread_active = False # Đặt cờ dừng trước

    # Gửi tín hiệu dừng vào queue
    try:
        log_queue.put_nowait(None)
        log.info("Đã gửi tín hiệu dừng (None) đến Discord Logger Thread.")
    except queue.Full:
        log.warning("Log queue đầy khi gửi tín hiệu dừng.")
    except Exception as e:
        log.error(f"Lỗi khi gửi tín hiệu dừng log: {e}")

    # Chờ thread kết thúc (có timeout)
    if discord_log_sender_thread and discord_log_sender_thread.is_alive():
        log.info("Đang chờ Discord Logger Thread dừng (tối đa 10s)...")
        discord_log_sender_thread.join(timeout=10.0)
        if discord_log_sender_thread.is_alive():
            log.warning("Discord Logger Thread không dừng sau 10 giây!")
        else:
            log.info("Discord Logger Thread đã dừng thành công.")
        discord_log_sender_thread = None # Xóa tham chiếu thread

def set_log_target_thread(thread: Optional[discord.Thread]):
    """Đặt thread đích cho việc gửi log (hoặc xóa target nếu thread=None)."""
    global discord_target_thread, discord_log_buffer
    with discord_log_sender_lock:
        if thread:
            # Kiểm tra xem target có thay đổi không
            if discord_target_thread is None or discord_target_thread.id != thread.id:
                log.info(f"Đặt thread log đích: {thread.mention} ({thread.id})")
                discord_target_thread = thread
                discord_log_buffer.clear() # Xóa buffer cũ khi có target mới
            else:
                log.debug(f"Target log thread không đổi ({thread.id}).")
        else:
            if discord_target_thread is not None:
                log.info("Xóa thread log đích.")
                # Gửi nốt buffer còn lại TRƯỚC KHI xóa target
                if discord_log_buffer:
                    log.debug("Gửi nốt log buffer trước khi xóa target...")
                    log_batch_to_send = list(discord_log_buffer)
                    target_id_before_clear = discord_target_thread.id
                    discord_log_buffer.clear()
                    if _bot_ref_for_log_sending:
                         asyncio.run_coroutine_threadsafe(
                              send_log_batch(log_batch_to_send, target_id_before_clear, is_final=False), # Không phải final
                              _bot_ref_for_log_sending.loop
                         )
                discord_target_thread = None

def get_log_target_thread() -> Optional[discord.Thread]:
    """Lấy thread log đích hiện tại."""
    with discord_log_sender_lock:
        return discord_target_thread

# --- END OF FILE discord_logging.py ---