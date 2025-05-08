# --- START OF FILE bot.py ---
import sys
import os
import asyncio
import logging
import traceback 
import discord
from discord.ext import commands
from dotenv import load_dotenv


load_dotenv()
print("[MAIN] Đã gọi load_dotenv() từ bot.py")
# -----------------------------------------

# --- Thêm thư mục gốc vào sys.path ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
# ------------------------------------

import config 
import database
import discord_logging
import utils
from bot_core import setup as bot_setup
from bot_core import events as bot_events

log = logging.getLogger(__name__)

# --- Định nghĩa Lớp Bot tùy chỉnh ---
class ShiromiBot(commands.Bot):
    def __init__(self, intents: discord.Intents):
        # Sử dụng hàm để lấy prefix, cho phép prefix thay đổi dựa trên người gửi
        super().__init__(command_prefix=self._get_prefix, intents=intents, help_command=None)

        # Đặt owner_id từ ADMIN_USER_ID trong config, nếu có.
        # Điều này quan trọng cho @commands.is_owner() hoạt động đúng với người dùng thật.
        if config.ADMIN_USER_ID:
            self.owner_id = config.ADMIN_USER_ID
            log.info(f"Bot owner_id được đặt thành: {self.owner_id} từ config.ADMIN_USER_ID (người dùng thực).")
        else:
            log.warning("ADMIN_USER_ID không được cấu hình trong .env, chức năng is_owner có thể không hoạt động như mong đợi.")


    async def _get_prefix(self, bot, message: discord.Message):
        """
        Lấy prefix lệnh. Cho phép PROXY_BOT_ID dùng lệnh ngay cả khi nó là bot.
        Người dùng thường cũng có thể dùng lệnh.
        Các bot khác (không phải PROXY_BOT_ID) sẽ không có prefix nào khớp.
        """
        if message.author.id == self.user.id: # Bỏ qua chính nó
            return []

        # Log kiểm tra giá trị config.PROXY_BOT_ID ngay tại thời điểm kiểm tra
        log.debug(f"_get_prefix: Checking message from {message.author.id}. Current config.PROXY_BOT_ID is: {config.PROXY_BOT_ID} (Type: {type(config.PROXY_BOT_ID)})")

        # Ưu tiên cho PROXY_BOT_ID
        if config.PROXY_BOT_ID and message.author.id == config.PROXY_BOT_ID:

            log.debug(f"_get_prefix: PROXY_BOT_ID ({config.PROXY_BOT_ID}) is sending potential command: '{message.content[:70]}'. Returning EMPTY prefix.")
            # Bot proxy (Mizuki) chịu trách nhiệm đảm bảo lệnh bắt đầu đúng.
            return "" # Prefix rỗng có nghĩa là tên lệnh phải ở đầu tin nhắn.

        # Nếu không phải PROXY_BOT_ID, chỉ cho phép nếu người gửi không phải là bot
        if not message.author.bot:

            log.debug(f"_get_prefix: User {message.author.id} sending message. Returning normal prefix config.")
            return commands.when_mentioned_or(config.COMMAND_PREFIX)(self, message)

        log.debug(f"_get_prefix: Other bot {message.author.id} or unhandled case. Returning no prefix match.")
        # Các trường hợp còn lại (bot khác không phải proxy) sẽ không có prefix
        return []

    async def process_commands(self, message: discord.Message):
        """
        Ghi đè để xử lý lệnh, đặc biệt cho phép PROXY_BOT_ID.
        Decorator is_owner() sẽ dựa vào self.owner_id đã được set trong __init__.
        """
        if message.author.id == self.user.id: # Bỏ qua tin nhắn từ chính nó
            return

  
        log.debug(f"process_commands: Received message from {message.author} (ID: {message.author.id}, Bot: {message.author.bot}). Content: '{message.content[:70]}'")

        ctx = await self.get_context(message)

        log.debug(f"process_commands: Context created. Prefix: '{ctx.prefix}', Valid: {ctx.valid}, Command: '{ctx.command.qualified_name if ctx.command else 'None'}'")

        if ctx.command is not None and ctx.valid: # ctx.valid sẽ true nếu prefix khớp và lệnh tồn tại
            is_proxy_bot_message = config.PROXY_BOT_ID and message.author.id == config.PROXY_BOT_ID

            # Kiểm tra cog_check của lệnh nếu có
            if hasattr(ctx.command.cog, 'cog_check') and ctx.command.cog is not None:
                try:
                    can_run_cog = await ctx.command.cog.cog_check(ctx)
                    if not can_run_cog:
                        log.info(f"Cog check failed for command '{ctx.command.qualified_name}' from {message.author} (ID: {message.author.id}, IsProxy: {is_proxy_bot_message})")
                        return # Dừng nếu cog_check fail
                except commands.CheckFailure as e:
                    log.info(f"Cog check explicitly failed with CheckFailure for command '{ctx.command.qualified_name}' from {message.author} (ID: {message.author.id}, IsProxy: {is_proxy_bot_message}): {e}")
                    # Gửi thông báo lỗi từ cog_check đã được xử lý bên trong nó hoặc bởi on_command_error
                    return
                except Exception as e_cog_check:
                    log.error(f"Error during cog_check for command '{ctx.command.qualified_name}': {e_cog_check}", exc_info=True)
                    # Có thể gửi một tin nhắn lỗi chung hoặc để on_command_error xử lý
                    await ctx.send(f"{utils.get_emoji('error', self)} Lỗi khi kiểm tra quyền lệnh.", delete_after=10)
                    return


            log.info(f"Processing command '{ctx.command.qualified_name}' from {message.author} (ID: {message.author.id}, IsProxy: {is_proxy_bot_message})")
            await self.invoke(ctx)
        else:
            # Log tại sao lệnh không được xử lý

            if ctx.prefix is not None and message.content.startswith(ctx.prefix):
                # Trường hợp có prefix nhưng không tìm thấy lệnh (ví dụ: user gõ Shi sai tên lệnh)
                log.debug(f"No command found for message from {message.author} (ID: {message.author.id}) with prefix '{ctx.prefix}': {message.content[:70]}")
            elif config.PROXY_BOT_ID and message.author.id == config.PROXY_BOT_ID and ctx.prefix == "":
                 # Trường hợp proxy bot, prefix rỗng đã được trả về, nhưng không tìm thấy lệnh hợp lệ
                 log.debug(f"PROXY_BOT_ID message, empty prefix returned, but NO VALID COMMAND found for: '{message.content[:70]}'")
            elif not message.author.bot and message.content.startswith(config.COMMAND_PREFIX):
                # Trường hợp user dùng prefix thường nhưng không tìm thấy lệnh
                log.debug(f"User message with normal prefix '{config.COMMAND_PREFIX}' but NO VALID COMMAND for: {message.content[:70]}")

    async def setup_hook(self):
        log.info("Đang chạy logic setup hook...")
        utils.set_bot_reference_for_emoji(self)
        discord_logging.setup_discord_logging(self)
        await database.connect_db()
        loop = asyncio.get_running_loop()
        discord_logging.start_discord_log_thread(loop)
        log.info("Đang tải Cogs...")
        initial_extensions = [
            "cogs.deep_scan_cog",
        ]
        for extension in initial_extensions:
            try:
                await self.load_extension(extension)
                log.info(f"=> Đã tải Cog: {extension}")
            except commands.ExtensionNotFound:
                log.error(f"Lỗi tải Cog: Không tìm thấy extension '{extension}'")
            except commands.ExtensionAlreadyLoaded:
                log.warning(f"Cog '{extension}' đã được tải trước đó.")
            except commands.NoEntryPointError:
                 log.error(f"Lỗi tải Cog '{extension}': Thiếu hàm setup().")
            except commands.ExtensionFailed as e:
                log.critical(f"Lỗi tải Cog '{extension}': {e.original}", exc_info=True)
            except Exception as e:
                log.critical(f"Lỗi không xác định khi tải Cog '{extension}': {e}", exc_info=True)
        log.info("Logic setup hook đã hoàn thành.")


# --- Cấu hình Bot ---
intents = bot_setup.create_intents()
bot = ShiromiBot(intents=intents)

# --- Đăng ký Event Handlers từ module events ---
@bot.event
async def on_ready():
    await bot_events.handle_on_ready(bot)

@bot.event
async def on_command_error(ctx: commands.Context, error):
    await bot_events.handle_on_command_error(ctx, error, bot)


# --- Chạy Bot ---
async def main():
    """Hàm async chính để thiết lập và chạy bot."""

    config.check_critical_config()
    bot_setup.configure_logging() 

    log.info("Đang cố gắng chạy bot...")
    try:
        log.info("Đang khởi động bot và kết nối tới Discord...")
        async with bot:
            await bot.start(config.BOT_TOKEN)
    except (discord.LoginFailure, discord.PrivilegedIntentsRequired) as e:
        log_reason = "Token không hợp lệ" if isinstance(e, discord.LoginFailure) else f"Thiếu Privileged Intents: {e}"
        log.critical(f"LỖI KHỞI ĐỘNG: {log_reason}.")
        discord_logging.stop_discord_log_thread()
        sys.exit(1)
    except KeyboardInterrupt:
        log.info("Nhận tín hiệu KeyboardInterrupt (Ctrl+C), đang tắt...")
    except SystemExit as e:
        log.info(f"Nhận tín hiệu SystemExit ({e.code}), đang tắt...")
    except Exception as e:
        log.critical(f"LỖI NGHIÊM TRỌNG không xử lý được trong main loop: {e}", exc_info=True)
    finally:
        log.info("Đang chạy dọn dẹp cuối cùng...")
        discord_logging.stop_discord_log_thread()
        await database.close_db()
        log.info("[bold blue]Bot đã tắt hoàn toàn.[/bold blue]")

if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("KeyboardInterrupt bắt được trong __main__, chương trình kết thúc.")
    except Exception as main_err:
        print(f"Lỗi không xử lý được trong __main__: {main_err}")
        traceback.print_exc()
        sys.exit(1)

# --- END OF FILE bot.py ---