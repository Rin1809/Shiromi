# --- START OF FILE bot.py ---
import sys
import os
import asyncio
import logging
import traceback # Thêm traceback để log lỗi main

import discord
from discord.ext import commands

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
        super().__init__(command_prefix=config.COMMAND_PREFIX, intents=intents, help_command=None)

    async def setup_hook(self): # Ghi đè phương thức setup_hook
        """Chạy một lần trước khi bot kết nối."""
        log.info("Đang chạy logic setup hook...")

        # Cung cấp tham chiếu bot (self) cho các module cần thiết
        utils.set_bot_reference_for_emoji(self) # Dùng self
        discord_logging.setup_discord_logging(self) # Dùng self

        # Kết nối database
        await database.connect_db()

        # Khởi động thread gửi log Discord
        loop = asyncio.get_running_loop()
        discord_logging.start_discord_log_thread(loop)

        # Tải các Cogs (lệnh)
        log.info("Đang tải Cogs...")
        initial_extensions = [
            "cogs.deep_scan_cog",
            # Thêm các cog khác vào đây ví dụ: "cogs.moderation_cog"
        ]
        for extension in initial_extensions:
            try:
                await self.load_extension(extension) # Dùng self.load_extension
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
# Khởi tạo bot bằng lớp con ShiromiBot
bot = ShiromiBot(intents=intents)

# --- Đăng ký Event Handlers từ module events ---

@bot.event
async def on_ready():
    await bot_events.handle_on_ready(bot) # Truyền bot instance vào handler

@bot.event
async def on_command_error(ctx: commands.Context, error):
    await bot_events.handle_on_command_error(ctx, error, bot) # Truyền bot instance


# --- Chạy Bot ---
async def main():
    """Hàm async chính để thiết lập và chạy bot."""
    config.check_critical_config()
    bot_setup.configure_logging()

    log.info("Đang cố gắng chạy bot...")
    try:
        log.info("Đang khởi động bot và kết nối tới Discord...")
        async with bot:
            # setup_hook sẽ tự động chạy trước khi start
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
        traceback.print_exc() # In traceback cho lỗi ở __main__
        sys.exit(1)

# --- END OF FILE bot.py ---