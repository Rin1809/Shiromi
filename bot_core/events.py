# --- START OF FILE bot_core/events.py ---
import sys
import logging
import traceback
import discord
from discord.ext import commands

import config
import utils
import database 
import discord_logging 

log = logging.getLogger(__name__)

async def handle_on_ready(bot: commands.Bot):
    """Xử lý sự kiện khi bot sẵn sàng."""
    e = lambda n: utils.get_emoji(n, bot) 

    print("-" * 50) 
    log.info(f'{e("success")} Đã đăng nhập với tư cách [bold cyan]{bot.user.name}[/] (ID: {bot.user.id})')
    log.info(f' discord.py [blue]{discord.__version__}[/]')
    log.info(f' Python [green]{sys.version.split(" ")[0]}[/]')
    log.info(f'{e("members")} Đã kết nối tới [magenta]{len(bot.guilds)}[/] máy chủ.')
    try:
        # Đếm emoji tùy chỉnh bot có thể truy cập
        # Không cần fetch ở đây nếu bot đã cache đủ, nhưng fetch đảm bảo hơn
        custom_emojis = bot.emojis 
        if not custom_emojis:
             log.debug("Cache emoji rỗng, đang thử fetch...")
             custom_emojis = await bot.fetch_emojis()
        log.info(f"{e('mention')} Đã tải [magenta]{len(custom_emojis)}[/] emoji tùy chỉnh.")
    except Exception as emoji_err:
        log.warning(f"Không thể fetch/đếm emoji tùy chỉnh: {emoji_err}")
    log.info('[dim]------[/dim]')

    # --- Kiểm tra Emoji ---
    log.info("Đang kiểm tra cấu hình Emoji...")
    required_emojis = [
        'success', 'error', 'loading', 'stats', 'role', 'thread', 'csv_file',
        'json_file', 'warning', 'info', 'hashtag', 'voice_channel', 'text_channel',
        'category', 'members', 'clock', 'calendar', 'mention', 'crown', 'shield',
        'invite', 'webhook', 'integration', 'link', 'image', 'sticker', 'award', 'reply'
        # Thêm các emoji cần thiết khác ở đây
    ]
    missing_emoji_report = []
    for name in required_emojis:
        emoji_val = utils.get_emoji(name, bot)
        is_placeholder = ':123' in emoji_val 
        is_fallback = emoji_val == utils.EMOJI_IDS.get(name, "❓") and name in utils.EMOJI_IDS
        is_missing = emoji_val == "❓" and name not in utils.EMOJI_IDS 

        status_str = ""
        if is_missing:
            status_str = " ([red]THIẾU![/])"
            missing_emoji_report.append(f"- `{name}`: {emoji_val}{status_str}")
        elif is_fallback:
            status_str = " ([yellow]Fallback[/])"
            missing_emoji_report.append(f"- `{name}`: {emoji_val}{status_str}")
        elif is_placeholder:
            status_str = " ([yellow]Placeholder[/])"
            missing_emoji_report.append(f"- `{name}`: {emoji_val}{status_str}")
        else:
            status_str = " ([green]OK[/])"

        # Log trạng thái của từng emoji 
        log.debug(f"  Emoji '{name}': {emoji_val}{status_str}")

    if missing_emoji_report:
        log.warning("[bold yellow]CẢNH BÁO EMOJI:[/bold yellow] Một số emoji bị thiếu hoặc dùng fallback/placeholder:")
        for report_line in missing_emoji_report:
            log.warning(report_line)
    else:
        log.info("[green]Kiểm tra Emoji: OK[/green]")
    log.info('[dim]------[/dim]')

    # --- Cập nhật Trạng thái Hoạt động (Presence) ---
    try:
        activity_name = f"{config.BOT_NAME} | {config.COMMAND_PREFIX}help"
        activity = discord.Activity(type=discord.ActivityType.watching, name=activity_name)
        await bot.change_presence(activity=activity, status=discord.Status.online)
        log.info(f"{e('online')} Trạng thái hoạt động đã được cập nhật: Watching '{activity_name}'")
    except Exception as e_act:
        log.warning(f"Không thể đặt trạng thái hoạt động: {e_act}")

    print("-" * 50)


async def handle_on_command_error(ctx: commands.Context, error, bot: commands.Bot):
    """Xử lý lỗi lệnh một cách tập trung."""
    e = lambda name: utils.get_emoji(name, bot)

    # --- LOG DEBUG CHI TIẾT ---
    log.debug(f"--- on_command_error invoked ---")
    log.debug(f"Context: Command='{ctx.command.qualified_name if ctx.command else 'None'}', Author='{ctx.author}', Guild='{ctx.guild}'")
    log.debug(f"Error Type: {type(error).__name__}")
    log.debug(f"Error Value: {error}")
    # --- KẾT THÚC LOG DEBUG ---

    original_exception = None
    original_error_info = "" 

    if isinstance(error, commands.CommandInvokeError):
        original_exception = error.original
        original_error_info = f"\n   [bold]Lỗi gốc:[/bold] {type(original_exception).__name__}: {original_exception}" 
        # --- LOG DEBUG LỖI GỐC ---
        log.debug(f"--> Original Exception Type: {type(original_exception).__name__}")
        log.debug(f"--> Original Exception Value: {original_exception}")

        try:
            # Tạo chuỗi traceback thủ công
            tb_lines = traceback.format_exception(type(original_exception), original_exception, original_exception.__traceback__)
            tb_string = "".join(tb_lines)
            log.error(f"--> Original Exception Traceback:\n{tb_string}") 
        except Exception as tb_err:
            log.warning(f"Không thể log traceback của lỗi gốc: {tb_err}")
        # --- KẾT THÚC LOG DEBUG LỖI GỐC ---


    # Bỏ qua lỗi CommandNotFound vì nó khá phổ biến và không cần log nhiều
    if isinstance(error, commands.CommandNotFound):
        return

    # Log lỗi chi tiết vào console/file (bao gồm cả traceback nếu là lỗi invoke)
    # Di chuyển log lỗi chính xuống sau khi đã lấy original_error_info
    log.error(
        f"{e('error')} Lỗi lệnh '[yellow]{ctx.command.qualified_name if ctx.command else 'Không rõ'}[/]' "
        f"bởi [cyan]{ctx.author}[/] ({ctx.author.id}) "
        f"tại server [magenta]{ctx.guild.name if ctx.guild else 'DM'}[/] ({ctx.guild.id if ctx.guild else 'N/A'}): "
        f"[bold red]{error}[/bold red]{original_error_info}", 
        exc_info=isinstance(error, commands.CommandInvokeError) 
    )


    # --- Chuẩn bị tin nhắn phản hồi cho người dùng ---
    msg = None
    reset_cooldown = False 

    # Phân loại lỗi và tạo tin nhắn phản hồi thân thiện
    if isinstance(error, commands.MissingPermissions):
        perms_list = ', '.join(f"`{perm}`" for perm in error.missing_permissions)
        msg = f"{e('error')} Bạn không có quyền {perms_list} để dùng lệnh này."
    elif isinstance(error, commands.BotMissingPermissions):
        perms_list = ', '.join(f"`{perm}`" for perm in error.missing_permissions)
        msg = f"{e('error')} Bot thiếu quyền {perms_list} để thực hiện lệnh này."
        reset_cooldown = True # Lỗi do bot, nên reset cooldown cho user
    elif isinstance(error, commands.CheckFailure):
        # Các check cụ thể 
        if isinstance(error, commands.GuildOnly):
            msg = f"{e('error')} Lệnh này chỉ có thể dùng trong server."
        elif isinstance(error, commands.NotOwner):
            msg = f"{e('error')} Chỉ chủ sở hữu bot mới dùng được lệnh này."
        elif isinstance(error, commands.MissingRole):
             missing_role = f"role ID `{error.missing_role}`" if isinstance(error.missing_role, int) else f"role `{error.missing_role}`"
             msg = f"{e('error')} Bạn cần có {missing_role} để dùng lệnh này."
        elif isinstance(error, commands.MissingAnyRole):
             missing_roles = ", ".join(f"`{r}`" if isinstance(r, int) else f"'{r}'" for r in error.missing_roles)
             msg = f"{e('error')} Bạn cần có ít nhất một trong các roles: {missing_roles} để dùng lệnh này."
        # Check chung
        else:
            msg = f"{e('error')} Bạn không đáp ứng điều kiện để chạy lệnh này."
            reset_cooldown = True # Reset cooldown nếu check fail không rõ lý do
    elif isinstance(error, commands.CommandOnCooldown):
        msg = f"{e('clock')} Lệnh đang trong thời gian hồi. Vui lòng chờ **{error.retry_after:.1f} giây**."
    elif isinstance(error, commands.UserInputError):
        # Các lỗi nhập liệu cụ thể
        if isinstance(error, commands.MissingRequiredArgument):
            msg = f"{e('warning')} Thiếu tham số bắt buộc: `{error.param.name}`."
        elif isinstance(error, (commands.BadArgument, commands.ConversionError)):
            msg = f"{e('warning')} Tham số không hợp lệ: {error}"
        elif isinstance(error, commands.TooManyArguments):
             msg = f"{e('warning')} Bạn đã nhập quá nhiều tham số."
        # Lỗi nhập liệu chung
        else:
            msg = f"{e('warning')} Sai cú pháp hoặc tham số không hợp lệ."
        # Gợi ý dùng help
        if ctx.command:
            msg += f" Dùng `{config.COMMAND_PREFIX}help {ctx.command.qualified_name}` để xem hướng dẫn."

    elif isinstance(error, commands.CommandInvokeError) and original_exception:
        # Xử lý lỗi gốc từ CommandInvokeError
        original = original_exception
        if isinstance(original, discord.Forbidden):
            msg = f"{e('error')} Bot không có quyền thực hiện hành động này: `{original.text}` (Code: {original.code})"
            reset_cooldown = True # Lỗi do bot
        elif isinstance(original, discord.HTTPException):
            msg = f"{e('error')} Lỗi mạng hoặc lỗi từ Discord (HTTP {original.status}): `{original.text}`"
            reset_cooldown = True # Lỗi môi trường
        elif isinstance(original, (ConnectionError, database.asyncpg.exceptions.PostgresError if database.asyncpg else ConnectionError)):
            # Che giấu chi tiết lỗi DB khỏi người dùng
            msg = f"{e('error')} Lỗi kết nối cơ sở dữ liệu hoặc mạng. Vui lòng thử lại sau."
            reset_cooldown = True # Lỗi môi trường
        # --- KIỂM TRA NAMEERROR ---
        elif isinstance(original, NameError): # Kiểm tra NameError cụ thể
             log.error(f"!!! Phát hiện NameError gốc: {original}", exc_info=False) # Log rõ ràng lỗi NameError
             msg = f"{e('error')} Lỗi logic chương trình (NameError). Lỗi đã được ghi lại."
             # Không reset cooldown vì có thể do input gây ra lỗi logic
        # --- KẾT THÚC KIỂM TRA NAMEERROR ---
        elif isinstance(original, (AttributeError, TypeError, KeyError, IndexError)):
             # Đây thường là lỗi logic trong code bot
             msg = f"{e('error')} Lỗi logic chương trình ({type(original).__name__}). Lỗi đã được ghi lại."
             # Không reset cooldown
        else:
            # Lỗi không xác định khác trong quá trình chạy lệnh
            msg = f"{e('error')} Đã xảy ra lỗi khi thực thi lệnh! Vui lòng kiểm tra log chi tiết."
            log.error(f"Lỗi không xác định trong CommandInvokeError: {type(original).__name__}: {original}", exc_info=True) # Log lỗi lạ này
            reset_cooldown = True
    else:
        # Các loại lỗi khác chưa được xử lý cụ thể ở trên
        msg = f"{e('error')} Lỗi không xác định: {type(error).__name__}"
        log.warning(f"Unhandled command error type: {type(error).__name__} - {error}")
        reset_cooldown = True # Reset cho lỗi lạ

    # Gửi tin nhắn lỗi tới người dùng 
    if msg:
        try:
            # Gửi vào kênh gốc hoặc DM nếu kênh gốc không gửi được
            await ctx.send(msg, delete_after=20)
        except discord.HTTPException as send_err:
            log.warning(f"Không thể gửi tin nhắn lỗi vào kênh {ctx.channel.id}: {send_err.status}")
            try:
                # Thử gửi DM cho người dùng
                await ctx.author.send(f"Lỗi khi chạy lệnh `{ctx.command.qualified_name if ctx.command else 'unknown'}` tại server `{ctx.guild.name if ctx.guild else 'DM'}`:\n{msg}")
            except discord.HTTPException:
                log.warning(f"Không thể gửi tin nhắn lỗi DM cho {ctx.author.id}.")
            except AttributeError: 
                 log.warning(f"Không thể gửi tin nhắn lỗi DM cho {ctx.author} (không phải User/Member?).")
        except Exception as send_e:
             log.error(f"Lỗi lạ khi gửi tin nhắn lỗi: {send_e}", exc_info=True)


    if reset_cooldown and ctx.command and hasattr(ctx.command, "reset_cooldown"):
        ctx.command.reset_cooldown(ctx)
        log.info(f"Đã reset cooldown cho lệnh '{ctx.command.qualified_name}' của user {ctx.author.id} do lỗi.")

    log.debug(f"--- on_command_error finished ---") 
# --- END OF FILE bot_core/events.py ---