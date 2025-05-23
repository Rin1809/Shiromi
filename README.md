# Shiromi - Bot Phân Tích Sâu Server Discord ᓚᘏᗢ

<!-- Vietnamese -->
<details>
<summary>🇻🇳 Tiếng Việt</summary>

## Giới thiệu

**Shiromi** là một bot Discord mạnh mẽ được thiết kế để thực hiện quét và phân tích sâu dữ liệu hoạt động của một server Discord. Bot thu thập thông tin chi tiết về tin nhắn, hoạt động của thành viên, việc sử dụng emoji/sticker, vai trò, kênh, luồng và nhiều hơn nữa. Kết quả phân tích được trình bày dưới dạng các báo cáo embeds trực quan trong Discord, file xuất CSV/JSON, và báo cáo DM cá nhân hóa cho thành viên.

Ngoài ra, Shiromi có khả năng lưu trữ dữ liệu quét vào cơ sở dữ liệu PostgreSQL, cho phép tra cứu và hiển thị thông tin qua một giao diện web (nếu được tích hợp).

**Các chức năng chính:**

*   **Quét Toàn Diện:** Thu thập dữ liệu từ tất cả các kênh text, voice (chat), và luồng (bao gồm cả luồng lưu trữ nếu có quyền).
*   **Phân Tích Hoạt Động:** Đếm tin nhắn, link, ảnh, emoji, sticker, lượt nhắc, trả lời, reaction (đã lọc) của từng thành viên và toàn server.
*   **Thống Kê Phụ Trợ:** Lấy thông tin boosters, kênh voice/stage, invites, webhooks, integrations, thành viên lâu năm nhất, và phân tích audit log (ví dụ: theo dõi lượt cấp role, tạo thread).
*   **Báo Cáo Đa Dạng:**
    *   **Embeds Discord:** Hiển thị các bảng xếp hạng và thống kê trực tiếp trong kênh Discord được chỉ định.
    *   **Xuất File:** Tạo file CSV và JSON chi tiết cho các mục dữ liệu khác nhau. (Đang thi công)
    *   **DM Cá Nhân:** Gửi báo cáo tóm tắt hoạt động và thành tích cá nhân cho từng thành viên (dựa trên role cấu hình hoặc cho admin ở chế độ test).
*   **Lưu Trữ Database:** Lưu kết quả quét và dữ liệu người dùng vào PostgreSQL để có thể truy cập qua web.
*   **Logging Chi Tiết:** Ghi log quá trình quét vào một thread Discord riêng biệt để dễ theo dõi.
*   **Tùy Biến Cao:** Nhiều tùy chọn cấu hình qua file `.env` (ví dụ: loại trừ category, theo dõi role cụ thể, ID sticker/emoji, kênh báo cáo).
*   **Hỗ Trợ Proxy Bot:** Có thể hoạt động như một bot worker, nhận lệnh từ một bot proxy chính (ví dụ: Mizuki).

## Tính năng

*   **Phân tích Server:**
    *   Thông tin chung server (owner, ngày tạo, boost, số lượng kênh/role/emoji/sticker).
    *   Thống kê quét (số kênh/luồng đã xử lý, tổng tin nhắn, tổng reaction đã lọc, thời gian quét).
    *   Bảng xếp hạng hoạt động kênh text và voice (chat).
    *   "Giờ Vàng" và "Giờ Âm" của server và các kênh/luồng.
    *   Top emoji/sticker được sử dụng nhiều/ít nhất server.
    *   Emoji server không được sử dụng.
*   **Phân tích Thành viên:**
    *   Bảng xếp hạng người dùng hoạt động nhiều/ít nhất (tin nhắn, link, ảnh, emoji server, sticker, mention gửi/nhận, trả lời, reaction gửi/nhận, số kênh hoạt động, thời gian hoạt động).
    *   Bảng xếp hạng người mời nhiều nhất (theo lượt dùng invite).
    *   Bảng xếp hạng booster "bền bỉ" nhất.
    *   Bảng xếp hạng thành viên lâu năm nhất.
    *   Bảng xếp hạng người tạo thread nhiều/ít nhất.
    *   Theo dõi và xếp hạng lượt nhận các role đặc biệt (từ Audit Log).
*   **Tìm Kiếm Từ Khóa:**
    *   Tìm kiếm các từ khóa cụ thể trong tin nhắn.
    *   Thống kê tổng số lần xuất hiện, top kênh/luồng và top user theo từ khóa.
*   **Xuất Dữ Liệu:** (Đang thi công)
    *   File CSV chi tiết cho thông tin server, kênh/luồng, hoạt động người dùng, roles, boosters, invites, webhooks, integrations, audit logs, và các bảng xếp hạng.
    *   File JSON tổng hợp toàn bộ dữ liệu quét.
*   **Báo Cáo DM Cá Nhân:**
    *   Tóm tắt hoạt động cá nhân (tin nhắn, nội dung gửi, tương tác, thời gian hoạt động, phạm vi hoạt động).
    *   Top items cá nhân (emoji, sticker).
    *   "Giờ Vàng" cá nhân.
    *   Thành tích và vị trí trong các bảng xếp hạng của server.
    *   Lời cảm ơn và ảnh cá nhân hóa cho các role đặc biệt (booster, người đóng góp).
*   **Kỹ thuật:**
    *   Sử dụng `asyncio` và `asyncpg` cho các hoạt động bất đồng bộ và tương tác database hiệu quả.
    *   Logging chi tiết lên console (sử dụng `rich`) và thread Discord.
    *   Cấu hình intents Discord linh hoạt.
    *   Quản lý lỗi và cooldown cho lệnh.
    *   Hỗ trợ PROXY_BOT_ID để nhận lệnh từ bot khác.

## Điều kiện tiên quyết

1.  **Python:** Phiên bản 3.8 trở lên.
2.  **Git:** Để tải mã nguồn.
3.  **PostgreSQL Server:** Một instance PostgreSQL đang chạy và có thể truy cập.
4.  (Tùy chọn) Một bot Discord khác để làm PROXY_BOT_ID nếu bạn muốn sử dụng tính năng này.

## Cài đặt

1.  **Tải mã nguồn:**
    ```bash
    git clone https://github.com/Rin1809/Shiromi
    cd Shiromi
    ```

2.  **Tạo môi trường ảo (khuyến nghị):**
    ```bash
    python -m venv venv
    # Windows
    venv\Scripts\activate
    # Linux/macOS
    source venv/bin/activate
    ```

3.  **Cài đặt thư viện:**
    ```bash
    pip install -r requirements.txt
    ```

## Cấu hình

1.  **Sao chép file `.env_example.md` thành `.env`:**
    ```bash
    # Windows
    copy .env_example.md .env
    # Linux/macOS
    cp .env_example.md .env
    ```

2.  **Chỉnh sửa file `.env` với các thông tin của bạn:**
    *   `DISCORD_TOKEN`: Token của bot discord của bạn.
    *   `DATABASE_URL`: Chuỗi kết nối đến PostgreSQL của bạn (ví dụ: `postgresql://user:password@host:port/database`).
    *   `ADMIN_USER_ID`: ID người dùng Discord của chủ sở hữu bot (quan trọng cho quyền `is_owner()`).
    *   `PROXY_BOT_ID` (Tùy chọn): ID của bot proxy (ví dụ: Mizuki) nếu bạn muốn Shiromi nhận lệnh từ bot đó.
    *   `BOT_NAME`: Tên bot sẽ hiển thị trong một số tin nhắn.
    *   `COMMAND_PREFIX`: Tiền tố lệnh (ví dụ: `Shi`).
    *   `EXCLUDED_CATEGORY_IDS` (Tùy chọn): Danh sách ID category cần loại trừ khỏi quét, cách nhau bởi dấu phẩy.
    *   `FINAL_STICKER_ID`, `INTERMEDIATE_STICKER_ID`, `LEAST_STICKER_ID`, `MOST_STICKER_ID` (Tùy chọn): ID các sticker sẽ được gửi ở các giai đoạn khác nhau của báo cáo.
    *   `WEBSITE_BASE_URL` (Tùy chọn): URL gốc của trang web hiển thị dữ liệu quét (nếu có).
    *   `REPORT_CHANNEL_ID` (Tùy chọn): ID kênh Discord để gửi báo cáo embeds công khai. Nếu không đặt, sẽ gửi vào kênh gốc nơi lệnh được gọi.
    *   `FINAL_DM_EMOJI` (Tùy chọn): Emoji gửi cuối mỗi DM cá nhân.
    *   `TRACKED_ROLE_GRANT_IDS` (Tùy chọn): ID các role cần theo dõi lượt cấp qua Audit Log.
    *   `DM_REPORT_RECIPIENT_ROLE_ID` (Tùy chọn): ID của role mà thành viên có role này sẽ nhận DM báo cáo.
    *   `BOOSTER_THANKYOU_ROLE_IDS` (Tùy chọn): ID các role (booster, đóng góp) để gửi lời cảm ơn đặc biệt và ảnh cá nhân hóa trong DM.
    *   `ADMIN_ROLE_IDS_FILTER` (Tùy chọn): ID các role admin khác (ngoài quyền Administrator của server) cần lọc khỏi một số BXH.
    *   `REACTION_UNICODE_EXCEPTIONS` (Tùy chọn): Danh sách emoji Unicode được phép xuất hiện trong BXH reaction (ngoài emoji của server).
    *   `ENABLE_REACTION_SCAN` (Tùy chọn): Đặt là `true` để bật quét reaction (có thể làm chậm quá trình quét).
    *   `MAX_CONCURRENT_CHANNEL_SCANS` (Tùy chọn): Số kênh/luồng quét đồng thời tối đa (mặc định là 5).

3.  **Cấu hình ảnh cá nhân hóa cho DM (Tùy chọn):**
    *   Chỉnh sửa file `quy_toc_anh.json`.
    *   Thêm các cặp `"USER_ID": "IMAGE_URL"` cho những người dùng có `BOOSTER_THANKYOU_ROLE_IDS` mà bạn muốn họ nhận ảnh riêng trong DM.

## Chạy Bot

Sau khi cài đặt và cấu hình:
```bash
python bot.py
```
Bot sẽ kết nối tới Discord và sẵn sàng nhận lệnh.

## Sử dụng Lệnh

Các lệnh chính được gọi qua tiền tố đã cấu hình (ví dụ: `Shi`).

*   **Chế độ Test (Gửi DM cho Admin):**
    *   `[prefix]romi [export_csv=True/False] [export_json=True/False] [keywords=từ khóa1,từ khóa2]`
    *   Ví dụ: `Shi romi export_csv=True keywords=chào,tạm biệt`
    *   Mặc định: `export_csv=False`, `export_json=False`, không tìm keywords.
    *   Báo cáo DM cá nhân sẽ được gửi đến `ADMIN_USER_ID` đã cấu hình.
*   **Chế độ Bình Thường (Gửi DM cho Role Cấu Hình):**
    *   `[prefix]Shiromi [export_csv=True/False] [export_json=True/False] [keywords=từ khóa1,từ khóa2]`
    *   Ví dụ: `Shi Shiromi export_json=True`
    *   Báo cáo DM cá nhân sẽ được gửi đến những người dùng có `DM_REPORT_RECIPIENT_ROLE_ID`.
*   **Kiểm tra Bot:**
    *   `[prefix]ping_shiromi`
    *   Kiểm tra xem bot có phản hồi không và hiển thị độ trễ.

**Lưu ý về PROXY_BOT_ID:**
Nếu `PROXY_BOT_ID` được cấu hình, bot đó có thể gọi lệnh của Shiromi bằng cách gửi tin nhắn bắt đầu trực tiếp bằng tên lệnh (không cần tiền tố `COMMAND_PREFIX` của Shiromi). Ví dụ, nếu bot proxy gửi `romi export_csv=True`, Shiromi sẽ hiểu và thực thi.

## Cấu trúc thư mục

```
Shiromi/
├── .git/
├── __pycache__/
├── bot_core/
│   ├── __init__.py
│   ├── events.py
│   ├── setup.py
├── cogs/
│   ├── deep_scan_helpers/
│   │   ├── __init__.py
│   │   ├── data_processing.py
│   │   ├── dm_sender.py
│   │   ├── export_generation.py
│   │   ├── finalization.py
│   │   ├── init_scan.py
│   │   ├── report_generation.py
│   │   ├── scan_channels.py
│   ├── __init__.py
│   ├── deep_scan_cog.py
├── moitruongao/ (Môi trường ảo Python, vd: venv)
├── reporting/
│   ├── __init__.py
│   ├── csv_writer.py
│   ├── embeds_analysis.py
│   ├── embeds_dm.py
│   ├── embeds_guild.py
│   ├── embeds_items.py
│   ├── embeds_user.py
│   ├── json_writer.py
├── .env                    # Đặt .env ở đây, biến môi trường (QUAN TRỌNG, BÍ MẬT)
├── .env_example.md         # File ví dụ cho .env
├── .gitignore
├── bot.py                  # File chạy bot chính
├── config.py               # Tải và quản lý cấu hình
├── database.py             # Tương tác với cơ sở dữ liệu PostgreSQL
├── discord_logging.py      # Gửi log lên thread Discord
├── quy_toc_anh.json        # Mapping ảnh cá nhân cho DM
├── README.md               # File bạn đang đọc
├── requirements.txt        # Danh sách thư viện Python
├── scanner.py              # (Trống - có thể dành cho chức năng tương lai)
└── utils.py                # Các hàm tiện ích chung
```

## Lưu ý Quan trọng

*   **Quyền Bot:** Shiromi cần nhiều quyền Discord để hoạt động đầy đủ (bao gồm các Privileged Intents như Guild Members, Message Content, và các quyền server như View Audit Log, Manage Server, Read Message History, Create Public Threads, Embed Links, Attach Files). Đảm bảo bot có đủ quyền trên Developer Portal và trong server.
*   **Cơ sở dữ liệu:** Kết nối và thiết lập bảng PostgreSQL là bắt buộc. Bot sẽ không hoạt động nếu không có database.
*   **Tài nguyên:** Quá trình quét sâu có thể tốn thời gian và tài nguyên (CPU, RAM, API rate limit của Discord), đặc biệt trên các server lớn hoặc khi bật quét reaction.
*   **API Rate Limits:** Bot cố gắng xử lý rate limit của Discord, nhưng với các server cực lớn, việc quét có thể bị gián đoạn.
*   **Bảo mật PROXY_BOT_ID:** Nếu sử dụng, đảm bảo rằng chỉ bot proxy đáng tin cậy mới có ID đó, vì nó có thể thực thi các lệnh mạnh mẽ của Shiromi.

</details>

<!-- English -->
<details>
<summary>🇬🇧 English</summary>

## Introduction

**Shiromi** is a powerful Discord bot designed to perform in-depth scans and analysis of a Discord server's activity data. It collects detailed information about messages, member activity, emoji/sticker usage, roles, channels, threads, and much more. The analysis results are presented as visually appealing embed reports in Discord, CSV/JSON export files, and personalized DM reports for members.

Additionally, Shiromi can store scan data in a PostgreSQL database, enabling data retrieval and display through a web interface (if integrated).

**Main functionalities:**

*   **Comprehensive Scanning:** Collects data from all text channels, voice channels (chat), and threads (including archived threads if permissions allow).
*   **Activity Analysis:** Counts messages, links, images, emojis, stickers, mentions, replies, and (filtered) reactions for each member and the entire server.
*   **Auxiliary Statistics:** Fetches information on boosters, voice/stage channels, invites, webhooks, integrations, oldest members, and analyzes audit logs (e.g., tracking role grants, thread creations).
*   **Diverse Reporting:**
    *   **Discord Embeds:** Displays leaderboards and statistics directly in a designated Discord channel.
    *   **File Exports:** Generates detailed CSV and JSON files for various data categories. (Working on it)
    *   **Personalized DMs:** Sends summary reports of individual activity and achievements to members (based on configured roles or to an admin in test mode).
*   **Database Storage:** Saves scan results and user data to PostgreSQL for potential web-based access.
*   **Detailed Logging:** Logs the scanning process to a separate Discord thread for easy monitoring.
*   **Highly Configurable:** Many customization options via the `.env` file (e.g., exclude categories, track specific roles, sticker/emoji IDs, report channel).
*   **Proxy Bot Support:** Can function as a worker bot, receiving commands from a main proxy bot (e.g., Mizuki).

## Features

*   **Server Analysis:**
    *   General server information (owner, creation date, boost level, counts of channels/roles/emojis/stickers).
    *   Scan summary (processed channels/threads, total messages, total filtered reactions, scan duration).
    *   Activity leaderboards for text and voice (chat) channels.
    *   Server and channel/thread "Golden Hours" (most active) and "Umbra Hours" (least active).
    *   Top most/least used server emojis/stickers.
    *   Unused server emojis.
*   **Member Analysis:**
    *   Leaderboards for most/least active users (messages, links, images, server emojis, stickers, mentions sent/received, replies, reactions sent/received, distinct channels active in, activity span).
    *   Top inviters leaderboard (by invite uses).
    *   "Most Enduring" booster leaderboard.
    *   Oldest members leaderboard.
    *   Most/least thread creators leaderboard.
    *   Tracking and ranking of grants for special roles (from Audit Log).
*   **Keyword Search:**
    *   Search for specific keywords in messages.
    *   Statistics on total occurrences, top channels/threads, and top users by keyword.
*   **Data Export:**
    *   Detailed CSV files for server info, channels/threads, user activity, roles, boosters, invites, webhooks, integrations, audit logs, and leaderboards.
    *   Comprehensive JSON file of all scanned data.
*   **Personalized DM Reports:**
    *   Summary of personal activity (messages, content sent, interactions, activity time, scope of activity).
    *   Top personal items (emojis, stickers).
    *   Personal "Golden Hour."
    *   Achievements and server ranking positions.
    *   Personalized thank-you messages and images for special roles (boosters, contributors).
*   **Technical:**
    *   Uses `asyncio` and `asyncpg` for efficient asynchronous operations and database interaction.
    *   Detailed logging to console (using `rich`) and a Discord thread.
    *   Flexible Discord intents configuration.
    *   Error handling and command cooldowns.
    *   Supports `PROXY_BOT_ID` for receiving commands from another bot.

## Prerequisites

1.  **Python:** Version 3.8 or higher.
2.  **Git:** To clone the source code.
3.  **PostgreSQL Server:** A running and accessible PostgreSQL instance.
4.  (Optional) Another Discord bot to act as `PROXY_BOT_ID` if you wish to use this feature.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Rin1809/Shiromi
    cd Shiromi
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    # Windows
    venv\Scripts\activate
    # Linux/macOS
    source venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

1.  **Copy `.env_example.md` to `.env`:**
    ```bash
    # Windows
    copy .env_example.md .env
    # Linux/macOS
    cp .env_example.md .env
    ```

2.  **Edit the `.env` file with your information:**
    *   `DISCORD_TOKEN`: Your discord bot's token.
    *   `DATABASE_URL`: Connection string for your PostgreSQL (e.g., `postgresql://user:password@host:port/database`).
    *   `ADMIN_USER_ID`: Your Discord user ID (important for `is_owner()` permissions).
    *   `PROXY_BOT_ID` (Optional): ID of the proxy bot (e.g., Mizuki) if you want Shiromi to accept commands from it.
    *   `BOT_NAME`: Bot's name to be displayed in some messages.
    *   `COMMAND_PREFIX`: Command prefix (e.g., `Shi`).
    *   `EXCLUDED_CATEGORY_IDS` (Optional): Comma-separated list of category IDs to exclude from scans.
    *   `FINAL_STICKER_ID`, `INTERMEDIATE_STICKER_ID`, `LEAST_STICKER_ID`, `MOST_STICKER_ID` (Optional): IDs of stickers to be sent at different reporting stages.
    *   `WEBSITE_BASE_URL` (Optional): Base URL of the website displaying scan data (if any).
    *   `REPORT_CHANNEL_ID` (Optional): Discord channel ID to send public embed reports. If not set, reports are sent to the channel where the command was invoked.
    *   `FINAL_DM_EMOJI` (Optional): Emoji sent at the end of each personalized DM.
    *   `TRACKED_ROLE_GRANT_IDS` (Optional): Role IDs to track grants for via Audit Log.
    *   `DM_REPORT_RECIPIENT_ROLE_ID` (Optional): ID of the role whose members will receive DM reports.
    *   `BOOSTER_THANKYOU_ROLE_IDS` (Optional): Role IDs (boosters, contributors) to send special thank-you messages and personalized images in DMs.
    *   `ADMIN_ROLE_IDS_FILTER` (Optional): Other admin role IDs (besides server Administrator permission) to filter from some leaderboards.
    *   `REACTION_UNICODE_EXCEPTIONS` (Optional): List of Unicode emojis allowed in reaction leaderboards (besides server emojis).
    *   `ENABLE_REACTION_SCAN` (Optional): Set to `true` to enable reaction scanning (can slow down scans).
    *   `MAX_CONCURRENT_CHANNEL_SCANS` (Optional): Maximum number of channels/threads to scan concurrently (default is 5).

3.  **Configure personalized DM images (Optional):**
    *   Edit the `quy_toc_anh.json` file.
    *   Add `"USER_ID": "IMAGE_URL"` pairs for users with `BOOSTER_THANKYOU_ROLE_IDS` whom you want to receive custom images in their DMs.

## Running the Bot

After installation and configuration:
```bash
python bot.py
```
The bot will connect to Discord and be ready to receive commands.

## Using Commands

Main commands are invoked using the configured prefix (e.g., `Shi`).

*   **Test Mode (Sends DMs to Admin):**
    *   `[prefix]romi [export_csv=True/False] [export_json=True/False] [keywords=keyword1,keyword2]`
    *   Example: `Shi romi export_csv=True keywords=hello,goodbye`
    *   Defaults: `export_csv=False`, `export_json=False`, no keywords.
    *   Personalized DM reports will be sent to the configured `ADMIN_USER_ID`.
*   **Normal Mode (Sends DMs to Configured Role):**
    *   `[prefix]Shiromi [export_csv=True/False] [export_json=True/False] [keywords=keyword1,keyword2]`
    *   Example: `Shi Shiromi export_json=True`
    *   Personalized DM reports will be sent to users with the `DM_REPORT_RECIPIENT_ROLE_ID`.
*   **Bot Check:**
    *   `[prefix]ping_shiromi`
    *   Checks if the bot is responsive and shows latency.

**Note on PROXY_BOT_ID:**
If `PROXY_BOT_ID` is configured, that bot can invoke Shiromi's commands by sending a message starting directly with the command name (no need for Shiromi's `COMMAND_PREFIX`). For example, if the proxy bot sends `romi export_csv=True`, Shiromi will understand and execute it.

## Folder Structure

```
Shiromi/
├── .git/
├── __pycache__/
├── bot_core/
│   ├── __init__.py
│   ├── events.py
│   ├── setup.py
├── cogs/
│   ├── deep_scan_helpers/
│   │   ├── __init__.py
│   │   ├── data_processing.py
│   │   ├── dm_sender.py
│   │   ├── export_generation.py
│   │   ├── finalization.py
│   │   ├── init_scan.py
│   │   ├── report_generation.py
│   │   ├── scan_channels.py
│   ├── __init__.py
│   ├── deep_scan_cog.py
├── moitruongao/ (Python virtual environment, e.g., venv)
├── reporting/
│   ├── __init__.py
│   ├── csv_writer.py
│   ├── embeds_analysis.py
│   ├── embeds_dm.py
│   ├── embeds_guild.py
│   ├── embeds_items.py
│   ├── embeds_user.py
│   ├── json_writer.py
├── .env                    # set it here, Environment variables (IMPORTANT, SECRET)
├── .env_example.md         # Example file for .env
├── .gitignore
├── bot.py                  # Main bot execution file
├── config.py               # Loads and manages configuration
├── database.py             # Interacts with PostgreSQL database
├── discord_logging.py      # Sends logs to a Discord thread
├── quy_toc_anh.json        # Mapping for personalized DM images
├── README.md               # This file
├── requirements.txt        # Python library dependencies
├── scanner.py              # (Empty - possibly for future features)
└── utils.py                # General utility functions
```

## Important Notes

*   **Bot Permissions:** Shiromi requires extensive Discord permissions to function fully (including Privileged Intents like Guild Members, Message Content, and server permissions such as View Audit Log, Manage Server, Read Message History, Create Public Threads, Embed Links, Attach Files). Ensure the bot has sufficient permissions on the Developer Portal and within the server.
*   **Database:** PostgreSQL connection and table setup are mandatory. The bot will not operate without a database.
*   **Resources:** Deep scans can be time-consuming and resource-intensive (CPU, RAM, Discord API rate limits), especially on large servers or when reaction scanning is enabled.
*   **API Rate Limits:** The bot attempts to handle Discord's rate limits, but scans on extremely large servers might still be interrupted.
*   **PROXY_BOT_ID Security:** If used, ensure that only the trusted proxy bot has that ID, as it can execute Shiromi's powerful commands.

</details>

<!-- Japanese -->
<details>
<summary>🇯🇵 日本語</summary>

## 概要

**Shiromi (シロミ)** は、Discordサーバーの活動データを詳細にスキャンおよび分析するために設計された強力なDiscordボットです。メッセージ、メンバーの活動、絵文字/スタンプの使用状況、ロール、チャンネル、スレッドなどに関する詳細情報を収集します。分析結果は、Discord内の視覚的に魅力的な埋め込みレポート、CSV/JSONエクスポートファイル、およびメンバー向けのパーソナライズされたDMレポートとして表示されます。

さらに、ShiromiはスキャンデータをPostgreSQLデータベースに保存でき、Webインターフェース（統合されている場合）を介したデータ検索と表示を可能にします。

**主な機能:**

*   **包括的スキャン:** すべてのテキストチャンネル、ボイスチャンネル（チャット）、スレッド（権限があればアーカイブ済みスレッドも含む）からデータを収集します。
*   **活動分析:** 各メンバーおよびサーバー全体のメッセージ、リンク、画像、絵文字、スタンプ、メンション、返信、および（フィルタリングされた）リアクションをカウントします。
*   **補助統計:** ブースター、ボイス/ステージチャンネル、招待、Webhook、連携、最古参メンバーに関する情報を取得し、監査ログを分析します（例: ロール付与、スレッド作成の追跡）。
*   **多様なレポート:**
    *   **Discord埋め込み:** 指定されたDiscordチャンネルにリーダーボードと統計を直接表示します。
    *   **ファイルエクスポート:** 様々なデータカテゴリに対応した詳細なCSVおよびJSONファイルを生成します。（まだです）
    *   **パーソナライズDM:** 個人の活動と実績の概要レポートをメンバーに送信します（設定されたロールに基づいて、またはテストモードでは管理者に）。
*   **データベース保存:** スキャン結果とユーザーデータをPostgreSQLに保存し、Web経由でのアクセスを可能にします。
*   **詳細ロギング:** スキャンプロセスを別のDiscordスレッドに記録し、監視を容易にします。
*   **高度な設定可能性:** `.env`ファイルを通じて多くのカスタマイズオプションを提供します（例: カテゴリ除外、特定ロールの追跡、スタンプ/絵文字ID、レポートチャンネル）。
*   **プロキシボット対応:** メインのプロキシボット（例: Mizuki）からコマンドを受信するワーカーボットとして機能できます。

## 機能

*   **サーバー分析:**
    *   一般サーバー情報（オーナー、作成日、ブーストレベル、チャンネル/ロール/絵文字/スタンプの数）。
    *   スキャン概要（処理済みチャンネル/スレッド数、総メッセージ数、総フィルタリング済みリアクション数、スキャン時間）。
    *   テキストおよびボイス（チャット）チャンネルの活動リーダーボード。
    *   サーバーおよびチャンネル/スレッドの「ゴールデンアワー」（最も活発）および「アンブラアワー」（最も閑散）。
    *   サーバーで最も/最も少なく使用された絵文字/スタンプ。
    *   未使用のサーバー絵文字。
*   **メンバー分析:**
    *   最も/最も活動の少ないユーザーのリーダーボード（メッセージ、リンク、画像、サーバー絵文字、スタンプ、送受信メンション、返信、送受信リアクション、活動チャンネル数、活動期間）。
    *   最多招待者リーダーボード（招待使用数による）。
    *   「最も長くブーストしている」ブースターのリーダーボード。
    *   最古参メンバーのリーダーボード。
    *   最多/最少スレッド作成者リーダーボード。
    *   特別ロールの付与追跡とランキング（監査ログから）。
*   **キーワード検索:**
    *   メッセージ内の特定のキーワードを検索。
    *   キーワードごとの総出現回数、トップチャンネル/スレッド、トップユーザーの統計。
*   **データエクスポート:**
    *   サーバー情報、チャンネル/スレッド、ユーザー活動、ロール、ブースター、招待、Webhook、連携、監査ログ、およびリーダーボードの詳細なCSVファイル。
    *   全スキャンデータの包括的なJSONファイル。
*   **パーソナライズDMレポート:**
    *   個人の活動概要（メッセージ、送信コンテンツ、インタラクション、活動時間、活動範囲）。
    *   個人のトップアイテム（絵文字、スタンプ）。
    *   個人の「ゴールデンアワー」。
    *   サーバーランキングにおける実績と順位。
    *   特別ロール（ブースター、貢献者）へのパーソナライズされた感謝メッセージと画像。
*   **技術仕様:**
    *   効率的な非同期操作とデータベース対話のために`asyncio`と`asyncpg`を使用。
    *   コンソール（`rich`を使用）およびDiscordスレッドへの詳細ロギング。
    *   柔軟なDiscordインテント設定。
    *   エラー処理とコマンドクールダウン。
    *   別のボットからコマンドを受信するための`PROXY_BOT_ID`をサポート。

## 前提条件

1.  **Python:** バージョン3.8以上。
2.  **Git:** ソースコードのクローン用。
3.  **PostgreSQLサーバー:** 実行中でアクセス可能なPostgreSQLインスタンス。
4.  (任意) この機能を使用したい場合は、`PROXY_BOT_ID`として機能する別のDiscordボット。

## インストール

1.  **リポジトリのクローン:**
    ```bash
    git clone https://github.com/Rin1809/Shiromi
    cd Shiromi
    ```

2.  **仮想環境の作成 (推奨):**
    ```bash
    python -m venv venv
    # Windows
    venv\Scripts\activate
    # Linux/macOS
    source venv/bin/activate
    ```

3.  **依存関係のインストール:**
    ```bash
    pip install -r requirements.txt
    ```

## 設定

1.  **`.env_example.md` を `.env` にコピー:**
    ```bash
    # Windows
    copy .env_example.md .env
    # Linux/macOS
    cp .env_example.md .env
    ```

2.  **`.env` ファイルを編集して情報を入力:**
    *   `DISCORD_TOKEN`: あなたのDiscordボットのトークン。
    *   `DATABASE_URL`: PostgreSQLへの接続文字列 (例: `postgresql://user:password@host:port/database`)。
    *   `ADMIN_USER_ID`: ボット所有者のDiscordユーザーID (`is_owner()`権限に重要)。
    *   `PROXY_BOT_ID` (任意): Shiromiがコマンドを受け付けるプロキシボットのID (例: Mizuki)。
    *   `BOT_NAME`: 一部のメッセージで表示されるボット名。
    *   `COMMAND_PREFIX`: コマンドプレフィックス (例: `Shi`)。
    *   `EXCLUDED_CATEGORY_IDS` (任意): スキャンから除外するカテゴリIDのコンマ区切りリスト。
    *   `FINAL_STICKER_ID`, `INTERMEDIATE_STICKER_ID`, `LEAST_STICKER_ID`, `MOST_STICKER_ID` (任意): レポートの様々な段階で送信されるスタンプのID。
    *   `WEBSITE_BASE_URL` (任意): スキャンデータを表示するウェブサイトのベースURL (もしあれば)。
    *   `REPORT_CHANNEL_ID` (任意): 公開埋め込みレポートを送信するDiscordチャンネルID。設定しない場合、コマンドが呼び出された元のチャンネルに送信されます。
    *   `FINAL_DM_EMOJI` (任意): 各パーソナライズDMの最後に送信される絵文字。
    *   `TRACKED_ROLE_GRANT_IDS` (任意): 監査ログ経由で付与を追跡するロールID。
    *   `DM_REPORT_RECIPIENT_ROLE_ID` (任意): DMレポートを受信するメンバーが持つロールのID。
    *   `BOOSTER_THANKYOU_ROLE_IDS` (任意): DMで特別な感謝メッセージとパーソナライズ画像を送信するロールID (ブースター、貢献者)。
    *   `ADMIN_ROLE_IDS_FILTER` (任意): 一部のリーダーボードから除外する他の管理者ロールID (サーバーの管理者権限以外)。
    *   `REACTION_UNICODE_EXCEPTIONS` (任意): リアクションリーダーボードで許可されるUnicode絵文字のリスト (サーバー絵文字以外)。
    *   `ENABLE_REACTION_SCAN` (任意): リアクションスキャンを有効にする場合は `true` に設定 (スキャンが遅くなる可能性あり)。
    *   `MAX_CONCURRENT_CHANNEL_SCANS` (任意): 同時にスキャンするチャンネル/スレッドの最大数 (デフォルトは5)。

3.  **パーソナライズDM画像の構成 (任意):**
    *   `quy_toc_anh.json` ファイルを編集します。
    *   `BOOSTER_THANKYOU_ROLE_IDS` を持ち、DMでカスタム画像を受信させたいユーザーに対して、`"USER_ID": "IMAGE_URL"` のペアを追加します。

## ボットの実行

インストールと設定後:
```bash
python bot.py
```
ボットはDiscordに接続し、コマンドを受け付ける準備ができます。

## コマンドの使用方法

主なコマンドは、設定されたプレフィックス (例: `Shi`) を使用して呼び出します。

*   **テストモード (管理者にDMを送信):**
    *   `[prefix]romi [export_csv=True/False] [export_json=True/False] [keywords=キーワード1,キーワード2]`
    *   例: `Shi romi export_csv=True keywords=こんにちは,さようなら`
    *   デフォルト: `export_csv=False`, `export_json=False`, キーワードなし。
    *   パーソナライズDMレポートは、設定された `ADMIN_USER_ID` に送信されます。
*   **通常モード (設定されたロールにDMを送信):** （まだ利用できないです）
    *   `[prefix]Shiromi [export_csv=True/False] [export_json=True/False] [keywords=キーワード1,キーワード2]`
    *   例: `Shi Shiromi export_json=True`
    *   パーソナライズDMレポートは、`DM_REPORT_RECIPIENT_ROLE_ID` を持つユーザーに送信されます。
*   **ボットチェック:**
    *   `[prefix]ping_shiromi`
    *   ボットが応答するかどうかを確認し、遅延を表示します。

**PROXY_BOT_IDに関する注意:**
`PROXY_BOT_ID` が設定されている場合、そのボットはShiromiの `COMMAND_PREFIX` なしで、コマンド名で始まるメッセージを送信することでShiromiのコマンドを呼び出すことができます。例えば、プロキシボットが `romi export_csv=True` と送信すると、Shiromiはそれを理解して実行します。

## フォルダ構造

```
Shiromi/
├── .git/
├── __pycache__/
├── bot_core/
│   ├── __init__.py
│   ├── events.py
│   ├── setup.py
├── cogs/
│   ├── deep_scan_helpers/
│   │   ├── __init__.py
│   │   ├── data_processing.py
│   │   ├── dm_sender.py
│   │   ├── export_generation.py
│   │   ├── finalization.py
│   │   ├── init_scan.py
│   │   ├── report_generation.py
│   │   ├── scan_channels.py
│   ├── __init__.py
│   ├── deep_scan_cog.py
├── moitruongao/ (Python仮想環境、例: venv)
├── reporting/
│   ├── __init__.py
│   ├── csv_writer.py
│   ├── embeds_analysis.py
│   ├── embeds_dm.py
│   ├── embeds_guild.py
│   ├── embeds_items.py
│   ├── embeds_user.py
│   ├── json_writer.py
├── .env                    #ここにおいて下さし 環境変数 (重要、機密)asda
├── .env_example.md         # .env のサンプルファイル
├── .gitignore
├── bot.py                  # メインボット実行ファイルdsd
├── config.py               # 設定の読み込みと管理
├── database.py             # PostgreSQLデータベースとの対話
├── discord_logging.py      # Discordスレッドへのログ送信
├── quy_toc_anh.json        # パーソナライズDM画像の対応表
├── README.md               # このファイル
├── requirements.txt        # Pythonライブラリの依存関係
├── scanner.py              # (空 - 将来の機能用かも)
└── utils.py                # 一般的なユーティリティ関数
```

## 重要な注意点

*   **ボット権限:** Shiromiが完全に機能するには、広範なDiscord権限が必要です（Guild Members、Message Contentなどの特権インテント、およびView Audit Log、Manage Server、Read Message History、Create Public Threads、Embed Links、Attach Filesなどのサーバー権限を含む）。Developer Portalおよびサーバー内でボットが十分な権限を持っていることを確認してください。
*   **データベース:** PostgreSQL接続とテーブル設定は必須です。データベースなしではボットは動作しません。
*   **リソース:** 詳細スキャンは、特に大規模サーバーやリアクションスキャンが有効な場合、時間とリソース（CPU、RAM、Discord APIレート制限）を消費する可能性があります。
*   **APIレート制限:** ボットはDiscordのレート制限を処理しようとしますが、非常に大規模なサーバーではスキャンが中断される可能性があります。
*   **PROXY_BOT_IDのセキュリティ:** 使用する場合、信頼できるプロキシボットのみがそのIDを持つようにしてください。Shiromiの強力なコマンドを実行できるためです。

</details>

## Image 1 (Server):

![image](https://github.com/user-attachments/assets/b402929b-5043-4991-999d-4b4daefd9991)

## Image 2 (Server):

![image](https://github.com/user-attachments/assets/1a150747-062b-491f-a363-bcc10f3af86d)

## Image 3 (DM:

![image](https://github.com/user-attachments/assets/e648e102-23ec-428f-a4f3-8bd193d17b8f)
![image](https://github.com/user-attachments/assets/f2661310-7422-41f2-ac02-ccdd787aa4ac)


