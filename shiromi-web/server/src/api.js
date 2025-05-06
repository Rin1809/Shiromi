// --- START OF FILE website/server/src/api.js ---
const express = require('express');
const db = require('./db');

const router = express.Router();

// GET /api/scan/:guildId/user?search=... hoặc ?showall=true
router.get('/scan/:guildId/user', async (req, res) => {
  const { guildId } = req.params;
  const { search, showall } = req.query; // Thêm showall

  if (!guildId) {
    return res.status(400).json({ error: 'Missing guildId.' });
  }

  // Nếu không có showall=true và không có search hoặc search quá ngắn
  if (showall !== 'true' && (!search || search.length < 2)) {
    return res.status(400).json({ error: 'Search term must be at least 2 characters, or use showall=true.' });
  }

  let client; // Khai báo client ở ngoài try/catch
  try {
    client = await db.pool.connect(); // Lấy một kết nối từ pool

    // 1. Tìm scan_id mới nhất, hoàn thành, web_accessible
    const scanQuery = `
      SELECT scan_id FROM scans
      WHERE guild_id = $1 AND status = 'completed' AND website_accessible = TRUE
      ORDER BY end_time DESC NULLS LAST
      LIMIT 1;
    `;
    const scanResult = await client.query(scanQuery, [guildId]);

    if (scanResult.rows.length === 0) {
      return res.status(404).json({ error: 'No completed scan data found for this guild.' });
    }
    const latestScanId = scanResult.rows[0].scan_id;

    // 2. Tìm user theo search term (ID hoặc tên) hoặc lấy tất cả
    let userQuery = '';
    let queryParams = [];
    
    if (showall === 'true') {
        userQuery = `
            SELECT user_id, display_name_at_scan, avatar_url_at_scan, is_bot, message_count,
                   reaction_received_count, reaction_given_count, reply_count,
                   mention_given_count, mention_received_count, link_count, image_count,
                   sticker_count, other_file_count, distinct_channels_count,
                   first_seen_utc, last_seen_utc, activity_span_seconds,
                   ranking_data, achievement_data
            FROM user_scan_results
            WHERE scan_id = $1 AND is_bot = FALSE
            ORDER BY display_name_at_scan; -- Sắp xếp theo tên
        `;
        queryParams = [latestScanId];
    } else {
        const searchTermLower = search.toLowerCase();
        // Kiểm tra xem search có phải là ID số không
        const isNumericId = /^\d+$/.test(search);

        if (isNumericId) {
            // Tìm chính xác theo User ID
            userQuery = `
                SELECT user_id, display_name_at_scan, avatar_url_at_scan, is_bot, message_count,
                       reaction_received_count, reaction_given_count, reply_count,
                       mention_given_count, mention_received_count, link_count, image_count,
                       sticker_count, other_file_count, distinct_channels_count,
                       first_seen_utc, last_seen_utc, activity_span_seconds,
                       ranking_data, achievement_data
                FROM user_scan_results
                WHERE scan_id = $1 AND user_id = $2;
            `;
            queryParams = [latestScanId, search];
        } else {
            // Tìm gần đúng theo tên (không phân biệt hoa thường)
            userQuery = `
                SELECT user_id, display_name_at_scan, avatar_url_at_scan, is_bot, message_count,
                       reaction_received_count, reaction_given_count, reply_count,
                       mention_given_count, mention_received_count, link_count, image_count,
                       sticker_count, other_file_count, distinct_channels_count,
                       first_seen_utc, last_seen_utc, activity_span_seconds,
                       ranking_data, achievement_data
                FROM user_scan_results
                WHERE scan_id = $1 AND display_name_at_scan ILIKE $2
                ORDER BY display_name_at_scan -- Sắp xếp để kết quả nhất quán hơn
                LIMIT 10; -- Giới hạn kết quả tìm theo tên
            `;
            queryParams = [latestScanId, `%${searchTermLower}%`]; // Thêm % cho ILIKE
        }
    }
    
    const userResult = await client.query(userQuery, queryParams);

    if (userResult.rows.length === 0) {
      return res.status(404).json({ error: `No users found matching criteria.` });
    }

    // Format kết quả (chuyển BigInt thành string nếu cần)
    const formattedResults = userResult.rows.map(row => ({
      ...row,
      user_id: String(row.user_id), // Đảm bảo user_id là string
      // Chuyển đổi các timestamp thành ISO string nếu chưa phải
      first_seen_utc: row.first_seen_utc?.toISOString(),
      last_seen_utc: row.last_seen_utc?.toISOString(),
    }));

    res.json({
        scan_id: String(latestScanId), // Trả về scan_id để biết dữ liệu thuộc lần quét nào
        users: formattedResults,
        is_full_list: showall === 'true' // Thêm cờ để client biết đây là danh sách đầy đủ
    });

  } catch (err) {
    console.error('API Error:', err);
    res.status(500).json({ error: 'Internal server error.' });
  } finally {
    if (client) {
      client.release(); // Luôn trả kết nối về pool
    }
  }
});

module.exports = router;
// --- END OF FILE website/server/src/api.js ---