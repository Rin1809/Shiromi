// --- START OF FILE website/server/src/db.js ---
const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Railway có thể yêu cầu SSL
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

pool.on('connect', () => {
  console.log('Database pool connected');
});

pool.on('error', (err, client) => {
  console.error('Unexpected error on idle database client', err);
  // process.exit(-1); // Cân nhắc thoát nếu lỗi DB nghiêm trọng
});

/**
 * Thực thi một câu lệnh SQL.
 * @param {string} text Câu lệnh SQL với placeholders ($1, $2, ...)
 * @param {Array} params Mảng các giá trị cho placeholders
 * @returns {Promise<QueryResult>} Kết quả từ pg
 */
const query = async (text, params) => {
  const start = Date.now();
  try {
    const res = await pool.query(text, params);
    const duration = Date.now() - start;
    console.log('executed query', { text: text.substring(0, 100) + (text.length > 100 ? '...' : ''), duration: `${duration}ms`, rows: res.rowCount });
    return res;
  } catch (error) {
    console.error('Error executing query', { text: text.substring(0, 100) + (text.length > 100 ? '...' : ''), params });
    throw error; // Ném lỗi ra để API xử lý
  }
};

module.exports = {
  query,
  pool // Export pool nếu cần truy cập trực tiếp (ít khi)
};
// --- END OF FILE website/server/src/db.js ---