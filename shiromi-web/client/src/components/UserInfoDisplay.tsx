// --- START OF FILE website/client/src/components/UserInfoDisplay.tsx ---
import React from 'react';
import './styles/UserInfoDisplay.css'; // Đường dẫn đúng

// Định nghĩa kiểu dữ liệu cho user (có thể import từ App.tsx nếu bạn tách ra)
interface UserScanResult {
  user_id: string;
  display_name_at_scan: string;
  is_bot: boolean;
  message_count?: number;
  reaction_received_count?: number;
  reaction_given_count?: number;
  reply_count?: number;
  mention_given_count?: number;
  mention_received_count?: number;
  link_count?: number;
  image_count?: number;
  sticker_count?: number;
  other_file_count?: number;
  distinct_channels_count?: number;
  first_seen_utc?: string; // Dạng ISO string
  last_seen_utc?: string;  // Dạng ISO string
  activity_span_seconds?: number;
  ranking_data?: Record<string, number>; // VD: {"messages": 1, "replies": 5}
  achievement_data?: Record<string, any>; // VD: {"top_emoji": {"id": 123, "count": 10}}
}

// Định nghĩa kiểu props cho component này
interface UserInfoDisplayProps {
  user: UserScanResult;
  formatRelativeTime: (isoString?: string) => string;
  formatTimeDelta: (seconds?: number) => string;
  style?: React.CSSProperties; // Thêm prop style để nhận animation delay
}

// Helper function để hiển thị rank (có thể đưa ra file utils nếu dùng nhiều nơi)
const renderRank = (rank?: number): string => {
  return rank ? ` (Hạng #${rank})` : '';
};

// Component UserInfoDisplay
const UserInfoDisplay: React.FC<UserInfoDisplayProps> = ({ user, formatRelativeTime, formatTimeDelta, style }) => {
  return (
    // Thêm class "appear" để kích hoạt animation và nhận style từ props
    <div className="user-info-card appear" style={style}>
      {/* Tên và ID user */}
      <h2>{user.display_name_at_scan} ({user.user_id}) {user.is_bot ? '🤖' : ''}</h2>

      {/* Phần thông tin về Tin nhắn & Nội dung */}
      <div className="info-section">
        <h3>📜 Tin Nhắn & Nội Dung</h3>
        <p>Tổng tin nhắn: <strong>{user.message_count?.toLocaleString() ?? '0'}</strong>{renderRank(user.ranking_data?.messages)}</p>
        <p>Links đã gửi: {user.link_count?.toLocaleString() ?? '0'}</p>
        <p>Ảnh đã gửi: {user.image_count?.toLocaleString() ?? '0'}</p>
        {/* Có thể thêm dòng hiển thị Custom Emoji Count nếu cần */}
        {/* <p>Emoji Server (Nội dung): {user.achievement_data?.top_content_emoji?.count?.toLocaleString() ?? user.ranking_data?.custom_emoji_content ?? '0'}</p> */}
        <p>Stickers đã gửi: {user.sticker_count?.toLocaleString() ?? '0'}{renderRank(user.ranking_data?.stickers_sent)}</p>
        <p>Files khác: {user.other_file_count?.toLocaleString() ?? '0'}</p>
      </div>

      {/* Phần thông tin về Tương tác */}
      <div className="info-section">
        <h3>💬 Tương Tác</h3>
        <p>Trả lời đã gửi: {user.reply_count?.toLocaleString() ?? '0'}{renderRank(user.ranking_data?.replies)}</p>
        <p>Mentions đã gửi: {user.mention_given_count?.toLocaleString() ?? '0'}{renderRank(user.ranking_data?.mention_given)}</p>
        <p>Mentions nhận: {user.mention_received_count?.toLocaleString() ?? '0'}{renderRank(user.ranking_data?.mention_received)}</p>
        {/* Chỉ hiển thị reaction nếu có dữ liệu */}
        {user.reaction_received_count !== undefined && <p>Reactions nhận (lọc): {user.reaction_received_count?.toLocaleString() ?? '0'}{renderRank(user.ranking_data?.reaction_received)}</p>}
        {user.reaction_given_count !== undefined && <p>Reactions đã thả (lọc): {user.reaction_given_count?.toLocaleString() ?? '0'}{renderRank(user.ranking_data?.reaction_given)}</p>}
      </div>

      {/* Phần thông tin về Hoạt động */}
      <div className="info-section">
          <h3>🎯 Hoạt Động</h3>
          <p>Số kênh/luồng khác nhau: <strong>{user.distinct_channels_count ?? '0'}</strong>{renderRank(user.ranking_data?.distinct_channels)}</p>
          {/* TODO: Có thể thêm hiển thị top channel hoạt động của user này nếu có trong achievement_data */}
      </div>

      {/* Phần thông tin về Thời gian */}
       <div className="info-section">
        <h3>⏳ Thời Gian Hoạt Động</h3>
        <p>Hoạt động đầu tiên: {formatRelativeTime(user.first_seen_utc)}</p>
        <p>Hoạt động cuối cùng: {formatRelativeTime(user.last_seen_utc)}</p>
        <p>Khoảng TG hoạt động: <strong>{formatTimeDelta(user.activity_span_seconds)}</strong>{renderRank(user.ranking_data?.activity_span)}</p>
      </div>

      {/* Phần hiển thị thành tích/rank */}
      {user.ranking_data && Object.keys(user.ranking_data).length > 0 && (
          <div className="info-section">
              <h3>🏆 Thành Tích Nổi Bật (Top)</h3>
              <ul>
                  {/* Lọc và hiển thị các rank cụ thể */}
                  {Object.entries(user.ranking_data)
                    // Chọn các key rank muốn hiển thị ở đây
                    .filter(([key]) => ['messages', 'replies', 'reaction_received', 'reaction_given', 'distinct_channels', 'oldest_members', 'activity_span', 'booster_duration'].includes(key))
                    .map(([key, rank]) => (
                      // Format tên hiển thị cho từng loại rank
                      <li key={key}>{key.replace('_', ' ').replace('received','nhận').replace('given','đã thả').replace('messages','Tin nhắn').replace('replies','Trả lời').replace('reaction','Reaction').replace('channels','kênh').replace('distinct','khác nhau').replace('oldest members','Lâu năm').replace('activity span','TG Hoạt động').replace('booster duration','Booster')} : <strong>Hạng #{rank}</strong></li>
                  ))}
                  {/* Hiển thị rank cho các role được theo dõi */}
                  {Object.entries(user.ranking_data)
                      .filter(([key]) => key.startsWith('tracked_role_'))
                      .map(([key, rank]) => {
                          const roleId = key.replace('tracked_role_', '');
                          // TODO: Cần lấy tên role từ roleId. Có thể cần API khác hoặc lưu tên role trong DB.
                          // Hiện tại chỉ hiển thị ID.
                          return <li key={key}>Nhận Role {roleId}: <strong>Hạng #{rank}</strong></li>;
                  })}
              </ul>
          </div>
      )}

      {/* TODO: Thêm phần hiển thị achievement_data (vd: top emoji, top sticker) nếu có */}
      {/* Ví dụ:
      {user.achievement_data?.top_content_emoji && (
        <p>Top Emoji: ID {user.achievement_data.top_content_emoji.id} ({user.achievement_data.top_content_emoji.count} lần)</p>
      )}
      */}

    </div>
  );
};

export default UserInfoDisplay;
// --- END OF FILE website/client/src/components/UserInfoDisplay.tsx ---