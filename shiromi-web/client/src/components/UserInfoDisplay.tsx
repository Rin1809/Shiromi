// --- START OF FILE website/client/src/components/UserInfoDisplay.tsx ---
import React from 'react';
import './styles/UserInfoDisplay.css';

interface UserScanResult {
  user_id: string;
  display_name_at_scan: string;
  avatar_url_at_scan?: string | null; 
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
  first_seen_utc?: string;
  last_seen_utc?: string;
  activity_span_seconds?: number;
  ranking_data?: Record<string, number>;
  achievement_data?: Record<string, any>;
}

interface UserInfoDisplayProps {
  user: UserScanResult;
  formatRelativeTime: (isoString?: string) => string;
  formatTimeDelta: (seconds?: number) => string;
  style?: React.CSSProperties;
}

const renderRank = (rank?: number): React.ReactNode => {
  return rank ? <span className="rank-indicator">(Hạng #{rank})</span> : null;
};

const UserInfoDisplay: React.FC<UserInfoDisplayProps> = ({ user, formatRelativeTime, formatTimeDelta, style }) => {
  const rankMapping: Record<string, string> = {
    messages: "Tổng Tin nhắn",
    replies: "Trả lời",
    reaction_received: "Reaction Nhận",
    reaction_given: "Reaction Thả",
    distinct_channels: "Số Kênh Khác Nhau",
    oldest_members: "Thành Viên Lâu Năm",
    activity_span: "Thời Gian Hoạt Động",
    booster_duration: "Thời Gian Boost",
    stickers_sent: "Gửi Sticker",
    links_sent: "Gửi Link",
    images_sent: "Gửi Ảnh",
    custom_emoji_content: "Dùng Custom Emoji",
    threads_created: "Tạo Thread",
    mention_given: "Mention Đã Gửi",
    mention_received: "Mention Đã Nhận",
  };

  const getAvatarUrl = (user: UserScanResult): string => {
    if (user.avatar_url_at_scan) {
      return user.avatar_url_at_scan.replace(/\.webp(\?size=\d+)?$/, '.png?size=64');
    }
    // Tính toán avatar mặc định của Discord
    // Discriminator không còn, nên dựa vào user_id để chọn avatar
    const avatarIndex = (parseInt(user.user_id) >> 22) % 6; // Hoặc 5 nếu API cũ
    return `https://cdn.discordapp.com/embed/avatars/${avatarIndex}.png`;
  };


  return (
    <div className="user-info-card appear" style={style}>
      <div className="card-header">
        {/* SỬA ĐỔI ĐỂ HIỂN THỊ AVATAR */}
        <img
            src={getAvatarUrl(user)}
            alt={`${user.display_name_at_scan}'s avatar`}
            className="user-avatar" // Class mới để style
            onError={(e) => {
                // Fallback nếu URL lỗi, hoặc avatar_url_at_scan không có
                const target = e.target as HTMLImageElement;
                const avatarIndex = (parseInt(user.user_id) >> 22) % 6;
                target.src = `https://cdn.discordapp.com/embed/avatars/${avatarIndex}.png`;
            }}
        />
        <div className="user-name-id">
          <h2>{user.display_name_at_scan} {user.is_bot ? '🤖' : ''}</h2>
          <span className="user-id-tag">ID: {user.user_id}</span>
        </div>
      </div>

      <div className="card-body">
        <div className="info-grid">
          {/* Section 1: Tin Nhắn & Nội Dung */}
          <div className="info-section">
            <h3 className="section-title"><span role="img" aria-label="Messages">📜</span> Tin Nhắn & Nội Dung</h3>
            <p>Tổng tin nhắn: <strong>{user.message_count?.toLocaleString() ?? '0'}</strong>{renderRank(user.ranking_data?.messages)}</p>
            <p>Links đã gửi: <strong>{user.link_count?.toLocaleString() ?? '0'}</strong>{renderRank(user.ranking_data?.links_sent)}</p>
            <p>Ảnh đã gửi: <strong>{user.image_count?.toLocaleString() ?? '0'}</strong>{renderRank(user.ranking_data?.images_sent)}</p>
            <p>Stickers đã gửi: <strong>{user.sticker_count?.toLocaleString() ?? '0'}</strong>{renderRank(user.ranking_data?.stickers_sent)}</p>
            <p>Files khác: <strong>{user.other_file_count?.toLocaleString() ?? '0'}</strong></p>
            {/* Có thể thêm Custom Emoji Count nếu có trong ranking_data hoặc achievement_data */}
            {user.ranking_data?.custom_emoji_content &&
                <p>Emoji Server (Nội dung): <strong>{user.achievement_data?.top_content_emoji?.count?.toLocaleString() ?? 'N/A'}</strong>{renderRank(user.ranking_data?.custom_emoji_content)}</p>
            }
          </div>

          {/* Section 2: Tương Tác */}
          <div className="info-section">
            <h3 className="section-title"><span role="img" aria-label="Interactions">💬</span> Tương Tác</h3>
            <p>Trả lời đã gửi: <strong>{user.reply_count?.toLocaleString() ?? '0'}</strong>{renderRank(user.ranking_data?.replies)}</p>
            <p>Mentions đã gửi: <strong>{user.mention_given_count?.toLocaleString() ?? '0'}</strong>{renderRank(user.ranking_data?.mention_given)}</p>
            <p>Mentions nhận: <strong>{user.mention_received_count?.toLocaleString() ?? '0'}</strong>{renderRank(user.ranking_data?.mention_received)}</p>
            {user.reaction_received_count !== undefined &&
              <p>Reactions nhận (lọc): <strong>{user.reaction_received_count?.toLocaleString() ?? '0'}</strong>{renderRank(user.ranking_data?.reaction_received)}</p>}
            {user.reaction_given_count !== undefined &&
              <p>Reactions đã thả (lọc): <strong>{user.reaction_given_count?.toLocaleString() ?? '0'}</strong>{renderRank(user.ranking_data?.reaction_given)}</p>}
          </div>

          {/* Section 3: Hoạt Động */}
          <div className="info-section">
            <h3 className="section-title"><span role="img" aria-label="Activity Scope">🎯</span> Phạm Vi Hoạt Động</h3>
            <p>Số kênh/luồng khác nhau: <strong>{user.distinct_channels_count ?? '0'}</strong>{renderRank(user.ranking_data?.distinct_channels)}</p>
             {user.ranking_data?.threads_created &&
                <p>Số thread đã tạo: <strong>{user.achievement_data?.threads_created_count ?? '0'}</strong>{renderRank(user.ranking_data?.threads_created)}</p>
            }
          </div>

          {/* Section 4: Thời Gian */}
          <div className="info-section">
            <h3 className="section-title"><span role="img" aria-label="Activity Time">⏳</span> Thời Gian Hoạt Động</h3>
            <p>Hoạt động đầu tiên: <span className="time-value">{formatRelativeTime(user.first_seen_utc)}</span></p>
            <p>Hoạt động cuối cùng: <span className="time-value">{formatRelativeTime(user.last_seen_utc)}</span></p>
            <p>Khoảng TG hoạt động: <strong className="time-value">{formatTimeDelta(user.activity_span_seconds)}</strong>{renderRank(user.ranking_data?.activity_span)}</p>
          </div>
        </div>

        {/* Section 5: Thành Tích */}
        {user.ranking_data && Object.keys(user.ranking_data).length > 0 && (
          <div className="info-section achievements-section">
            <h3 className="section-title"><span role="img" aria-label="Achievements">🏆</span> Thành Tích Nổi Bật (Top Server)</h3>
            <ul className="achievement-list">
              {Object.entries(user.ranking_data)
                .filter(([key]) => rankMapping[key]) // Chỉ hiển thị các rank có trong mapping
                .map(([key, rank]) => (
                  <li key={key}>
                    <span className="achievement-name">{rankMapping[key] || key.replace('_', ' ')}:</span>
                    <strong className="achievement-rank">Hạng #{rank}</strong>
                  </li>
              ))}
               {Object.entries(user.ranking_data)
                  .filter(([key]) => key.startsWith('tracked_role_'))
                  .map(([key, rank]) => {
                      const roleId = key.replace('tracked_role_', '');
                      return (
                        <li key={key}>
                            <span className="achievement-name">Nhận Role <code>{roleId}</code>:</span>
                            <strong className="achievement-rank">Hạng #{rank}</strong>
                        </li>
                      );
              })}
            </ul>
          </div>
        )}
      </div>
    </div>
  );
};

export default UserInfoDisplay;
// --- END OF FILE website/client/src/components/UserInfoDisplay.tsx ---