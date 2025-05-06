// --- START OF FILE website/client/src/components/UserSuggestionItem.tsx ---
import React from 'react';
import './styles/UserSuggestions.css'; // Sẽ tạo file CSS này sau

interface UserSuggestion {
  user_id: string;
  display_name_at_scan: string;
  avatar_url_at_scan?: string | null;
}

interface UserSuggestionItemProps {
  user: UserSuggestion;
  onSelect: (userId: string, userName: string) => void; // Callback khi user được chọn
}

const UserSuggestionItem: React.FC<UserSuggestionItemProps> = ({ user, onSelect }) => {
  const getAvatarUrl = (suggestionUser: UserSuggestion): string => {
    if (suggestionUser.avatar_url_at_scan) {
      return suggestionUser.avatar_url_at_scan.replace(/\.webp(\?size=\d+)?$/, '.png?size=32'); // Kích thước nhỏ hơn cho gợi ý
    }
    const avatarIndex = (parseInt(suggestionUser.user_id) >> 22) % 6;
    return `https://cdn.discordapp.com/embed/avatars/${avatarIndex}.png`;
  };

  return (
    <div className="user-suggestion-item" onClick={() => onSelect(user.user_id, user.display_name_at_scan)}>
      <img
        src={getAvatarUrl(user)}
        alt={`${user.display_name_at_scan}'s avatar`}
        className="suggestion-avatar"
        onError={(e) => {
            const target = e.target as HTMLImageElement;
            const avatarIndex = (parseInt(user.user_id) >> 22) % 6;
            target.src = `https://cdn.discordapp.com/embed/avatars/${avatarIndex}.png`;
        }}
      />
      <span className="suggestion-name">{user.display_name_at_scan}</span>
    </div>
  );
};

export default UserSuggestionItem;
// --- END OF FILE website/client/src/components/UserSuggestionItem.tsx ---