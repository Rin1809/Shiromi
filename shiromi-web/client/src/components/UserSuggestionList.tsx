// --- START OF FILE website/client/src/components/UserSuggestionList.tsx ---
import React from 'react';
import './styles/UserSuggestionList.css';

interface UserScanResult {
  user_id: string;
  display_name_at_scan: string;
  avatar_url_at_scan?: string | null;
  is_bot: boolean; 
}

interface UserSuggestionListProps {
  users: UserScanResult[];
  onUserSelect: (userNameOrId: string) => void;
}

const getAvatarUrlForSuggestion = (user: UserScanResult): string => {
    if (user.avatar_url_at_scan) {
      return user.avatar_url_at_scan.replace(/\.webp(\?size=\d+)?$/, '.png?size=32');
    }
    const avatarIndex = (parseInt(user.user_id) >> 22) % 6;
    return `https://cdn.discordapp.com/embed/avatars/${avatarIndex}.png`;
};

const UserSuggestionList: React.FC<UserSuggestionListProps> = ({ users, onUserSelect }) => {
  if (!users || users.length === 0) {
    return null;
  }

  return (
    <div className="user-suggestions-container">
      {users.map(user => (
        <div
          key={user.user_id}
          className="suggestion-item"
          onClick={() => onUserSelect(user.display_name_at_scan)} 
          onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') onUserSelect(user.display_name_at_scan);}}
          tabIndex={0} 
          role="button"
          aria-label={`Search for ${user.display_name_at_scan}`}
        >
          <img
            src={getAvatarUrlForSuggestion(user)}
            alt={`${user.display_name_at_scan}'s avatar`}
            className="suggestion-avatar"
          />
          <span className="suggestion-name">{user.display_name_at_scan}</span>
        </div>
      ))}
    </div>
  );
};

export default UserSuggestionList;
// --- END OF FILE website/client/src/components/UserSuggestionList.tsx ---