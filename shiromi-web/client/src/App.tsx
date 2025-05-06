// --- START OF FILE website/client/src/App.tsx ---
import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { Routes, Route, useParams } from 'react-router-dom';
import SearchBar from './components/SearchBar';
import UserInfoDisplay from './components/UserInfoDisplay';
import UserSuggestionList from './components/UserSuggestionList';
import './components/styles/App.css';

// Interface UserScanResult
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

type IntroStage = 'cat' | 'serverName' | 'search';

const ArrowDownIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" width="24" height="24">
    <path d="M7.41 8.59L12 13.17l4.59-4.58L18 10l-6 6-6-6 1.41-1.41z"/>
  </svg>
);
const ArrowUpIcon = () => (
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" width="24" height="24">
      <path d="M7.41 15.41L12 10.83l4.59 4.58L18 14l-6-6-6 6 1.41 1.41z"/>
    </svg>
  );

function ScanPage() {
  const [searchTerm, setSearchTerm] = useState('');
  const [searchResults, setSearchResults] = useState<UserScanResult[]>([]);
  const [isLoading, setIsLoading] = useState(false); // Loading này chỉ cho tìm kiếm cụ thể
  const [isLoadingAllUsers, setIsLoadingAllUsers] = useState(false); // Loading riêng cho fetch all users
  const [error, setError] = useState<string | null>(null);
  const [scanId, setScanId] = useState<string | null>(null);
  const { guildId } = useParams<{ guildId: string }>();
  const [guildName, setGuildName] = useState<string | null>(null);
  const [introStage, setIntroStage] = useState<IntroStage>('cat');
  const [isFadingOut, setIsFadingOut] = useState(false);
  const [hasDisplayedResults, setHasDisplayedResults] = useState(false); 
  
  const [allUsersList, setAllUsersList] = useState<UserScanResult[]>([]);
  const [isShowingAllUsersList, setIsShowingAllUsersList] = useState(false);

  useEffect(() => {
    if (!guildId) {
      setError('Lỗi: Không thể xác định ID Guild từ đường dẫn URL.');
      setIntroStage('search');
      return;
    } else {
      setError(null);
    }
    setGuildName("Hôm qua ᓚᘏᗢ | きのう");
    setIntroStage('cat');
    setIsFadingOut(false);
    setHasDisplayedResults(false);
    setSearchResults([]);
    setAllUsersList([]);
    setSearchTerm('');
    setScanId(null);
    setIsShowingAllUsersList(false);
    
    let stage1Timer: number | null = null; 
    let stage2Timer: number | null = null; 
    let fade1Timer: number | null = null;
    let fade2Timer: number | null = null; 

    const catDisplayTime = 1800;
    const serverNameDisplayTime = 2000;
    const fadeDuration = 500;

    stage1Timer = setTimeout(() => { setIsFadingOut(true); }, catDisplayTime);
    fade1Timer = setTimeout(() => { setIntroStage('serverName'); setIsFadingOut(false); }, catDisplayTime + fadeDuration);
    stage2Timer = setTimeout(() => { setIsFadingOut(true); }, catDisplayTime + fadeDuration + serverNameDisplayTime);
    fade2Timer = setTimeout(() => { setIntroStage('search'); setIsFadingOut(false); }, catDisplayTime + fadeDuration + serverNameDisplayTime + fadeDuration);

    return () => {
      if (stage1Timer) clearTimeout(stage1Timer);
      if (stage2Timer) clearTimeout(stage2Timer);
      if (fade1Timer) clearTimeout(fade1Timer);
      if (fade2Timer) clearTimeout(fade2Timer);
    }
  }, [guildId]);

  const performFetch = useCallback(async (isSearchAll = false) => {
    if (!guildId) {
      setError('Lỗi: Không thể xác định ID Guild.');
      return;
    }
    if (!isSearchAll && searchTerm.trim().length < 2) {
      setError('Vui lòng nhập ít nhất 2 ký tự để bắt đầu tìm kiếm.');
      setSearchResults([]); 
      setHasDisplayedResults(false);
      return;
    }

    if (isSearchAll) {
        setIsLoadingAllUsers(true); // Sử dụng state loading riêng
    } else {
        setIsLoading(true); // Loading cho tìm kiếm cụ thể
    }
    setError(null);
    
    if (!isSearchAll) {
        setSearchResults([]);
    }

    try {
      let apiUrl = `/api/scan/${guildId}/user?`;
      if (isSearchAll) {
        apiUrl += `showall=true`;
      } else {
        apiUrl += `search=${encodeURIComponent(searchTerm.trim())}`;
      }

      const response = await fetch(apiUrl);
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: `Lỗi HTTP ${response.status}` }));
        throw new Error(errorData.error || `Lỗi ${response.status}`);
      }
      const data = await response.json();
      setScanId(data.scan_id || null);

      if (isSearchAll) {
        setAllUsersList(data.users || []);
        if (!data.users || data.users.length === 0) {
          setError('Không tìm thấy người dùng nào (không phải bot) trong lần quét này.');
        }
      } else {
        setSearchResults(data.users || []);
        if (!data.users || data.users.length === 0) {
          setError(`Không tìm thấy người dùng nào khớp với '${searchTerm}'.`);
          setHasDisplayedResults(false);
        } else {
          setHasDisplayedResults(true); 
        }
      }
    } catch (err: any) {
      console.error("API Error:", err);
      setError(err.message || 'Đã xảy ra lỗi không mong muốn.');
      if (isSearchAll) setAllUsersList([]); else setSearchResults([]);
      if (!isSearchAll) setHasDisplayedResults(false); 
    } finally {
        if (isSearchAll) {
            setIsLoadingAllUsers(false);
        } else {
            setIsLoading(false);
        }
    }
  }, [guildId, searchTerm]);


  const handleSearchClick = () => {
    setIsShowingAllUsersList(false); 
    setAllUsersList([]);
    setHasDisplayedResults(false); 
    performFetch(false);
  };

  const handleToggleShowAllClick = async () => {
    if (isShowingAllUsersList) {
      setIsShowingAllUsersList(false);
      // Không cần xóa allUsersList ngay, để nếu người dùng mở lại thì có sẵn
    } else {
      setSearchTerm(''); 
      setSearchResults([]); 
      setHasDisplayedResults(false); 
      
      // Chỉ fetch nếu allUsersList rỗng
      if (allUsersList.length === 0) {
        await performFetch(true); 
      }
      setIsShowingAllUsersList(true);
    }
  };
  
  const handleUserSuggestionSelect = (userNameOrId: string) => {
    setSearchTerm(userNameOrId);
    setIsShowingAllUsersList(false); 
    // Không cần xóa allUsersList
    setHasDisplayedResults(false); 
    
    setTimeout(() => {
        const searchInput = document.querySelector('.search-bar-container input') as HTMLInputElement;
        if (searchInput) searchInput.focus();
        performFetch(false); 
    }, 0);
  };

  const handleSearchInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const newSearchTerm = event.target.value;
    setSearchTerm(newSearchTerm);
    if (isShowingAllUsersList && newSearchTerm.trim() !== '') {
      setIsShowingAllUsersList(false); 
      // Không cần xóa allUsersList
    }
     if (newSearchTerm.trim() === '' && searchResults.length > 0) {
        setSearchResults([]); 
        setHasDisplayedResults(false); 
    }
  };

   const formatRelativeTime = (isoString?: string): string => {
    if (!isoString) return 'N/A';
    try {
      const date = new Date(isoString);
      const now = new Date();
      const diffSeconds = Math.round((now.getTime() - date.getTime()) / 1000);
      const diffMinutes = Math.round(diffSeconds / 60);
      const diffHours = Math.round(diffMinutes / 60);
      const diffDays = Math.round(diffHours / 24);

      if (diffSeconds < 60) return `${diffSeconds} giây trước`;
      if (diffMinutes < 60) return `${diffMinutes} phút trước`;
      if (diffHours < 24) return `${diffHours} giờ trước`;
      return `${diffDays} ngày trước`;
    } catch {
      return 'Ngày không hợp lệ';
    }
  };

  const formatTimeDelta = (seconds?: number): string => {
    if (seconds === undefined || seconds === null || seconds <= 0) return 'N/A';
    const d = Math.floor(seconds / (3600 * 24));
    const h = Math.floor(seconds % (3600 * 24) / 3600);
    const m = Math.floor(seconds % 3600 / 60);
    const s = Math.floor(seconds % 60);
    let result = '';
    if (d > 0) result += `${d} ngày `;
    if (h > 0) result += `${h} giờ `;
    if (m > 0) result += `${m} phút `;
    if (s > 0 || result === '') result += `${s} giây`;
    return result.trim() || 'N/A';
  };

  const appContainerClass = useMemo(() => {
    if (introStage !== 'search') return "AppContainer intro-active";
    if (hasDisplayedResults && searchResults.length > 0) {
        return "AppContainer search-top";
    }
    return "AppContainer search-centered";
  }, [introStage, hasDisplayedResults, searchResults.length]);

  return (
    <div className={appContainerClass}>
      {introStage === 'cat' && (
        <div className={`intro-stage cat-stage ${isFadingOut ? 'hiding' : 'visible'}`}>
          <span className="cat-icon">ᓚᘏᗢ</span>
          <span className="ellipsis">...</span>
        </div>
      )}

      {introStage === 'serverName' && (
        <div className={`intro-stage server-stage-wow ${isFadingOut ? 'hiding' : 'visible'}`}>
            <div className="avatar-container-wow">
                <img
                    src="https://cdn.discordapp.com/icons/1259368902937280593/81ce19857ca473711292dfa495e3c90d.webp?size=128&quality=lossless"
                    alt="Server Icon"
                    className="server-avatar-wow"
                />
            </div>
             <div className="text-container-wow">
                <h2 className="server-name-wow">{guildName || `Server ${guildId}`}</h2>
             </div>
        </div>
      )}

      <div className={`main-content ${introStage === 'search' ? 'visible' : ''}`}>
          <div className="search-interaction-area">
            <SearchBar
              value={searchTerm}
              onChange={handleSearchInputChange}
              onSearch={handleSearchClick}
              isLoading={isLoading} // Chỉ dùng isLoading cho SearchBar
            />
            
            {introStage === 'search' && ( 
              <button
                onClick={handleToggleShowAllClick}
                className={`show-all-toggle-button ${isShowingAllUsersList ? 'active' : ''}`}
                disabled={isLoading || isLoadingAllUsers} // Disable nếu có loading bất kỳ
                title={isShowingAllUsersList ? "Ẩn danh sách" : "Hiển thị tất cả người dùng (không phải bot)"}
              >
                {isShowingAllUsersList ? <ArrowUpIcon /> : <ArrowDownIcon />}
              </button>
            )}

            {isLoadingAllUsers && isShowingAllUsersList && (
              <p className="loading-users-list">Đang tải danh sách người dùng...</p>
            )}
            {isShowingAllUsersList && allUsersList.length > 0 && !isLoadingAllUsers && (
              <UserSuggestionList
                users={allUsersList}
                onUserSelect={handleUserSuggestionSelect}
              />
            )}
          </div>
          
          <div className="results-display-area">
            {isLoading && <p className="loading">Đang tìm kiếm...</p>} {/* Chỉ hiển thị khi tìm kiếm cụ thể */}
            {error && <p className="error">{error}</p>}
            
            {scanId && searchResults.length > 0 && ( 
              <p className="scan-info">
                ID trích từ Database là: {scanId}
              </p>
            )}
            <div className="results-container">
              {searchResults.map((user, index) => (
                <UserInfoDisplay
                  key={user.user_id}
                  user={user}
                  formatRelativeTime={formatRelativeTime}
                  formatTimeDelta={formatTimeDelta}
                  style={{ animationDelay: `${index * 0.05}s` }}
                />
              ))}
            </div>
          </div>
      </div>
    </div>
  );
}


function App() {
  return (
    <div className="App">
      <Routes>
        <Route path="/scan/:guildId" element={<ScanPage />} />
        <Route
          path="/"
          element={
            <div className="home-page">
              <h1>📊 Shiromi Scan</h1>
              <p>Đây là trang tra cứu kết quả quét của bot Shiromi.</p>
              <p>Vui lòng truy cập đường dẫn dạng <strong>/scan/{'<ID_SERVER>'}</strong> được cung cấp bởi bot trong Discord.</p>
              <p>Ví dụ: <code>/scan/123456789012345678</code></p>
            </div>
          }
        />
        <Route
          path="*"
          element={
            <div className="not-found-page">
              <h2>404 - Không Tìm Thấy Trang</h2>
              <p>Xin lỗi, trang bạn tìm kiếm không tồn tại.</p>
            </div>
          }
        />
      </Routes>
      <footer>
           ᓚᘏᗢ Rin 
      </footer>
    </div>
  );
}

export default App;
// --- END OF FILE website/client/src/App.tsx ---