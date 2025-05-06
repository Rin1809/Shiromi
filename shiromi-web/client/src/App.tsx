// --- START OF FILE website/client/src/App.tsx ---
import { useState, useEffect, useCallback, useMemo } from 'react';
import { Routes, Route, useParams, useNavigate } from 'react-router-dom';
import SearchBar from './components/SearchBar';
import UserInfoDisplay from './components/UserInfoDisplay';
import './components/styles/App.css';

// Interface UserScanResult (Giữ nguyên)
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
  first_seen_utc?: string;
  last_seen_utc?: string;
  activity_span_seconds?: number;
  ranking_data?: Record<string, number>;
  achievement_data?: Record<string, any>;
}

type IntroStage = 'cat' | 'serverName' | 'search';

function ScanPage() {
  const [searchTerm, setSearchTerm] = useState('');
  const [searchResults, setSearchResults] = useState<UserScanResult[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scanId, setScanId] = useState<string | null>(null);
  const { guildId } = useParams<{ guildId: string }>();
  const navigate = useNavigate(); // Giữ lại navigate
  const [guildName, setGuildName] = useState<string | null>(null);
  const [introStage, setIntroStage] = useState<IntroStage>('cat');
  const [isFadingOut, setIsFadingOut] = useState(false);
  const [hasSearched, setHasSearched] = useState(false);

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
    setHasSearched(false);
    setSearchResults([]);
    setSearchTerm('');
    setScanId(null);

    let stage1Timer: NodeJS.Timeout | null = null;
    let stage2Timer: NodeJS.Timeout | null = null;
    let fade1Timer: NodeJS.Timeout | null = null;
    let fade2Timer: NodeJS.Timeout | null = null;

    const catDisplayTime = 1800;
    const serverNameDisplayTime = 2000; // Có thể cần tăng thêm chút nếu animation phức tạp hơn
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

  const handleSearch = useCallback(async () => {
    if (!guildId || searchTerm.length < 2) {
      setError('Vui lòng nhập ít nhất 2 ký tự để bắt đầu tìm kiếm.');
      setSearchResults([]);
      return;
    }
    setIsLoading(true);
    setError(null);
    setSearchResults([]);
    setHasSearched(true);
    try {
      const response = await fetch(`/api/scan/${guildId}/user?search=${encodeURIComponent(searchTerm)}`);
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: `Lỗi HTTP ${response.status}` }));
        throw new Error(errorData.error || `Lỗi ${response.status}`);
      }
      const data = await response.json();
      setSearchResults(data.users || []);
      setScanId(data.scan_id || null);
      if (!data.users || data.users.length === 0) {
        setError(`Không tìm thấy người dùng nào khớp với '${searchTerm}'.`);
      }
    } catch (err: any) {
      console.error("API Search Error:", err);
      setError(err.message || 'Đã xảy ra lỗi không mong muốn khi tìm kiếm.');
      setSearchResults([]);
    } finally {
      setIsLoading(false);
    }
  }, [guildId, searchTerm]);

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
    if (hasSearched) return "AppContainer search-top";
    return "AppContainer search-centered";
  }, [introStage, hasSearched]);

  return (
    <div className={appContainerClass}>
      {introStage === 'cat' && (
        <div className={`intro-stage cat-stage ${isFadingOut ? 'hiding' : 'visible'}`}>
          <span className="cat-icon">ᓚᘏᗢ</span>
          <span className="ellipsis">...</span>
        </div>
      )}

      {introStage === 'serverName' && (
         // Container này sẽ style khác đi trong CSS
        <div className={`intro-stage server-stage-wow ${isFadingOut ? 'hiding' : 'visible'}`}>
            {/* Đặt avatar và text vào các div riêng để dễ điều khiển animation */}
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
          <SearchBar
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            onSearch={handleSearch}
            isLoading={isLoading}
          />
          {isLoading && <p className="loading">Đang tìm kiếm...</p>}
          {error && <p className="error">{error}</p>}
          {scanId && searchResults.length > 0 && (
            <p className="scan-info">Hiển thị kết quả từ lần quét ID: {scanId}</p>
          )}
          <div className="results-container">
            {searchResults.map((user, index) => (
              <UserInfoDisplay
                key={user.user_id}
                user={user}
                formatRelativeTime={formatRelativeTime}
                formatTimeDelta={formatTimeDelta}
                style={{ animationDelay: `${index * 0.1}s` }}
              />
            ))}
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
          Author: Rin 🥰
      </footer>
    </div>
  );
}

export default App;
// --- END OF FILE website/client/src/App.tsx ---