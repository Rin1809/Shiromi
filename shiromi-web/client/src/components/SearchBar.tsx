// --- START OF FILE website/client/src/components/SearchBar.tsx ---
import React from 'react';
import './styles/SearchBar.css';

interface SearchBarProps {
  value: string;
  onChange: (event: React.ChangeEvent<HTMLInputElement>) => void;
  onSearch: () => void;
  isLoading: boolean;
}

const SearchBar: React.FC<SearchBarProps> = ({ value, onChange, onSearch, isLoading }) => {

  const handleKeyPress = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter' && !isLoading && value.length >= 2) { // Thêm kiểm tra length
      onSearch();
    }
  };

  return (
    <div className="search-bar-container">
      <input
        type="text"
        placeholder="Nhập tên hoặc ID ai đó muốn check..."
        value={value}
        onChange={onChange}
        onKeyPress={handleKeyPress}
        disabled={isLoading}
        aria-label="Search user input" // Cập nhật aria-label
      />
      {/* Thay nút text bằng nút icon kính lúp */}
      <button
        className="search-button" // Thêm class để style riêng
        onClick={onSearch}
        disabled={isLoading || value.length < 2}
        aria-label="Search" // Aria-label cho nút icon
      >
        {/* SVG Kính lúp */}
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" width="20" height="20">
          <path d="M15.5 14h-.79l-.28-.27A6.471 6.471 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/>
        </svg>
      </button>
    </div>
  );
};

export default SearchBar;
// --- END OF FILE website/client/src/components/SearchBar.tsx ---