// --- START OF FILE website/client/src/main.tsx ---
import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom';
import App from './App.tsx'
// SỬA ĐƯỜNG DẪN IMPORT CSS Ở ĐÂY
import './components/styles/index.css' // Đường dẫn đúng

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
)
// --- END OF FILE website/client/src/main.tsx ---
