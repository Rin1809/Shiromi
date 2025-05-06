// --- START OF FILE website/server/src/server.js ---
const express = require('express');
const cors = require('cors');
const path = require('path');
const apiRoutes = require('./api');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3001; // Railway cung cấp PORT

// Middleware
app.use(cors()); // Cho phép CORS (cấu hình chặt chẽ hơn nếu cần)
app.use(express.json()); // Parse JSON bodies

// API Routes
app.use('/api', apiRoutes);

// Serve static files from the Vite build output
const clientBuildPath = path.join(__dirname, '../../client/dist'); // Đường dẫn tới thư mục build của client
app.use(express.static(clientBuildPath));

// Serve index.html for any other requests (SPA fallback)
app.get('*', (req, res) => {
  res.sendFile(path.join(clientBuildPath, 'index.html'));
});

// Error Handling Middleware (Basic)
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send('Something broke!');
});

// Start server
app.listen(port, () => {
  console.log(`Shiromi Web Server listening on port ${port}`);
  console.log(`Serving static files from: ${clientBuildPath}`);
});
// --- END OF FILE website/server/src/server.js ---
