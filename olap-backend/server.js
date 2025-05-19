const express = require('express');
const cors = require('cors');
const olapRoutes = require('./routes/olap');

const app = express();
app.use(cors());
app.use(express.json());

app.use('/api/olap', olapRoutes);

const PORT = 3001;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
