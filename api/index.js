const express = require('express');
const { Pool } = require('pg');

const app = express();
const port = 3000;

// Configure PostgreSQL clients using real environment variables
const sourceDb = new Pool({
  user: process.env.SOURCE_USER,        // Real environment variables
  host: process.env.SOURCE_HOST,        // Real environment variables
  database: process.env.SOURCE_DB,      // Real environment variables
  password: process.env.SOURCE_PASSWORD,// Real environment variables
  port: process.env.SOURCE_PORT || 5432 // Default to 5432 if not provided
});

const destinationDb = new Pool({
  user: process.env.DEST_USER,          // Real environment variables
  host: process.env.DEST_HOST,          // Real environment variables
  database: process.env.DEST_DB,        // Real environment variables
  password: process.env.DEST_PASSWORD,  // Real environment variables
  port: process.env.DEST_PORT || 5432   // Default to 5432 if not provided
});

// Function to transfer data
async function transferData() {
  try {
    const res = await sourceDb.query('SELECT * FROM source_table');
    const rows = res.rows;
    console.log('Fetched data from source DB:', rows);

    for (let row of rows) {
      await destinationDb.query(
        'INSERT INTO destination_table (column1, column2) VALUES ($1, $2)',
        [row.column1, row.column2]
      );
    }
    console.log('Data transferred to destination DB');
  } catch (err) {
    console.error('Error transferring data:', err);
  }
}

// Route to trigger data transfer
app.get('/load-data', async (req, res) => {
  try {
    console.log('Starting data transfer...');
    await transferData();
    res.status(200).send('Data transfer successful!');
  } catch (err) {
    console.error('Data transfer failed:', err);
    res.status(500).send('Data transfer failed!');
  }
});

app.get('/', (req, res) => {
  res.send('DB sync service is running!');
});

app.listen(port, () => {
  console.log(`App listening at http://localhost:${port}`);
});
