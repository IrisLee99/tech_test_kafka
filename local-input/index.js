const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

// Kafka configuration
const kafka = new Kafka({
  clientId: 'csv-producer',
  brokers: ['localhost:9092'], // Default Kafka broker address
});

const producer = kafka.producer();

// Function to read and process a single CSV file
async function processCSVFile(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => {
        results.push(data);
      })
      .on('end', () => {
        console.log(`Processed ${results.length} rows from ${path.basename(filePath)}`);
        resolve(results);
      })
      .on('error', (error) => {
        console.error(`Error reading ${filePath}:`, error);
        reject(error);
      });
  });
}

// Function to send messages to Kafka
async function sendToKafka(messages, sourceFile) {
  try {
    const kafkaMessages = messages.map((message, index) => ({
      key: `${path.basename(sourceFile)}-${index}`, // Use filename and row index as key
      value: JSON.stringify(message),
      headers: {
        source: path.basename(sourceFile),
        timestamp: new Date().toISOString()
      }
    }));

    await producer.send({
      topic: 'raw-transactions',
      messages: kafkaMessages
    });

    console.log(`Successfully sent ${kafkaMessages.length} messages from ${path.basename(sourceFile)} to raw-transactions topic`);
  } catch (error) {
    console.error(`Error sending messages from ${sourceFile}:`, error);
    throw error;
  }
}

// Main function
async function main() {
  try {
    console.log('Starting CSV to Kafka producer...');
    
    // Connect to Kafka
    await producer.connect();
    console.log('Connected to Kafka');

    // Find all CSV files in the current directory
    const csvFiles = fs.readdirSync(__dirname)
      .filter(file => file.endsWith('.csv'))
      .map(file => path.join(__dirname, file));

    if (csvFiles.length === 0) {
      console.log('No CSV files found in the current directory');
      return;
    }

    console.log(`Found ${csvFiles.length} CSV files:`, csvFiles.map(f => path.basename(f)));

    // Process each CSV file
    for (const csvFile of csvFiles) {
      console.log(`\nProcessing ${path.basename(csvFile)}...`);
      
      try {
        const rows = await processCSVFile(csvFile);
        
        if (rows.length > 0) {
          await sendToKafka(rows, csvFile);
        } else {
          console.log(`No data rows found in ${path.basename(csvFile)}`);
        }
      } catch (error) {
        console.error(`Failed to process ${csvFile}:`, error);
        // Continue with other files even if one fails
      }
    }

    console.log('\nAll CSV files processed successfully!');
    
  } catch (error) {
    console.error('Error in main process:', error);
  } finally {
    // Disconnect from Kafka
    await producer.disconnect();
    console.log('Disconnected from Kafka');
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nReceived SIGINT, shutting down gracefully...');
  await producer.disconnect();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\nReceived SIGTERM, shutting down gracefully...');
  await producer.disconnect();
  process.exit(0);
});

// Run the main function
if (require.main === module) {
  main().catch(console.error);
}

module.exports = { main, processCSVFile, sendToKafka };
