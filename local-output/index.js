const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');

// Kafka configuration
const kafka = new Kafka({
  clientId: 'csv-consumer',
  brokers: ['localhost:9092'], // Default Kafka broker address
});

const consumer = kafka.consumer({ groupId: 'csv-reconstruction-group' });

// Store to collect messages by source file
const fileData = new Map();
let totalMessagesReceived = 0;
let totalFilesReconstructed = 0;

// Function to convert JSON object to CSV row
function jsonToCSVRow(jsonObj) {
  const values = Object.values(jsonObj).map(value => {
    // Handle values that might contain commas or quotes
    if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
      return `"${value.replace(/"/g, '""')}"`;
    }
    return value;
  });
  return values.join(',');
}

// Function to reconstruct CSV file
function reconstructCSVFile(filename, messages) {
  try {
    // Sort messages by their original row index (extracted from key)
    const sortedMessages = messages.sort((a, b) => {
      const aIndex = parseInt(a.key.split('-').pop());
      const bIndex = parseInt(b.key.split('-').pop());
      return aIndex - bIndex;
    });

    // Get headers from the first message
    const firstMessage = sortedMessages[0];
    const headers = Object.keys(firstMessage.value);
    const csvContent = [headers.join(',')];

    // Add data rows
    sortedMessages.forEach(message => {
      const csvRow = jsonToCSVRow(message.value);
      csvContent.push(csvRow);
    });

    // Write to file
    const outputPath = path.join(__dirname, filename);
    fs.writeFileSync(outputPath, csvContent.join('\n'));
    
    console.log(`✅ Reconstructed ${filename} with ${sortedMessages.length} data rows`);
    return true;
  } catch (error) {
    console.error(`❌ Error reconstructing ${filename}:`, error);
    return false;
  }
}

// Function to process received messages
async function processMessages() {
  console.log('🔄 Processing received messages...');
  
  for (const [filename, messages] of fileData.entries()) {
    if (reconstructCSVFile(filename, messages)) {
      totalFilesReconstructed++;
    }
  }
  
  console.log(`\n📊 Summary:`);
  console.log(`   Total messages received: ${totalMessagesReceived}`);
  console.log(`   Files reconstructed: ${totalFilesReconstructed}`);
  console.log(`   Files in queue: ${fileData.size}`);
}

// Main consumer function
async function main() {
  try {
    console.log('🚀 Starting CSV reconstruction consumer...');
    
    // Connect to Kafka
    await consumer.connect();
    console.log('✅ Connected to Kafka');

    // Subscribe to the raw-transactions topic
    await consumer.subscribe({ 
      topic: 'raw-transactions',
      fromBeginning: true // Start from the beginning to catch all messages
    });
    console.log('📡 Subscribed to raw-transactions topic');

    // Start consuming messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          totalMessagesReceived++;
          
          // Parse the message
          const key = message.key.toString();
          const value = JSON.parse(message.value.toString());
          const headers = message.headers;
          
          // Extract source filename from headers
          const sourceFile = headers.source ? headers.source.toString() : 'unknown.csv';
          
          console.log(`📨 Received message ${totalMessagesReceived}: ${key} from ${sourceFile}`);
          
          // Group messages by source file
          if (!fileData.has(sourceFile)) {
            fileData.set(sourceFile, []);
          }
          
          fileData.get(sourceFile).push({
            key: key,
            value: value,
            headers: headers,
            partition: partition,
            offset: message.offset
          });
          
          // Show progress every 10 messages
          if (totalMessagesReceived % 10 === 0) {
            console.log(`📈 Progress: ${totalMessagesReceived} messages received, ${fileData.size} files in queue`);
          }
          
        } catch (error) {
          console.error('❌ Error processing message:', error);
        }
      },
    });

  } catch (error) {
    console.error('❌ Error in consumer:', error);
  }
}

// Function to handle graceful shutdown
async function gracefulShutdown() {
  console.log('\n🛑 Shutting down gracefully...');
  
  // Process any remaining messages
  if (fileData.size > 0) {
    console.log('🔄 Processing remaining messages before shutdown...');
    await processMessages();
  }
  
  // Disconnect from Kafka
  await consumer.disconnect();
  console.log('✅ Disconnected from Kafka');
  console.log('👋 Consumer shutdown complete');
}

// Handle graceful shutdown signals
process.on('SIGINT', async () => {
  console.log('\n📡 Received SIGINT, shutting down gracefully...');
  await gracefulShutdown();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n📡 Received SIGTERM, shutting down gracefully...');
  await gracefulShutdown();
  process.exit(0);
});

// Handle uncaught exceptions
process.on('uncaughtException', async (error) => {
  console.error('💥 Uncaught Exception:', error);
  await gracefulShutdown();
  process.exit(1);
});

process.on('unhandledRejection', async (reason, promise) => {
  console.error('💥 Unhandled Rejection at:', promise, 'reason:', reason);
  await gracefulShutdown();
  process.exit(1);
});

// Run the main function
if (require.main === module) {
  main().catch(async (error) => {
    console.error('💥 Fatal error:', error);
    await gracefulShutdown();
    process.exit(1);
  });
}

module.exports = { main, processMessages, reconstructCSVFile };
