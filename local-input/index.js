const { Kafka, CompressionTypes } = require('kafkajs');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const winston = require('winston');
require('winston-daily-rotate-file');

// Ensure logs directory exists
if (!fs.existsSync('logs')) {
  fs.mkdirSync('logs', { recursive: true });
}

const kafka = new Kafka({
  clientId: 'csv-producer',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

// Daily rolling log setup
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level.toUpperCase()}]: ${message}`;
    })
  ),
  transports: [
    new winston.transports.DailyRotateFile({
      filename: 'logs/producer-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      maxSize: '20m',
      maxFiles: '14d'
    }),
    new winston.transports.Console()
  ]
});

const TOPIC = 'raw-transactions';
const BATCH_SIZE = 5;

// Stream + send in batches
async function processCSVFile(filePath) {
  const fileName = path.basename(filePath);

  return new Promise((resolve, reject) => {
    let rowNumber = 0;
    let batch = [];

    const stream = fs.createReadStream(filePath).pipe(csv());

    stream.on('data', async (row) => {
      stream.pause(); // backpressure control

      batch.push({
        key: fileName, // ensures same partition → ordering guarantee
        value: JSON.stringify({
          fileId: fileName,
          rowNumber,
          payload: row,
        }),
      });

      rowNumber++;

      if (batch.length >= BATCH_SIZE) {
        await sendBatch(batch);
        batch = [];
      }

      stream.resume();
    });

    stream.on('end', async () => {
      if (batch.length > 0) {
        await sendBatch(batch);
      }

      logger.info(`Finished processing ${fileName} (${rowNumber} rows)`);
      resolve();
    });

    stream.on('error', reject);
  });
}

async function sendBatch(messages) {
  await producer.send({
    topic: TOPIC,
    messages,
    compression: CompressionTypes.GZIP,
  });
}

// MAIN
async function main() {
  logger.info('Starting CSV producer...');
  await producer.connect();

  const csvFiles = fs.readdirSync(__dirname)
    .filter(f => f.endsWith('.csv'))
    .map(f => path.join(__dirname, f));

  logger.info(`Processing files concurrently: ${csvFiles.join(', ')}`);

  // Parallel processing
  await Promise.all(csvFiles.map(processCSVFile));

  await producer.disconnect();  logger.info('Producer disconnected. All files processed.');}

if (require.main === module) {
  main().catch((err) => {
    logger.error('Producer error:', err);
    process.exit(1);
  });
}