const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');
const retry = require('async-retry');
const winston = require('winston');
require('winston-daily-rotate-file');

// Ensure logs directory exists
if (!fs.existsSync('logs')) {
  fs.mkdirSync('logs', { recursive: true });
}

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
      filename: 'logs/consumer-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      maxSize: '20m',
      maxFiles: '14d'
    }),
    new winston.transports.Console()
  ]
});

const kafka = new Kafka({
  clientId: 'csv-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'csv-reconstruction-group' });

const DLQ_PATH = path.join(__dirname, 'dead-letter-queue.jsonl');

// Per-file state
const fileState = new Map();

/*
fileState structure:
{
  file1.csv: {
    stream,
    expectedRow: 0,
    buffer: Map<rowNumber, payload>,
    headersWritten: false
  }
}
*/

function initFile(fileId, payload) {
  const outputPath = path.join(__dirname, fileId);

  const stream = fs.createWriteStream(outputPath);

  fileState.set(fileId, {
    stream,
    expectedRow: 0,
    buffer: new Map(),
    headersWritten: false,
  });

  return fileState.get(fileId);
}

function writeRow(state, payload) {
  if (!state.headersWritten) {
    const headers = Object.keys(payload).join(',') + '\n';
    state.stream.write(headers);
    state.headersWritten = true;
  }

  const row = Object.values(payload).join(',') + '\n';
  state.stream.write(row);
}

function addToDlq(msg, err, attempt) {
  const entry = {
    timestamp: new Date().toISOString(),
    partition: msg.partition,
    offset: msg.offset,
    attempt,
    error: err.message || String(err),
    message: msg.value.toString(),
  };

  fs.appendFileSync(DLQ_PATH, JSON.stringify(entry) + '\n', { encoding: 'utf8' });
  logger.error(`DLQ write: partition ${msg.partition}, offset ${msg.offset}, attempt ${attempt}`);
}

function processMessage(msg) {
  logger.info(`Received message: partition ${msg.partition}, offset ${msg.offset}`);
  const { fileId, rowNumber, payload } = JSON.parse(msg.value.toString());

  let state = fileState.get(fileId);
  if (!state) {
    state = initFile(fileId, payload);
  }

  // If correct order → write immediately
  if (rowNumber === state.expectedRow) {
    writeRow(state, payload);
    state.expectedRow++;

    // flush buffer if possible
    while (state.buffer.has(state.expectedRow)) {
      const buffered = state.buffer.get(state.expectedRow);
      state.buffer.delete(state.expectedRow);

      writeRow(state, buffered);
      state.expectedRow++;
    }

  } else {
    // out-of-order → buffer
    state.buffer.set(rowNumber, payload);
  }
}

async function processMessageWithRetries(msg, maxRetries = 3) {
  await retry(async (bail, attempt) => {
    try {
      processMessage(msg);
    } catch (err) {
      logger.warn(`Process message failed (attempt ${attempt}/${maxRetries})`, err);
      if (attempt >= maxRetries) {
        addToDlq(msg, err, attempt);
        // don't retry after DLQ write; bail out
        bail(err);
        return;
      }
      throw err;
    }
  }, {
    retries: maxRetries - 1,
    factor: 2,
    minTimeout: 100,
    maxTimeout: 1000,
  });
}

async function main() {
  logger.info('Starting CSV consumer...');
  await consumer.connect();

  await consumer.subscribe({
    topic: 'raw-transactions',
    fromBeginning: true,
  });

  await consumer.run({
    eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary }) => {
      for (const message of batch.messages) {
        await processMessageWithRetries(message, 3);
        resolveOffset(message.offset);
      }

      await commitOffsetsIfNecessary();
    },
  });
}

// Graceful shutdown
async function shutdown() {
  logger.info('Shutting down consumer...');

  for (const { stream } of fileState.values()) {
    stream.end();
  }

  await consumer.disconnect();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

if (require.main === module) {
  main().catch((err) => {
    logger.error('Consumer error:', err);
    process.exit(1);
  });
}