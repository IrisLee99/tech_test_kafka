# CSV to Kafka Producer

This Node.js application reads CSV files from the current directory and sends each row to an Apache Kafka topic named `raw-transactions` as JSON messages.

## Features

- Automatically discovers all `.csv` files in the current directory
- Converts CSV rows to JSON format
- Sends messages to Kafka topic `raw-transactions`
- Includes metadata headers (source file, timestamp)
- Graceful error handling and shutdown
- Progress logging

## Prerequisites

- Node.js (v14 or higher)
- Apache Kafka running on `localhost:9092`
- Kafka topic `raw-transactions` created

## Installation

Dependencies are already installed. If you need to reinstall:

```bash
npm install
```

## Usage

### Start the producer

```bash
npm start
# or
npm run produce
# or
node index.js
```

### Create the Kafka topic (if not exists)

```bash
# Using Kafka CLI tools
kafka-topics.sh --create --topic raw-transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## Configuration

The producer is configured to connect to Kafka at `localhost:9092`. To change this, modify the `brokers` array in `index.js`:

```javascript
const kafka = Kafka({
  clientId: 'csv-producer',
  brokers: ['your-kafka-broker:9092'],
});
```

## Message Format

Each CSV row is sent as a JSON message with:

- **Key**: `{filename}-{rowIndex}` (e.g., "artificial-test-transactions.csv-0")
- **Value**: JSON representation of the CSV row
- **Headers**: 
  - `source`: Original filename
  - `timestamp`: When the message was sent

## CSV Files

The application will process all `.csv` files in the current directory:

- `artificial-test-transactions.csv`
- `artificial-test-transactions-1.csv` 
- `artificial-test-transactions-2.csv`

## Error Handling

- Individual file processing errors won't stop the entire process
- Graceful shutdown on SIGINT/SIGTERM
- Detailed error logging

## Monitoring

The application provides detailed console output showing:
- Connection status
- Files being processed
- Number of rows processed per file
- Success/failure status for each file
