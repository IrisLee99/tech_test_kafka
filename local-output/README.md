# CSV Reconstruction Consumer

This Node.js application subscribes to the Apache Kafka topic `raw-transactions` and reconstructs the original CSV files from JSON messages.

## Features

- Subscribes to `raw-transactions` Kafka topic
- Reconstructs CSV files with original filenames
- Maintains proper row ordering using message keys
- Handles CSV escaping for special characters
- Graceful shutdown with message processing
- Progress tracking and detailed logging
- Error handling and recovery

## Prerequisites

- Node.js (v14 or higher)
- Apache Kafka running on `localhost:9092`
- Kafka topic `raw-transactions` with messages from the producer

## Installation

Dependencies are already installed. If you need to reinstall:

```bash
npm install
```

## Usage

### Start the consumer

```bash
npm start
# or
npm run consume
# or
node index.js
```

### Prerequisites

Make sure the producer has sent messages to the `raw-transactions` topic:

```bash
# In local-input directory
cd ../local-input
npm start
```

## How It Works

1. **Connection**: Connects to Kafka at `localhost:9092`
2. **Subscription**: Subscribes to `raw-transactions` topic from the beginning
3. **Message Processing**: 
   - Receives JSON messages with metadata headers
   - Groups messages by source filename
   - Maintains row order using message keys
4. **File Reconstruction**:
   - Converts JSON back to CSV format
   - Handles CSV escaping for special characters
   - Writes files with original names to current directory
5. **Graceful Shutdown**: Processes remaining messages before disconnecting

## Message Processing

The consumer expects messages with:
- **Key**: `{filename}-{rowIndex}` (used for ordering)
- **Value**: JSON object representing CSV row
- **Headers**: 
  - `source`: Original filename
  - `timestamp`: When message was sent

## Output Files

Reconstructed CSV files will be created in the `local-output` directory:
- `artificial-test-transactions.csv`
- `artificial-test-transactions-1.csv`
- `artificial-test-transactions-2.csv`

## Configuration

The consumer is configured to connect to Kafka at `localhost:9092`. To change this, modify the `brokers` array in `index.js`:

```javascript
const kafka = Kafka({
  clientId: 'csv-consumer',
  brokers: ['your-kafka-broker:9092'],
});
```

## Consumer Group

The consumer uses group ID `csv-reconstruction-group`. This ensures:
- Messages are processed only once per group
- Multiple consumers can work together
- Offset management is handled automatically

## Monitoring

The application provides detailed console output showing:
- Connection status
- Messages received and processed
- File reconstruction progress
- Error handling and recovery
- Graceful shutdown status

## Error Handling

- Individual message processing errors won't stop the consumer
- Graceful shutdown on SIGINT/SIGTERM
- Uncaught exception handling
- Detailed error logging

## Testing the Complete Flow

1. **Start Kafka** (if not already running)
2. **Start the consumer**:
   ```bash
   cd local-output
   npm start
   ```
3. **In another terminal, start the producer**:
   ```bash
   cd local-input
   npm start
   ```
4. **Verify**: Check that CSV files are reconstructed in `local-output`

## Troubleshooting

- **No messages received**: Ensure producer has sent messages to the topic
- **Connection errors**: Verify Kafka is running on `localhost:9092`
- **File reconstruction errors**: Check message format and headers
- **Missing files**: Ensure all messages were sent and received
