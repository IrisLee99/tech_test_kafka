# Principal Data Engineer Technical Test

## Overview

Thank you for taking the time to complete the Principal Data Engineer technical test.  

This should take you no more than one to two hours maximum and you are welcome to use whatever tools you feel appropriate: i.e the use of LLMs is permitted.

Some code has been provided, local-input directory, that reads 3 CSV files containing example banking transactions and writes the transactions as messages, JSON payload, onto a Kafka topic.

Running the code in the local-output directory reads the messages from the Kafka topic and reconstructs the original 3 files with the same row ordering (i.e. the output is identical to the input).

Instructions on how to run the existing code can be found below.

## Your Task

The CSV files could contain millions or tens of millions of rows.  Update the implementation to:

* Handle the fact the files could contain millions, or tens of millions, of rows.
* Still maintain the same row ordering as the input files when reconstructing the files in local-output.
* Process all 3 files concurrently in local-input.

You will be assessed on the conciseness, correctness and technical knowledge demonstrated in your solution.

## Submission

Once complete, please share your solution via an appropriate mechanism (e.g. Google Drive link or Github repo) and email daniel.cook@moneyhub.com with the location.

## Running The Existing Code

Download Kafka (binary download) from https://kafka.apache.org/downloads.

Start the server (replacing the version number for Kafka as appropriate):

```
tar -zxvf kafka_2.13-4.1.0.tgz
cd kafka_2.13-4.1.0
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

Create the topic that connects the two services (from another terminal):

```
cd kafka_2.13-4.1.0
bin/kafka-topics.sh --create --topic raw-transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Send the transactions:

```
cd local-input
npm install
npm start
```

Receive the transactions and re-create the original files:

```
cd local-output
npm install
npm start
```

After all transactions have been received: run Ctrl+C to exit the process and the files will be written.
