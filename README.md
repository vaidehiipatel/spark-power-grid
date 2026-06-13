# Smart Power Grid — Real-Time Stream Processing

## Scenario
Identify any residential areas with higher consumption values than the industrial ones by implementing Spark Structured Streaming on the UCI Household Power Consumption dataset.

## Window Type
Sliding window: 5-minute window, advancing every 1 minute.

## Why this window type?
In this case, a sliding window should be used as there is a need to receive notifications on an ongoing consumption trend regularly. With the help of a 5-minute window, enough data is collected to determine the average value. Also, the choice of a sliding window implies that the system does not need to wait until the end of a tumbling window to identify anomalies.

## Where the pipeline requires state
There are two points in the data pipeline where the state should be maintained. First, the stateful aggregation process used in Spark maintains partial aggregates across micro-batch boundaries until they are considered complete due to the watermarking. In addition, the average values for each meter/zones cannot be calculated until several file batches have been processed.

## Dataset
UCI Household Power Consumption
https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption

Download and place in data/. A sample of 40,000 rows is sufficient.

## Project Structure
```
spark-power-grid/
├── pipeline.py          # Main Spark Structured Streaming job
├── make_batches.py      # Splits dataset into stream batch files
├── static/
│   └── zone_mapping.csv # Static meter ID to zone type mapping
└── README.md
```

## Requirements
- Java 17
- Python 3.8+
- PySpark 3.5.0

Install PySpark:
```bash
pip3 install pyspark==3.5.0
```

## How to Run

### 1. Prepare the data
```bash
python3 make_batches.py
```

### 2. Open two terminal windows

**Terminal 1 — Start the Spark pipeline:**
```bash
python3 pipeline.py
```

**Terminal 2 — Simulate the stream (once Terminal 1 shows "Monitor running"):**
```bash
for f in stream_input_backup/batch_*.csv; do
    cp "$f" stream_input/
    sleep 2
done
```

### 3. Expected output
The pipeline prints out aggregations based on sliding windows every 15 seconds. After sufficient data has been collected, the alert table fires, showing residential zones whose average consumption is higher than the average consumption in the industrial zone in the same sliding window period.


## Alert Condition
A residential zone's average consumption exceeds the industrial zone average within the same sliding window → grid anomaly flagged with zone ID, window timestamps, and both consumption values.

![Grid Anomaly Alerts](screenshot.png)

