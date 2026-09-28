# 🚀 Day 25 - Apache Spark Window Operations

## 📌 Project Overview

Day 25 demonstrates **window operations in Apache Spark DStreams** using Scala.

The project processes streaming sales transactions received through a TCP socket using `nc` and calculates rolling transaction counts and rolling sales totals.

The project also demonstrates how a sudden increase in transaction activity can be detected using a time-based window.

---

## 🎯 Objectives

- Understand batch interval
- Understand window size
- Understand sliding interval
- Use `countByWindow`
- Use `reduceByKeyAndWindow`
- Calculate rolling sales totals
- Monitor transaction activity
- Detect a sudden increase in transactions
- Process streaming sales data using DStreams

---

## 🛠️ Technologies Used

- Apache Spark 3.5.3
- Spark Streaming / DStreams
- Scala 2.12.18
- SBT
- Ubuntu/Linux
- TCP Socket
- Netcat (`nc`)
- Git & GitHub

---

## 📁 Project Structure

```text
day25-spark/
│
├── project/
│   └── build.properties
│
├── src/
│   └── main/
│       └── scala/
│           └── Day25WindowOperations.scala
│
├── .gitignore
├── build.sbt
└── README.md
```

---

# ⏱️ Window Concepts

## Batch Interval

The **batch interval** determines how frequently Spark Streaming creates a new micro-batch.

This project uses:

```text
Batch Interval = 2 seconds
```

---

## Window Size

The **window size** determines how much recent streaming data is included in each calculation.

This project uses:

```text
Window Size = 10 seconds
```

Therefore, every calculation considers transactions from the latest 10 seconds.

---

## Sliding Interval

The **sliding interval** determines how frequently the window moves forward and a new calculation is performed.

This project uses:

```text
Sliding Interval = 2 seconds
```

---

## 🔄 Window Processing

```text
Incoming Transactions
          ↓
     Micro-Batches
      2 seconds
          ↓
     10-second Window
          ↓
   ┌───────────────┐
   │ countByWindow │
   └───────────────┘
          ↓
 Transaction Count

          +

   ┌───────────────────────┐
   │ reduceByKeyAndWindow  │
   └───────────────────────┘
          ↓
   Rolling Sales Total
```

---

# 🔢 countByWindow

The project uses:

```scala
val transactionCount = sales.countByWindow(
  Seconds(10),
  Seconds(2)
)
```

This calculates the number of transactions received during the latest 10-second window.

### Configuration

```text
Window Size    : 10 seconds
Sliding Interval: 2 seconds
```

For example, if five transactions arrive within the current window:

```text
Transactions: 5
```

---

# 💰 reduceByKeyAndWindow

The project also calculates a rolling sales total.

Transactions are converted into:

```text
("SALES", amount)
```

Then:

```scala
val rollingSales = keyedSales.reduceByKeyAndWindow(
  (a: Double, b: Double) => a + b,
  Seconds(10),
  Seconds(2)
)
```

This calculates the total sales amount inside the current 10-second window.

Example:

```text
1000
2500
500
```

Rolling sales:

```text
SALES -> 4000.0
```

---

# 🚨 Sudden Transaction Increase Detection

The project monitors the number of transactions in the 10-second window.

If the number of transactions reaches or exceeds 5:

```text
ALERT: Sudden increase in transactions!
```

Otherwise:

```text
Normal transaction activity
```

This demonstrates a simple real-time transaction activity monitoring scenario.

---

# 🔌 Socket Input

The application reads transaction amounts from:

```text
localhost:9999
```

The Spark application uses:

```scala
socketTextStream("localhost", 9999)
```

---

# 🖥️ Running the Application

## Step 1 - Start Spark

Open Terminal 1:

```bash
cd ~/day25-spark
sbt -error run
```

The application displays:

```text
==========================================
       DAY 25 - WINDOW STREAMING
==========================================
Batch Interval : 2 seconds
Window Size    : 10 seconds
Slide Interval : 2 seconds
Socket         : localhost:9999
Status         : WAITING FOR SALES
==========================================
```

---

## Step 2 - Start Netcat

Open Terminal 2:

```bash
nc -lk 9999
```

---

## Step 3 - Send Transaction Amounts

Enter transaction amounts:

```text
1000
2500
500
1500
3000
```

Press **Enter** after each transaction.

Spark processes the incoming data using 2-second micro-batches and a 10-second window.

---

# 📊 Sample Output

### Count by Window

```text
==========================================
          COUNT BY WINDOW
==========================================
Window Size     : 10 seconds
Slide Interval  : 2 seconds
Transactions    : 1
==========================================
```

### Rolling Sales Total

```text
==========================================
        REDUCE BY KEY AND WINDOW
==========================================
Rolling Sales Total (10 sec):
SALES -> 3000.0
==========================================
```

### Activity Monitor

```text
========== ACTIVITY MONITOR ==========
Normal transaction activity
Transactions in 10-sec window: 1
=======================================
```

When transaction activity reaches the configured threshold:

```text
========== ACTIVITY MONITOR ==========
ALERT: Sudden increase in transactions!
Transactions in 10-sec window: 5
=======================================
```

---

# 🏦 Real-World Scenario

This project represents a **real-time transaction monitoring system**.

A banking or payment system can continuously receive transaction events:

```text
1000
2500
500
1500
3000
```

A sliding window can be used to monitor recent transaction activity.

For example:

```text
10-minute window
        ↓
Count transactions
        ↓
Compare with threshold
        ↓
Generate alert
```

The current demonstration uses a **10-second window** so that the behavior can be observed quickly during development.

---

# 🧠 Key Concepts Learned

### Batch Interval

Frequency at which Spark creates micro-batches.

```text
2 seconds
```

### Window Size

Amount of recent streaming data considered in the calculation.

```text
10 seconds
```

### Sliding Interval

Frequency at which the window moves and recalculates results.

```text
2 seconds
```

### `countByWindow`

Counts records within a sliding time window.

### `reduceByKeyAndWindow`

Aggregates values for keys within a sliding time window.

### Rolling Aggregation

A rolling aggregation continuously calculates results over the most recent window of data.

---

# 🔄 Processing Flow

```text
Transaction Stream
        ↓
TCP Socket
        ↓
DStream
        ↓
2-second Micro-Batches
        ↓
10-second Sliding Window
        ↓
 ┌─────────────────────┐
 │                     │
 ↓                     ↓
countByWindow    reduceByKeyAndWindow
 │                     │
 ↓                     ↓
Transaction Count   Sales Total
 │
 ↓
Activity Monitoring
 │
 ↓
Alert / Normal
```

---

# 📌 Current Project Configuration

```text
Batch Interval  : 2 seconds
Window Size     : 10 seconds
Sliding Interval: 2 seconds
Socket          : localhost:9999
Alert Threshold : 5 transactions
```

---

# ✅ Day 25 Checklist

- [x] Create StreamingContext
- [x] Configure batch interval
- [x] Explain window size
- [x] Explain sliding interval
- [x] Use `countByWindow`
- [x] Use `reduceByKeyAndWindow`
- [x] Calculate rolling sales totals
- [x] Monitor transaction activity
- [x] Detect sudden transaction increase
- [x] Use TCP socket streaming
- [x] Use `nc` for transaction input
- [x] Verify window output

---

⭐ **Day 25 – Apache Spark Window Operations**
