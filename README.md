# Hands-on L10: Spark Structured Streaming + Machine Learning with MLlib

## Overview
This project demonstrates real-time analytics and machine learning using Apache Spark Structured Streaming and Spark MLlib.

The project includes two tasks:

- **Task 4:** Real-time fare prediction using a linear regression model trained on historical ride data.
- **Task 5:** Time-based fare trend prediction using windowed streaming averages and regression.

The input data is generated continuously by a Python socket server (`data_generator.py`), and Spark consumes this stream from `localhost:9999`.

---

## Files Included

- `data_generator.py`  
  Generates ride-sharing data and streams it over a socket.

- `task4.py`  
  Trains a fare prediction model and performs real-time inference on streaming ride data.

- `task5.py`  
  Trains a fare trend model and predicts average fare trends over streaming time windows.

- `training-dataset.csv`  
  Historical dataset used for offline model training.

- `README.md`  
  Project instructions and reproduction steps.

---

## Environment Used

This project was run successfully in **GitHub Codespaces** using:

- Python 3.12
- Java 17
- PySpark
- Faker
- NumPy

---

## Installation

Install the required Python packages:

```bash
python3 -m pip install pyspark faker numpy
```

Set Java 17 before running Spark:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Verify Java:

```bash
java -version
```

---

## How to Reproduce the Project

### Step 1: Start the data generator
Open a terminal and run:

```bash
python3 data_generator.py
```

This starts a socket server that streams ride data to:

```text
localhost:9999
```

Leave this terminal running.

---

### Step 2: Run Task 4
Open a second terminal and run:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
python3 task4.py
```

### What Task 4 does
- Loads `training-dataset.csv`
- Trains a linear regression model in memory
- Reads streaming ride data from the socket
- Predicts fare in real time using `distance_km`
- Computes deviation between actual fare and predicted fare
- Prints results to the console

### Expected output columns
- `trip_id`
- `driver_id`
- `distance_km`
- `fare_amount`
- `predicted_fare`
- `deviation`

---

### Step 3: Run Task 5
Stop Task 4 after capturing the output, but keep the data generator workflow in mind.

Because the data generator accepts one client connection at a time, the simplest reproduction process is:

1. Stop `task4.py`
2. Stop `data_generator.py`
3. Start `data_generator.py` again
4. Run `task5.py`

Open a terminal and run:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
python3 task5.py
```

### What Task 5 does
- Loads `training-dataset.csv`
- Trains a regression model in memory on time-based fare trends
- Reads live streaming ride data
- Aggregates fares over short time windows
- Predicts the next average fare trend
- Prints results to the console

### Expected output columns
- `window_start`
- `window_end`
- `avg_fare`
- `predicted_next_avg_fare`

---

## Notes

- The final working version trains models **in memory** instead of saving them to disk.
- This was done to avoid local filesystem and environment issues during Spark model persistence.
- No separate `models/` folder is required for the final version.
- No output folder is required because results are shown directly in the terminal.
- Screenshots of successful terminal output should be included for submission if required.

---

## Suggested Run Order

Use this order to reproduce the project successfully:

### Task 4
1. Start `data_generator.py`
2. Run `task4.py`
3. Capture screenshot of terminal output
4. Stop `task4.py`

### Task 5
1. Stop `data_generator.py`
2. Start `data_generator.py` again
3. Run `task5.py`
4. Wait for Batch 1 or later to print windowed results
5. Capture screenshot of terminal output
6. Stop `task5.py`

---

## Summary

This project successfully demonstrates:
- socket-based streaming input
- Spark Structured Streaming
- real-time regression inference
- deviation analysis
- windowed aggregation
- time-based fare trend prediction
