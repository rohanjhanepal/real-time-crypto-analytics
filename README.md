# Crypto Real-Time Analytics Platform

## Overview

This project implements a production-style real-time data engineering pipeline for ingesting, processing, and analyzing live cryptocurrency market data. The system streams trades from public exchange WebSocket APIs, aggregates them into time-based market data, computes technical indicators, and exposes analytics through an interactive dashboard.

The platform demonstrates scalable stream processing, fault-tolerant data pipelines, and low-latency analytics using modern open-source technologies.

---

## Key Capabilities

- Real-time market data ingestion via WebSocket streams
- Stream-based processing using Redis Streams and Consumer Groups
- OHLCV candle aggregation
- Technical indicator computation:
  - Relative Strength Index (RSI)
  - Moving Average Convergence Divergence (MACD)
  - Bollinger Bands
  - Simple and Exponential Moving Averages
- Dual-layer storage:
  - Redis for low-latency access
  - SQLite for historical persistence
- Interactive dashboard for real-time visualization
- Containerized deployment using Docker
- Fault-tolerant ingestion and processing

---

## System Architecture

External Exchange WebSocket
        |
        v
 Ingestion Service
        |
        v
  Redis Streams
        |
        v
Processing Service
        |
 -------------------
 |                 |
 v                 v
SQLite         Redis Cache
 |                 |
 -------------------
        |
        v
  Web Dashboard

---

## Technology Stack

- Language: Python 3.11
- Streaming: Redis Streams
- Ingestion: WebSockets (Binance API)
- Processing: Pandas, NumPy
- Storage: SQLite, Redis
- Visualization: Streamlit, Plotly
- Containerization: Docker, Docker Compose

---

## Project Structure

crypto-pipeline/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── data/
│   └── crypto.db
└── src/
    ├── common.py
    ├── producer.py
    ├── consumer.py
    ├── indicators.py
    ├── storage.py
    └── dashboard.py

---

## Installation and Setup

### Prerequisites

- Docker 20+
- Docker Compose v2+

---

### Environment Configuration

Copy the example environment file:

cp .env.example .env

Edit the file and adjust parameters as required.

---

### Build and Run

Start all services:

docker compose up --build

Open the dashboard in your browser:

http://localhost:8501

---

### Stop Services

`docker compose down`

---

### Run Tests
Once the containers are up and running run command below:

`docker compose run --rm consumer pytest -q`

## Reliability

- Automatic WebSocket reconnection
- Redis consumer groups for durable processing
- Explicit message acknowledgements
- Dead-letter stream for malformed events
- Stream trimming for memory control
- Automatic recovery after restarts

---

## Performance

- Supports multiple trading pairs concurrently
- Sub-second processing latency under normal conditions
- Horizontal scaling via additional consumers
- Bounded memory usage

---

## Limitations

- SQLite is intended for small to medium workloads
- No authentication or access control
- Not designed for high-frequency trading

For large-scale deployments, PostgreSQL and Kafka are recommended.

---

## Disclaimer

This project is for educational and demonstration purposes only.

It is not intended for live trading or financial decision-making. No warranty is provided.
