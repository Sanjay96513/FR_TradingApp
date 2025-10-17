# FastAPI + Kafka + Fyers: Real-time Stock Market Data Processing

This project implements a complete, real-time stock market data processing system using a modern data stack. It fetches live market data from the Fyers API, streams it through Apache Kafka for robust, scalable messaging, processes it in real-time to calculate advanced analytics, and serves the results via a high-performance FastAPI application with REST and WebSocket endpoints.

## System Architecture

```mermaid
graph TB
    A[Fyers API] --> B[Fyers Data Manager (Producer)]
    B --> C[Kafka Topic: stock_prices]
    C --> D[Fyers Stream Processor (Consumer)]
    D --> E[In-Memory Analytics Engine]
    E --> F[Aggregations (MA, RSI)]
    E --> G[Alerts]
    E --> H[Trend & Volatility Analysis]
    D --> I[FastAPI Application]
    I --> J[WebSocket Updates]
    I --> K[REST APIs]
    L[Frontend Apps / Clients] --> K
    L --> J
```

## Features

- **Live Data Integration:** Connects directly to the Fyers API for real-time stock quotes.
- **Scalable Data Streaming:** Uses Apache Kafka to handle high-throughput data streams, ensuring reliability and decoupling of services.
- **Real-time Analytics:** Performs on-the-fly calculations for:
    - Moving Averages (MA5, MA20, MA50)
    - Relative Strength Index (RSI) for overbought/oversold signals
    - Price trend and volatility analysis
    - Daily high/low tracking
- **High-Performance API:** Built with FastAPI, offering a fast and efficient REST API for historical and aggregated data.
- **Real-time Updates:** Provides WebSocket endpoints for pushing live updates to connected clients.
- **Secure Configuration:** Loads sensitive API credentials from environment variables, following security best practices.

## Prerequisites

- Python 3.10+
- An active Apache Kafka and Zookeeper instance.
- A Fyers Trading Account with API credentials.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-name>
    ```

2.  **Install the required Python packages:**
    ```bash
    pip install -r requirements.txt
    ```
    *(Note: A `requirements.txt` file would be generated for a production setup. For now, install manually as below.)*
    ```bash
    pip install fastapi uvicorn kafka-python numpy pandas requests websockets fyers_apiv3
    ```

## Configuration

This application requires you to set the following environment variables with your Fyers API credentials. You can get these from your [Fyers API Dashboard](https://myapi.fyers.in/).

-   `FYERS_CLIENT_ID`: Your Fyers application's Client ID.
-   `FYERS_ACCESS_TOKEN`: Your Fyers Access Token.

You can set them in your shell before running the application:

```bash
export FYERS_CLIENT_ID="YOUR_CLIENT_ID"
export FYERS_ACCESS_TOKEN="YOUR_ACCESS_TOKEN"
```

## How to Run the System

The system is composed of three main components that need to be run in separate terminals. Ensure Kafka and Zookeeper are running before you start.

**1. Start the Fyers Stream Processor**

This service consumes data from Kafka and performs the real-time analytics.

```bash
python fyers_streams_app.py
```

**2. Start the Fyers Data Manager**

This service connects to the Fyers API, fetches live data, and produces it to the `stock_prices` Kafka topic.

```bash
python fyers_data_manager.py
```

**3. Start the FastAPI Server**

This service serves the processed data via the REST API and WebSocket endpoints.

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

The API will now be available at `http://localhost:8000`.

## API Endpoints

Here are some of the key endpoints available:

-   `GET /api/stocks`: Get a list of all currently tracked stocks and their latest prices.
-   `GET /api/stocks/{symbol}`: Get the latest price and data for a specific stock symbol.
-   `GET /api/market-depth/{symbol}`: Get real-time market depth (bid/ask prices and volumes).
-   `GET /api/historical/{symbol}`: Get historical data for a symbol (defaults to the last 24 hours).
-   `GET /api/aggregations`: Get all calculated real-time aggregations (e.g., moving averages).
-   `GET /api/trends/{symbol}`: Get the latest trend analysis for a symbol.
-   `GET /api/volatility/{symbol}`: Get the latest volatility calculation for a symbol.
-   `GET /api/comprehensive-analysis/{symbol}`: Get a full analytical report for a symbol.
-   `POST /api/alerts`: Create a price alert for a stock.
-   `WS /ws/stock/{symbol}`: WebSocket endpoint for subscribing to real-time updates for a specific stock.