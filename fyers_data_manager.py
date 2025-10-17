# fyers_data_manager.py
from fyers_apiv3 import fyersModel
import threading
import time
import json
from datetime import datetime
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FyersDataManager:
    def __init__(self, access_token: str, client_id: str):
        self.fyers = fyersModel.FyersModel(
            token=access_token,
            is_async=False,
            client_id=client_id,
            log_path=""
        )
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )

        # Default symbols to track
        self.symbols = [
            "NSE:SBIN-EQ", "NSE:RELIANCE-EQ", "NSE:TCS-EQ", "NSE:HDFCBANK-EQ",
            "NSE:INFY-EQ", "NSE:ICICIBANK-EQ", "NSE:KOTAKBANK-EQ", "NSE:AXISBANK-EQ"
        ]

    def get_quote(self, symbols: list):
        """Get real-time quotes for given symbols"""
        try:
            data = {
                "symbols": ','.join(symbols)
            }
            response = self.fyers.quotes(data=data)
            return response
        except Exception as e:
            logger.error(f"Error fetching quotes: {e}")
            return None

    def get_market_depth(self, symbols: list):
        """Get market depth for given symbols"""
        try:
            data = {
                "symbols": ','.join(symbols)
            }
            response = self.fyers.depth(data=data)
            return response
        except Exception as e:
            logger.error(f"Error fetching market depth: {e}")
            return None

    def format_stock_data(self, symbol_raw: str, data: dict):
        """Format Fyers data for Kafka"""
        # Extract symbol from nse:SBIN-EQ format
        symbol = symbol_raw.split(':')[1].replace('-EQ', '')

        stock_data = {
            'symbol': symbol,
            'price': data.get('ltp', 0),
            'timestamp': datetime.now().isoformat(),
            'volume': data.get('vol_traded_today', 0),
            'change': data.get('change', 0),
            'change_percent': data.get('per_change', 0),
            'open': data.get('open_price', 0),
            'high': data.get('high_price', 0),
            'low': data.get('low_price', 0),
            'close': data.get('prev_close_price', 0),
            'bid': data.get('bid_price', 0),
            'ask': data.get('ask_price', 0),
            'bid_volume': data.get('bid_size', 0),
            'ask_volume': data.get('ask_size', 0)
        }

        return stock_data

    def fetch_and_stream_data(self, interval: int = 1):
        """Fetch data from Fyers and stream to Kafka"""
        while True:
            try:
                response = self.get_quote(self.symbols)
                if response and response.get('s') == 'ok':
                    for symbol_data in response.get('d', []):
                        symbol_raw = symbol_data['n']
                        formatted_data = self.format_stock_data(symbol_raw, symbol_data['v'])
                        self.producer.send('stock_prices', key=formatted_data['symbol'], value=formatted_data)
                        self.producer.flush()
                        logger.info(f"Sent {formatted_data['symbol']} data to Kafka: {formatted_data['price']}")

                time.sleep(interval)
            except Exception as e:
                logger.error(f"Error in data fetching loop: {e}")
                time.sleep(5)  # Wait 5 seconds before retrying

    def start_data_streaming(self):
        """Start streaming data in a separate thread"""
        thread = threading.Thread(target=self.fetch_and_stream_data, daemon=True)
        thread.start()
        logger.info("Fyers data streaming started")
        return thread

# For authentication and initialization
def initialize_fyers_data_stream(access_token: str, client_id: str):
    """Initialize and start Fyers data streaming"""
    data_manager = FyersDataManager(access_token, client_id)
    data_manager.start_data_streaming()
    return data_manager

import os

# Example of how to use with your Fyers credentials
def run_fyers_data_stream():
    """Run the Fyers data stream with your credentials"""
    # Load credentials from environment variables
    ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
    CLIENT_ID = os.getenv("FYERS_CLIENT_ID")

    if not ACCESS_TOKEN or not CLIENT_ID:
        raise ValueError("FYERS_ACCESS_TOKEN and FYERS_CLIENT_ID environment variables must be set.")

    # Initialize the data manager
    data_manager = FyersDataManager(ACCESS_TOKEN, CLIENT_ID)

    # Start streaming data
    data_manager.start_data_streaming()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(60)  # Keep running
    except KeyboardInterrupt:
        logger.info("Shutting down Fyers data stream...")

if __name__ == "__main__":
    run_fyers_data_stream()