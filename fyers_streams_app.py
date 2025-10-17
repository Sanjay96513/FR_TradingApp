# fyers_streams_app.py
from kafka import KafkaConsumer
import json
import threading
import logging
from collections import defaultdict, deque
import numpy as np
from datetime import datetime, timedelta
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FyersStreamProcessor:
    def __init__(self):
        self.current_prices = {}
        self.historical_data = defaultdict(lambda: deque(maxlen=1000))
        self.aggregations = {}
        self.alerts = []
        self.patterns = defaultdict(list)
        self.trend_analytics = {}
        self.volatility_data = defaultdict(lambda: deque(maxlen=100))
        self.market_depth = {}

        self.lock = threading.Lock()
        self.running = True

    def process_stock_price(self, symbol, data):
        """Process incoming stock price data from Fyers"""
        with self.lock:
            # Update current price
            self.current_prices[symbol] = data

            # Store in historical data
            self.historical_data[symbol].append(data)

            # Update market depth
            self.update_market_depth(symbol, data)

            # Update volatility data
            self.update_volatility(symbol, data['price'])

            # Calculate aggregations
            self.calculate_aggregations(symbol)

            # Detect patterns
            self.detect_patterns(symbol)

            # Analyze trends
            self.analyze_trends(symbol)

            # Check for alerts
            self.check_alerts(symbol, data['price'])

    def update_market_depth(self, symbol, data):
        """Update market depth information"""
        self.market_depth[symbol] = {
            'bid': data.get('bid', 0),
            'ask': data.get('ask', 0),
            'bid_volume': data.get('bid_volume', 0),
            'ask_volume': data.get('ask_volume', 0),
            'spread': data.get('ask', 0) - data.get('bid', 0)
        }

    def update_volatility(self, symbol, price):
        """Update volatility calculations"""
        historical = list(self.historical_data[symbol])
        if len(historical) >= 2:
            prices = [item['price'] for item in historical]
            # Ensure the previous price is not zero to avoid division by zero
            if prices[-2] != 0:
                returns = np.diff(prices) / prices[:-1]
                if len(returns) > 0:
                    volatility = np.std(returns) * np.sqrt(252)  # Annualized
                    self.volatility_data[symbol].append(volatility)

    def calculate_aggregations(self, symbol):
        """Calculate real-time aggregations"""
        historical = list(self.historical_data[symbol])

        if len(historical) >= 5:
            prices = [item['price'] for item in historical[-5:]]
            self.aggregations[f"{symbol}_ma5"] = sum(prices) / len(prices)

        if len(historical) >= 20:
            prices = [item['price'] for item in historical[-20:]]
            self.aggregations[f"{symbol}_ma20"] = sum(prices) / len(prices)

        if len(historical) >= 50:
            prices = [item['price'] for item in historical[-50:]]
            self.aggregations[f"{symbol}_ma50"] = sum(prices) / len(prices)

        # Calculate high/low for the day
        today_data = [item for item in historical
                     if datetime.fromisoformat(item['timestamp']).date() == datetime.now().date()]
        if today_data:
            prices_today = [item['price'] for item in today_data]
            self.aggregations[f"{symbol}_day_high"] = max(prices_today)
            self.aggregations[f"{symbol}_day_low"] = min(prices_today)

    def detect_patterns(self, symbol):
        """Detect technical patterns"""
        historical = list(self.historical_data[symbol])
        if len(historical) < 10:
            return

        prices = [item['price'] for item in historical]

        # Simple pattern detection
        if self._is_overbought(prices):
            self.patterns[symbol].append({
                'type': 'overbought',
                'timestamp': datetime.now().isoformat()
            })

        if self._is_oversold(prices):
            self.patterns[symbol].append({
                'type': 'oversold',
                'timestamp': datetime.now().isoformat()
            })

    def _is_overbought(self, prices):
        """Simple overbought detection using RSI"""
        if len(prices) < 14:
            return False

        returns = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [r for r in returns[-14:] if r > 0]
        losses = [abs(r) for r in returns[-14:] if r < 0]

        avg_gain = sum(gains) / 14 if gains else 0
        avg_loss = sum(losses) / 14 if losses else 0

        if avg_loss == 0:
            return True if avg_gain > 0 else False

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi > 70

    def _is_oversold(self, prices):
        """Simple oversold detection using RSI"""
        if len(prices) < 14:
            return False

        returns = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [r for r in returns[-14:] if r > 0]
        losses = [abs(r) for r in returns[-14:] if r < 0]

        avg_gain = sum(gains) / 14 if gains else 0
        avg_loss = sum(losses) / 14 if losses else 0

        if avg_loss == 0:
            return False

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi < 30

    def analyze_trends(self, symbol):
        """Analyze price trends"""
        historical = list(self.historical_data[symbol])
        if len(historical) < 10:
            return

        prices = [item['price'] for item in historical[-10:]]
        slope = np.polyfit(range(len(prices)), prices, 1)[0]

        trend = "up" if slope > 0 else "down"
        strength = abs(slope)

        self.trend_analytics[symbol] = {
            'trend': trend,
            'slope': slope,
            'strength': strength,
            'timestamp': datetime.now().isoformat()
        }

    def check_alerts(self, symbol, current_price):
        """Check for alert triggers"""
        triggered_alerts = []

        for i, alert in enumerate(self.alerts):
            if alert['symbol'] == symbol:
                threshold = alert['threshold_price']
                alert_type = alert['alert_type']

                if (alert_type == 'above' and current_price >= threshold) or \
                   (alert_type == 'below' and current_price <= threshold):
                    logger.warning(f"ALERT: {symbol} price ${current_price} triggered {alert_type} ${threshold}")
                    triggered_alerts.append(i)

        # Remove triggered alerts
        for i in reversed(triggered_alerts):
            del self.alerts[i]

    def get_current_price(self, symbol):
        with self.lock:
            return self.current_prices.get(symbol)

    def get_all_current_prices(self):
        with self.lock:
            return list(self.current_prices.values())

    def get_historical_data(self, symbol, hours=24):
        with self.lock:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            filtered_data = [
                item for item in self.historical_data[symbol]
                if datetime.fromisoformat(item['timestamp']) >= cutoff_time
            ]
            return filtered_data

    def get_aggregations(self):
        with self.lock:
            return dict(self.aggregations)

    def get_patterns(self, symbol):
        with self.lock:
            return self.patterns.get(symbol, [])

    def get_trend_analytics(self, symbol):
        with self.lock:
            return self.trend_analytics.get(symbol, {})

    def get_volatility(self, symbol):
        with self.lock:
            if self.volatility_data[symbol]:
                return self.volatility_data[symbol][-1]
            return 0.0

    def get_market_depth(self, symbol):
        with self.lock:
            return self.market_depth.get(symbol, {})

# Global processor instance
processor = FyersStreamProcessor()

class KafkaStreamProcessor:
    def __init__(self, processor):
        self.processor = processor

    def start_consuming(self):
        """Start consuming from Kafka"""
        consumer = KafkaConsumer(
            'stock_prices',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='fyers_stock_streams_group'
        )

        logger.info("Fyers Kafka Streams processor started...")

        for message in consumer:
            try:
                data = message.value
                symbol = data['symbol']
                self.processor.process_stock_price(symbol, data)
            except Exception as e:
                logger.error(f"Error processing message: {e}")

def start_fyers_streams():
    """Start Fyers Kafka Streams in background thread"""
    stream_processor = KafkaStreamProcessor(processor)
    stream_processor.start_consuming()

if __name__ == "__main__":
    start_fyers_streams()