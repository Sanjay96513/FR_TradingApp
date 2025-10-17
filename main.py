# main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional
import asyncio
import json
from datetime import datetime, timedelta
import threading
import logging

# Initialize FastAPI app
app = FastAPI(title="Real-time Stock Market API with Fyers & Kafka Streams", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockPrice(BaseModel):
    symbol: str
    price: float
    timestamp: str
    volume: int
    change: float
    change_percent: float

class AlertConfig(BaseModel):
    symbol: str
    threshold_price: float
    alert_type: str  # 'above' or 'below'

class StockPriceResponse(BaseModel):
    symbol: str
    current_price: float
    change: float
    change_percent: float
    volume: int
    timestamp: str

# Import the processor from fyers_streams_app
from fyers_streams_app import processor

# In-memory storage for WebSocket connections
active_connections: Dict[str, List[WebSocket]] = {}

class WebSocketManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, symbol: str):
        await websocket.accept()
        if symbol not in self.active_connections:
            self.active_connections[symbol] = []
        self.active_connections[symbol].append(websocket)
        logger.info(f"WebSocket connected for symbol: {symbol}")

    def disconnect(self, websocket: WebSocket, symbol: str):
        if symbol in self.active_connections:
            self.active_connections[symbol].remove(websocket)
            if not self.active_connections[symbol]:
                del self.active_connections[symbol]
        logger.info(f"WebSocket disconnected for symbol: {symbol}")

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast_to_symbol(self, message: str, symbol: str):
        if symbol in self.active_connections:
            for connection in self.active_connections[symbol]:
                try:
                    await connection.send_text(message)
                except WebSocketDisconnect:
                    self.disconnect(connection, symbol)

# Initialize WebSocket manager
websocket_manager = WebSocketManager()

@app.on_event("startup")
async def startup_event():
    """Initialize Fyers Kafka Streams in background"""
    from fyers_streams_app import start_fyers_streams
    thread = threading.Thread(target=start_fyers_streams, daemon=True)
    thread.start()
    logger.info("Fyers Kafka Streams started")

@app.websocket("/ws/stock/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    """WebSocket endpoint for real-time stock updates"""
    await websocket_manager.connect(websocket, symbol)
    try:
        while True:
            # Keep connection alive
            data = await websocket.receive_text()
            # Handle any commands if needed
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket, symbol)

@app.get("/api/stocks/{symbol}", response_model=StockPriceResponse)
async def get_stock_price(symbol: str):
    """Get current stock price from Fyers data"""
    current_price = processor.get_current_price(symbol)
    if current_price:
        return StockPriceResponse(
            symbol=current_price['symbol'],
            current_price=current_price['price'],
            change=current_price['change'],
            change_percent=current_price['change_percent'],
            volume=current_price['volume'],
            timestamp=current_price['timestamp']
        )

    raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")

@app.get("/api/stocks", response_model=List[StockPriceResponse])
async def get_all_stocks():
    """Get all available stock prices"""
    all_prices = processor.get_all_current_prices()
    stocks = []
    for data in all_prices:
        stocks.append(StockPriceResponse(
            symbol=data['symbol'],
            current_price=data['price'],
            change=data['change'],
            change_percent=data['change_percent'],
            volume=data['volume'],
            timestamp=data['timestamp']
        ))
    return stocks

@app.post("/api/alerts")
async def create_alert(alert_config: AlertConfig):
    """Create price alert"""
    # Add alert to processor
    processor.alerts.append(alert_config.model_dump())
    return {"message": "Alert created successfully", "config": alert_config}

@app.get("/api/historical/{symbol}")
async def get_historical_data(symbol: str, hours: int = 24):
    """Get historical stock data"""
    historical_data = processor.get_historical_data(symbol, hours)
    return historical_data

@app.get("/api/aggregations")
async def get_aggregations():
    """Get real-time aggregations"""
    aggregations = processor.get_aggregations()
    return aggregations

@app.get("/api/patterns/{symbol}")
async def get_patterns(symbol: str):
    """Get detected patterns for a symbol"""
    patterns = processor.get_patterns(symbol)
    return patterns

@app.get("/api/trends/{symbol}")
async def get_trends(symbol: str):
    """Get trend analysis for a symbol"""
    trends = processor.get_trend_analytics(symbol)
    return trends

@app.get("/api/volatility/{symbol}")
async def get_volatility(symbol: str):
    """Get volatility for a symbol"""
    volatility = processor.get_volatility(symbol)
    return {"symbol": symbol, "volatility": volatility}

@app.get("/api/market-depth/{symbol}")
async def get_market_depth(symbol: str):
    """Get market depth for a symbol"""
    market_depth = processor.get_market_depth(symbol)
    return market_depth

@app.get("/api/market-status")
async def get_market_status():
    """Get overall market status"""
    all_prices = processor.get_all_current_prices()
    return {
        "status": "open",
        "last_update": datetime.now().isoformat(),
        "total_stocks": len(all_prices),
        "active_connections": sum(len(connections) for connections in websocket_manager.active_connections.values()),
        "processing_latency": "real-time",
        "data_source": "Fyers API"
    }

@app.get("/api/top-movers")
async def get_top_movers():
    """Get top gainers and losers"""
    all_prices = processor.get_all_current_prices()

    # Sort by change percentage
    sorted_prices = sorted(
        all_prices,
        key=lambda x: abs(x['change_percent']),
        reverse=True
    )

    top_gainers = [s for s in sorted_prices if s['change_percent'] > 0][:5]
    top_losers = [s for s in sorted_prices if s['change_percent'] < 0][:5]

    return {
        "top_gainers": top_gainers,
        "top_losers": top_losers
    }

# Advanced Analytics Endpoints
@app.get("/api/comprehensive-analysis/{symbol}")
async def get_comprehensive_analysis(symbol: str):
    """Get comprehensive technical analysis"""
    current_price = processor.get_current_price(symbol)
    historical_data = processor.get_historical_data(symbol, 30 * 24)  # 30 days
    patterns = processor.get_patterns(symbol)
    trends = processor.get_trend_analytics(symbol)
    volatility = processor.get_volatility(symbol)
    market_depth = processor.get_market_depth(symbol)

    return {
        "symbol": symbol,
        "current_price": current_price,
        "historical_data_count": len(historical_data),
        "patterns": patterns,
        "trends": trends,
        "volatility": volatility,
        "market_depth": market_depth,
        "analysis_timestamp": datetime.now().isoformat()
    }

# Fyers-specific endpoints
@app.get("/api/fyers-status")
async def get_fyers_status():
    """Get Fyers API status"""
    return {
        "status": "connected",
        "last_update": datetime.now().isoformat(),
        "streams_running": True,
        "data_source": "Fyers API via Kafka Streams"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)