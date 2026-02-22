# WebSocket Server for Real-Time Updates

This module implements a WebSocket server for broadcasting real-time updates to frontend clients.

## Features

- **Real-time Position Updates**: Broadcasts position changes (open, update, close)
- **Real-time Equity Updates**: Broadcasts equity and peak equity changes
- **Real-time Risk Status Updates**: Broadcasts risk status changes (soft pause, hard stop)
- **Real-time System Logs**: Broadcasts system log messages (INFO and above)
- **Connection Management**: Handles client connections and disconnections gracefully
- **Authentication Support**: Optional token-based authentication (MVP allows unauthenticated connections)
- **Graceful Degradation**: Continues operation when no clients are connected

## Requirements Validated

- **17.1**: WebSocket server for frontend connections
- **17.2**: Broadcast position updates to all connected clients
- **17.3**: Broadcast equity updates to all connected clients
- **17.4**: Broadcast risk status updates to all connected clients
- **17.5**: Broadcast system log messages to all connected clients
- **17.6**: Handle client disconnections gracefully
- **17.7**: Authenticate WebSocket connections before accepting subscriptions
- **17.8**: Continue normal operation when no clients are connected

## Architecture

### Components

1. **ConnectionManager**: Manages WebSocket connections and broadcasts
   - Maintains set of active connections
   - Provides broadcast methods for different message types
   - Handles connection/disconnection lifecycle

2. **FastAPI Application**: Provides WebSocket endpoint and health checks
   - `/ws`: WebSocket endpoint for client connections
   - `/health`: Health check endpoint
   - `/`: Root endpoint with service information

3. **Integration Points**:
   - **PositionService**: Broadcasts on position create/update/close
   - **EquityService**: Broadcasts on equity updates
   - **RiskEngine**: Broadcasts on risk status changes
   - **Logger**: Broadcasts system log messages (INFO and above)

## Usage

### Running the WebSocket Server

#### Standalone Mode

```bash
python backend/websocket/run_server.py
```

This runs the WebSocket server on `http://0.0.0.0:8000`.

#### Integrated Mode

The WebSocket server is automatically integrated into the main application. When you run `backend/main.py`, the WebSocket connection manager is initialized and broadcasts are sent automatically.

### Connecting from Frontend

#### JavaScript/TypeScript Example

```javascript
const ws = new WebSocket('ws://localhost:8000/ws?token=your-auth-token');

ws.onopen = () => {
  console.log('Connected to WebSocket server');
};

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  
  switch (message.type) {
    case 'connection_established':
      console.log('Connection confirmed:', message.message);
      break;
    
    case 'position_update':
      console.log('Position update:', message.data);
      // Update UI with position data
      break;
    
    case 'equity_update':
      console.log('Equity update:', message.data);
      // Update UI with equity data
      break;
    
    case 'risk_status_update':
      console.log('Risk status update:', message.data);
      // Update UI with risk status
      break;
    
    case 'system_log':
      console.log('System log:', message.data);
      // Display log in UI
      break;
  }
};

ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

ws.onclose = () => {
  console.log('Disconnected from WebSocket server');
  // Implement reconnection logic
};
```

#### Python Example (for testing)

```python
import asyncio
import websockets
import json

async def connect():
    uri = "ws://localhost:8000/ws?token=your-auth-token"
    
    async with websockets.connect(uri) as websocket:
        print("Connected to WebSocket server")
        
        # Receive messages
        async for message in websocket:
            data = json.loads(message)
            print(f"Received: {data['type']}")
            print(f"Data: {data}")

asyncio.run(connect())
```

## Message Types

### Connection Established

Sent immediately after connection is accepted.

```json
{
  "type": "connection_established",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "message": "Connected to Stable Cash & Carry WebSocket server"
}
```

### Position Update

Sent when a position is created, updated, or closed.

```json
{
  "type": "position_update",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "data": {
    "position_id": "uuid-here",
    "symbol": "BTC/USDT",
    "status": "open",
    "spot_entry_price": "45000.00",
    "perp_entry_price": "45010.00",
    "spot_quantity": "0.1",
    "perp_quantity": "0.1",
    "unrealized_pnl": "50.25",
    "funding_collected": "12.50"
  }
}
```

### Equity Update

Sent when total equity or peak equity changes.

```json
{
  "type": "equity_update",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "data": {
    "total_equity": "10000.00",
    "peak_equity": "10500.00",
    "current_equity": "10000.00"
  }
}
```

### Risk Status Update

Sent when risk status changes (soft pause, hard stop).

```json
{
  "type": "risk_status_update",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "data": {
    "status": "soft_pause",
    "message": "New position openings stopped. Existing positions remain open.",
    "drawdown_pct": "0.04",
    "daily_loss_pct": "0.03"
  }
}
```

### System Log

Sent for system log messages (INFO and above).

```json
{
  "type": "system_log",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "data": {
    "level": "INFO",
    "module": "PositionService",
    "message": "Position opened successfully",
    "timestamp": "2024-01-15T10:30:00.000Z"
  }
}
```

## Authentication

### MVP Implementation

For the MVP, authentication is optional. Connections without a token are accepted with a warning logged.

### Production Implementation

For production, implement proper JWT validation:

1. Generate JWT tokens on user login
2. Pass token as query parameter: `ws://host/ws?token=jwt-token`
3. Validate token in `ConnectionManager.connect()` method
4. Reject connections with invalid tokens

Example production authentication:

```python
async def connect(self, websocket: WebSocket, token: Optional[str] = None) -> bool:
    if token is None:
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="Authentication required"
        )
        return False
    
    # Validate JWT token
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id = payload.get("user_id")
        
        if not user_id:
            await websocket.close(
                code=status.WS_1008_POLICY_VIOLATION,
                reason="Invalid token"
            )
            return False
        
        await websocket.accept()
        # Store user_id with connection if needed
        return True
        
    except jwt.InvalidTokenError:
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="Invalid token"
        )
        return False
```

## Health Check

The WebSocket server provides a health check endpoint:

```bash
curl http://localhost:8000/health
```

Response:

```json
{
  "status": "healthy",
  "active_connections": 2,
  "timestamp": "2024-01-15T10:30:00.000Z"
}
```

## CORS Configuration

The server is configured with permissive CORS for development:

```python
allow_origins=["*"]  # Allow all origins
```

For production, specify exact origins:

```python
allow_origins=[
    "https://your-frontend-domain.com",
    "https://app.your-domain.com"
]
```

## Testing

Run the WebSocket server tests:

```bash
pytest tests/test_websocket_server.py -v
```

## Integration with Services

### PositionService

Broadcasts are automatically sent when:
- Position is created
- Position is updated
- Position is closed

### EquityService

Broadcasts are automatically sent when:
- Total equity is calculated
- Peak equity is updated

### RiskEngine

Broadcasts are automatically sent when:
- Soft pause is activated
- Hard stop is activated

### Logger

Broadcasts are automatically sent for:
- INFO level logs
- WARNING level logs
- ERROR level logs
- CRITICAL level logs

(DEBUG logs are not broadcast to reduce noise)

## Performance Considerations

- **Non-blocking**: All broadcasts are non-blocking and won't slow down the main application
- **Graceful degradation**: If no clients are connected, broadcasts are skipped immediately
- **Error handling**: Failed broadcasts to individual clients don't affect other clients
- **Automatic cleanup**: Disconnected clients are automatically removed from the connection pool

## Future Enhancements

1. **Message Queuing**: Queue messages during disconnection for replay on reconnection
2. **Selective Subscriptions**: Allow clients to subscribe to specific message types
3. **Rate Limiting**: Implement rate limiting for broadcasts to prevent overwhelming clients
4. **Compression**: Enable WebSocket compression for large messages
5. **Metrics**: Add metrics for connection count, message rate, etc.
