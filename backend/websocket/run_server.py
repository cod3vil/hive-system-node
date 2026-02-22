"""
Standalone script to run the WebSocket server.

This can be run independently or integrated into the main application.
"""

import uvicorn
from websocket.websocket_server import create_websocket_app


def main():
    """Run the WebSocket server."""
    app = create_websocket_app()
    
    # Run with uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        access_log=True
    )


if __name__ == "__main__":
    main()
