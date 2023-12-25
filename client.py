import websocket
import requests
import threading

from json import loads


class Client(threading.Thread):
    def __init__(self, url, exchange):
        super().__init__()
        # create websocket connection
        self.ws = websocket.WebSocketApp(
            url=url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
            on_ping=self.on_ping
        )

        # exchange name
        self.exchange = exchange

    # keep connection alive
    def run(self):
        self.ws.run_forever(ping_interval=1700, ping_timeout=60, reconnect=5)
        
    def on_error(self, ws, error):
        print(f"{self.exchange} on_error: {error}")

    # run when websocket is closed
    def on_close(self):
        print("### closed ###")

    # run when websocket is initialised
    def on_open(self, ws):
        pass

    def on_ping(self):
        pass