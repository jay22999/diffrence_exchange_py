from client import Client
import json
from datetime import datetime
import re
import math
import time
import threading

class Binance(Client):
    def __init__(self, url, exchange, orderbooks, symbol , pair_with):
        super().__init__(url, exchange)
        self.orderbook = orderbooks
        self.exchange = exchange
        self.symbol= symbol
        self.pair_with = pair_with
        self.id = 1

    def orderbook_update(self, data, lock):
        with lock:
            self.orderbook[self.exchange][self.symbol] = {"data" : data, "lastupdate" : int(time.time())}
        
    def on_message(self, ws, message):
        data = json.loads(message)
        if (data.get("bids")):
            del data["lastUpdateId"]
            lock = threading.Lock()
            t1 = threading.Thread(target=self.orderbook_update , args= (data,lock,))
            t1.daemon = True
            t1.start()
            

    def on_ping(self, a,b):
        self.ws.send(json.dumps({"method": "GET_PROPERTY", "params": ["combined"], "id": self.id}))
        self.id = 1+1

    def on_open(self, ws):
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [f"{self.symbol.lower()}{self.pair_with.lower()}@depth20@1000ms"],
            "id": 1
        }
        self.ws.send(json.dumps(subscribe_message))

    def on_close(self, a,b,c):
        print(f"{self.exchange}--close , symbol: {self.symbol}") 