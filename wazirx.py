import json
from client import Client
from websocket import WebSocketApp
import math
import time



class Wazirx(Client):

    def __init__(self, url, exchange, orderbooks, symbol,  pair_with):
        super().__init__(url, exchange)
        self.exchange = exchange
        self.orderbook = orderbooks
        self.symbol = symbol
        self.pair_with =  pair_with

    def on_message(self, ws, message):
        data = json.loads(message)
        data = data["data"]
        if (data.get("s")):
            self.current_symbol = data["s"].replace(self.pair_with.lower(), "")

            self.orderbook[self.exchange][self.current_symbol.upper()] = {"data" : {
                'bids': data['b'], 'asks': data['a']} , "lastupdate" : int(time.time())}  

    def on_ping(self,ws, b):
        self.ws.send(json.dumps({"event":"ping"}))

    def on_open(self, ws):
        subscribe_message = {
            "event": "subscribe",
            "streams": [f"{self.symbol.lower()}{self.pair_with.lower()}@depth20@100ms"]
        }
        self.ws.send(json.dumps(subscribe_message))

    def on_close(self, a,b,c):
        print(f"{self.exchange}--close, symbol : {self.current_symbol}") 
