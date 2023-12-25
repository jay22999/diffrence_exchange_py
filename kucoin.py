import json
from client import Client
from websocket import WebSocketApp
import requests
import math
import time
import threading
import re
import aiohttp
import asyncio



class Kucoin(Client):

    def __init__(self, url, exchange, orderbooks, pair_with, is_top, sorted_data, current_symbol_dict, top_exchange):
        super().__init__(url, exchange)
        self.exchange = exchange.upper()
        self.orderbook = orderbooks
        self.current_symbol_dict = current_symbol_dict
        self.interval = 2
        self.pair_with = pair_with
        self.is_top = is_top
        self.sorted_data = sorted_data
        self.kucoin_top_pair_list = []
        self.top_exchange = top_exchange
        self.copy_top_pairs = []

    async def screen_top_pair(self):
        if self.is_top:
            url = "https://api.kucoin.com/api/v1/market/allTickers"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:

                    symbols_list = await response.json()
                    symbols_list = symbols_list["data"]["ticker"]

                    prohibited_substring = ["DOWN", "UP", "3S", "2S", "3L", "2L"]

                    symbol_data = {}

                    symbol_data["KUCOIN"] = {symbol_pair["symbol"].replace(f"-{self.pair_with.upper()}", "") : {"volume" : round(float(symbol_pair["volValue"]), 2) , "per_change" : round(float(symbol_pair["changeRate"]) * 100, 2)} for symbol_pair in symbols_list  if f"-{self.pair_with.upper()}" in symbol_pair["symbol"] and not  any(item in symbol_pair["symbol"] for item in prohibited_substring)}

                    sorted_data = {
                        exchange: dict(sorted(coins.items(), key=lambda item: item[1]["per_change"],reverse=True))
                        for exchange, coins in symbol_data.items()
                    }
                    
                    self.sorted_data["KUCOIN"] = sorted_data["KUCOIN"]

            final_list = []

            lists_to_compare = list(self.sorted_data.keys())
            lists_to_compare.remove("KUCOIN")

            for item in list((self.sorted_data["KUCOIN"]).keys()):

                if any(item in list((self.sorted_data[sublist]).keys()) for sublist in lists_to_compare):
                    final_list.append(item)

            self.kucoin_top_pair_list = final_list[:10]

        
        sorted_dict_bool = sorted(self.current_symbol_dict[self.exchange]) != sorted(self.kucoin_top_pair_list) if self.is_top else sorted(self.current_symbol_dict[self.top_exchange]) != sorted(self.copy_top_pairs)
        
        if sorted_dict_bool:
                
            new_added_symbols = list(set(self.kucoin_top_pair_list) - set(self.current_symbol_dict[self.top_exchange])) if self.is_top else list(set(self.current_symbol_dict[self.top_exchange]) - set(self.kucoin_top_pair_list))
            remove_symbols = list(set(self.current_symbol_dict[self.top_exchange]) - set(self.kucoin_top_pair_list)) if self.is_top else list(set(self.kucoin_top_pair_list) - set(self.current_symbol_dict[self.top_exchange]))
            
            
            added_items = [f'{symbol}-USDT' for symbol in new_added_symbols if symbol in list(self.sorted_data[self.exchange].keys())]
            added_items = f"/market/snapshot:{','.join(added_items)}" if len(added_items) != 0 else None
            

            removed_items = [f'{symbol}-USDT' for symbol in remove_symbols if symbol in list(self.sorted_data[self.exchange].keys())]
            removed_items = f"/market/snapshot:{','.join(removed_items)}" if len(removed_items) != 0 else None

            self.current_symbol_dict[self.exchange] = self.kucoin_top_pair_list
            self.copy_top_pairs = self.current_symbol_dict[self.top_exchange]
                    
            methods = {"unsubscribe" : removed_items , "subscribe" : added_items}
        
            for method in methods:
                if methods[method] != None :
                    subscribe_message = {
                        "type": method,
                        "topic": methods[method]
                    }
                    self.ws.send(json.dumps(subscribe_message))

                    print(subscribe_message, self.exchange)

                    if method == "unsubscribe":
                        print(remove_symbols, "remove_symbols", self.exchange)
                        for rm_symbol in remove_symbols:
                            if self.orderbook["KUCOIN"].get(rm_symbol):
                                del self.orderbook["KUCOIN"][rm_symbol]
                    
    

    def send_ping(self):

        try:
            
            ping_message = json.dumps({"id": str(int(time.time() * 1000)), "type": "ping"})
            self.ws.send(ping_message)

            #top 10 pair retrive
            asyncio.run(self.screen_top_pair())

            threading.Timer(self.interval, self.send_ping).start()
            
        except:
            threading.Timer(self.interval, self.send_ping).start()

    def on_message(self, ws, message):
        data = json.loads(message)

        if data.get("topic"):
            data = data["data"]["data"]

            current_symbol = data["baseCurrency"]
            buy = data["buy"]
            sell = data["sell"]
            per_change = data["changeRate"]
            volume = round(float(data["volValue"]) ,2)

            self.orderbook[self.exchange][current_symbol] = { "data" : {"buy" : sell , "sell" : buy , "volume" : volume , "per_change" : per_change} , "lastupdate" : int(time.time())}

    def on_close(self, ws ,a,b):
        print(f"{self.exchange}--close") 
        print(ws ,a,b)

    def on_open(self, ws):

        message = f"/market/snapshot:{','.join([f'{symbol}-{self.pair_with}' for symbol in self.current_symbol_dict[self.exchange]])}"

        subscribe_message = {
                "type": "subscribe",
                "topic": message
            }
            
        self.ws.send(json.dumps(subscribe_message))

        self.kucoin_top_pair_list = self.current_symbol_dict[self.exchange]
        self.copy_top_pairs = self.current_symbol_dict[self.top_exchange]
        
        time.sleep(2)
        self.send_ping()

