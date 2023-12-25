import json
from client import Client
from websocket import WebSocketApp
import time
import math
import threading
import asyncio
import aiohttp

class Mexc(Client):

    def __init__(self, url, pair_with, exchange, orderbooks, is_top,  sorted_data, current_symbol_dict,top_exchange):
        super().__init__(url, exchange)
        self.exchange = exchange.upper()
        self.pair_with = pair_with
        self.orderbook = orderbooks
        self.top_pairs = {}
        self.mexc_top_pair_list = []
        self.is_top = is_top
        self.new_added_symbols = []
        self.remove_symbols = []
        self.sorted_data = sorted_data
        self.interval = 20
        self.current_symbol_dict = current_symbol_dict
        self.top_exchange = top_exchange
        self.copy_top_pairs =[]
            
        
    async def screen_top_pair(self):
        if self.is_top:
            url = "https://api.mexc.com/api/v3/ticker/24hr"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:

                    symbols_list = await response.json()

                    prohibited_substring = ["DOWN", "UP", "3S", "2S", "3L", "2L"]

                    symbol_data = {}

                    symbol_data[self.exchange] = {symbol_pair["symbol"].replace(f"{self.pair_with.upper()}", "") : {"volume" : round(float(symbol_pair["volume"]), 2) , "per_change" : round(float(symbol_pair["priceChangePercent"]) * 100, 2)} for symbol_pair in symbols_list  if f"{self.pair_with.upper()}" in symbol_pair["symbol"] and not  any(item in symbol_pair["symbol"] for item in prohibited_substring)}
                    
                    for symbol in symbols_list:
                        if self.orderbook[self.exchange].get(symbol):
                            self.orderbook[self.exchange][symbol.replace("USDT", "")]["data"]["volume"] = round(float(symbol["volume"]), 2)
                            self.orderbook[self.exchange][symbol.replace("USDT", "")]["data"]["per_change"] = round(float(symbol["priceChangePercent"]) * 100, 2)

                    sorted_data = {
                        exchange: dict(sorted(coins.items(), key=lambda item: item[1]["per_change"],reverse=True))
                        for exchange, coins in symbol_data.items()
                    }
                    
                    self.sorted_data[self.exchange] = sorted_data[self.exchange]

            final_list = []

            lists_to_compare = list(self.sorted_data.keys())
            lists_to_compare.remove(self.exchange)

            for item in list((self.sorted_data[self.exchange]).keys()):

                if any(item in list((self.sorted_data[sublist]).keys()) for sublist in lists_to_compare):
                    final_list.append(item)

            self.mexc_top_pair_list = final_list[:10]


        # self.mexc_top_pair_list.append("BTC")

        sorted_dict_bool = sorted(self.current_symbol_dict[self.exchange]) != sorted(self.mexc_top_pair_list) if self.is_top else sorted(self.current_symbol_dict[self.top_exchange]) != sorted(self.copy_top_pairs)
        

        if sorted_dict_bool:
          
            new_added_symbols = list(set(self.mexc_top_pair_list) - set(self.current_symbol_dict[self.top_exchange])) if self.is_top else list(set(self.current_symbol_dict[self.top_exchange]) - set(self.mexc_top_pair_list))
            remove_symbols = list(set(self.current_symbol_dict[self.top_exchange]) - set(self.mexc_top_pair_list)) if self.is_top else list(set(self.mexc_top_pair_list) - set(self.current_symbol_dict[self.top_exchange]))
             
            added_items = [f"spot@public.bookTicker.v3.api@{symbol}{self.pair_with}" for symbol in new_added_symbols if symbol in list(self.sorted_data[self.exchange].keys())]
            removed_items = [f"spot@public.bookTicker.v3.api@{symbol}{self.pair_with}" for symbol in remove_symbols if symbol in list(self.sorted_data[self.exchange].keys())]
        
            self.current_symbol_dict[self.exchange] = self.mexc_top_pair_list
            self.copy_top_pairs = self.current_symbol_dict[self.top_exchange]
    
            methods = {"UNSUBSCRIPTION" : removed_items , "SUBSCRIPTION" : added_items}

            for method in methods:
                if len(methods[method]) != 0 :
                    subscribe_message = {
                        "method": method,
                        "params": methods[method]
                    }
                    self.ws.send(json.dumps(subscribe_message))
                    print(subscribe_message, self.exchange)
                    if method == "UNSUBSCRIPTION":
                        print(remove_symbols, "remove_symbols" , self.exchange)
                        for rm_symbol in remove_symbols:
                             if self.orderbook["MEXC"].get(rm_symbol):
                                del self.orderbook["MEXC"][rm_symbol]
                    
        
    def send_ping(self):
        try:
            self.ws.send(json.dumps({"method":"PING"}))

            #top 10 pair retrive
            asyncio.run(self.screen_top_pair())

            threading.Timer(self.interval, self.send_ping).start()
        except:
            threading.Timer(self.interval, self.send_ping).start()

    def on_message(self, ws, message):
        data = json.loads(message)
        # stream symbol data and orderbook update

        if data.get("c") and  "spot@public.bookTicker.v3.api" in data["c"]:

            if self.exchange in self.orderbook:
                self.orderbook[self.exchange][data["s"].replace("USDT", "")] = {"data" : {"buy" : data["d"]["a"], "sell" : data["d"]["b"], "volume" : 0, "per_change" : 0}, "lastupdate" : int(time.time())}


    def on_open(self, ws):

        symbol_list = list(f"spot@public.bookTicker.v3.api@{symbol}{self.pair_with}" for symbol in self.current_symbol_dict[self.exchange])
        
        subscribe_message = {
            "method": "SUBSCRIPTION",
            "params": symbol_list
        }

        self.mexc_top_pair_list = self.current_symbol_dict[self.exchange]
        self.copy_top_pair_list = self.current_symbol_dict[self.top_exchange] 

        self.ws.send(json.dumps(subscribe_message))

        time.sleep(2)
        self.send_ping()
