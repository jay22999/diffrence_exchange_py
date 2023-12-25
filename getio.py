import json
from client import Client
from websocket import WebSocketApp
import time
import asyncio
import aiohttp
import threading
import time


class Getio(Client):

    def __init__(self, url, pair_with, exchange, orderbooks, is_top,  sorted_data, current_symbol_dict, top_exchange):
        
        super().__init__(url, exchange)
        self.exchange = exchange.upper()
        self.pair_with = pair_with
        self.orderbook = orderbooks
        self.top_pairs = {}
        self.gateio_top_pair_list = []
        self.is_top = is_top
        self.new_added_symbols = []
        self.remove_symbols = []
        self.sorted_data = sorted_data
        self.interval = 2
        self.current_symbol_dict = current_symbol_dict
        self.is_first = False
        self.copy_top_pairs = []
        self.top_exchange = top_exchange


    async def screen_top_pair(self):
    
        if self.is_top:
            url = "https://api.gateio.ws/api/v4/spot/tickers"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:

                    symbols_list = await response.json()

                    prohibited_substring = ["DOWN", "UP", "3S", "2S", "3L", "2L"]

                    symbol_data = {}
                    

                    symbol_data["GATEIO"] = {symbol_pair["currency_pair"].replace(f"_{self.pair_with}", "") : {"volume" : round(float(symbol_pair["base_volume"]), 2) , "per_change" : round(float(symbol_pair["change_percentage"]), 2)} for symbol_pair in symbols_list  if  f"_{self.pair_with}" in symbol_pair["currency_pair"] and not  any(item in symbol_pair["currency_pair"] for item in prohibited_substring)}

                    sorted_data = {
                        exchange: dict(sorted(coins.items(), key=lambda item: item[1]["per_change"],reverse=True))
                        for exchange, coins in symbol_data.items()
                    }
                    
                    self.sorted_data["GATEIO"] = sorted_data["GATEIO"]

            final_list = []

            lists_to_compare = list(self.sorted_data.keys())
            lists_to_compare.remove(self.exchange)


            for item in list((self.sorted_data[self.exchange]).keys()):

                if any(item in list((self.sorted_data[sublist]).keys()) for sublist in lists_to_compare):
                    final_list.append(item)

            self.gateio_top_pair_list = final_list[:10]
        
        sorted_dict_bool = sorted(self.current_symbol_dict[self.exchange]) != sorted(self.gateio_top_pair_list) if self.is_top else sorted(self.current_symbol_dict[self.top_exchange]) != sorted(self.copy_top_pairs)

        if sorted_dict_bool:

            new_added_symbols = list(set(self.gateio_top_pair_list) - set(self.current_symbol_dict[self.exchange])) if self.is_top else list(set(self.current_symbol_dict[self.top_exchange]) - set(self.gateio_top_pair_list))
            remove_symbols = list(set(self.current_symbol_dict[self.exchange]) - set(self.gateio_top_pair_list)) if self.is_top else list(set(self.gateio_top_pair_list) - set(self.current_symbol_dict[self.top_exchange]))

            added_items = [f'{symbol}_{self.pair_with}' for symbol in new_added_symbols if symbol in list(self.sorted_data[self.exchange].keys())]
            removed_items = [f'{symbol}_{self.pair_with}' for symbol in  remove_symbols if symbol in list(self.sorted_data[self.exchange].keys())]
            
            self.gateio_top_pair_list = self.current_symbol_dict[self.exchange]
            self.copy_top_pairs = self.current_symbol_dict[self.top_exchange]

            methods = {"unsubscribe" : removed_items , "subscribe" : added_items}

            for method in methods:

                if len(methods[method]) != 0:
                    subscribe_message = {
                        "time": int(time.time()),
                        "channel": "spot.tickers",
                        "event": method,
                        "payload": methods[method]
                    }

                    self.ws.send(json.dumps(subscribe_message))   

                    # print(subscribe_message , self.exchange)

                    if method == "unsubscribe":
                        # print(remove_symbols, "remove_symbols" , self.exchange)
                        for rm_symbol in remove_symbols:
                            del self.orderbook["GATEIO"][rm_symbol]
                            
            

    def send_ping(self):

        try:

            ping_message = json.dumps({"time": int(time.time()), "channel" : "spot.ping"})
            self.ws.send(ping_message)

            #top 10 pair retrive

            asyncio.run(self.screen_top_pair())
                
            threading.Timer(self.interval, self.send_ping).start()
            
        except:
            threading.Timer(self.interval, self.send_ping).start()


    def on_message(self, ws, message):
        data = json.loads(message)
        if data["event"] == "update":
            data = data["result"]

            current_symbol = (data["currency_pair"]).replace("_USDT", "")
            buy = data["highest_bid"]
            sell = data["lowest_ask"]
            per_change = data["change_percentage"]
            volume = round(float(data["quote_volume"]), 2)

            self.orderbook[self.exchange][current_symbol] = { "data" : {"buy" : sell , "sell" : buy , "volume" : volume , "per_change" : per_change} , "lastupdate" : int(time.time())}


    def on_open(self, ws):

        message = [f'{symbol}_{self.pair_with}' for symbol in self.current_symbol_dict[self.exchange] ]

        subscribe_message = {
            "time": int(time.time()),
            "channel": "spot.tickers",
            "event": "subscribe",
            "payload": message
        }

        self.ws.send(json.dumps(subscribe_message))
        self.gateio_top_pair_list = self.current_symbol_dict[self.exchange] 
        self.copy_top_pairs = self.current_symbol_dict[self.top_exchange] 

        time.sleep(2)
        self.send_ping()

            
