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
        
    def pair_manager(self , sorted_dict_bool):
        print(self.exchange, sorted_dict_bool)
        # if sorted_dict_bool:
            
        #     new_added_symbols = list(set(self.kucoin_top_pair_list) - set(self.current_symbol_dict[self.top_exchange])) if self.is_top else list(set(self.current_symbol_dict[self.top_exchange]) - set(self.kucoin_top_pair_list))
        #     remove_symbols = list(set(self.current_symbol_dict[self.top_exchange]) - set(self.kucoin_top_pair_list)) if self.is_top else list(set(self.kucoin_top_pair_list) - set(self.current_symbol_dict[self.top_exchange]))
            
            
        #     added_items = [f'{symbol}-USDT' for symbol in new_added_symbols if symbol in list(self.sorted_data[self.exchange].keys())]
        #     added_items = f"/market/snapshot:{','.join(added_items)}" if len(added_items) != 0 else None
            

        #     removed_items = [f'{symbol}-USDT' for symbol in remove_symbols if symbol in list(self.sorted_data[self.exchange].keys())]
        #     removed_items = f"/market/snapshot:{','.join(removed_items)}" if len(removed_items) != 0 else None

        #     self.current_symbol_dict[self.exchange] = self.kucoin_top_pair_list
        #     self.copy_top_pairs = self.current_symbol_dict[self.top_exchange]
                    
        #     methods = {"unsubscribe" : removed_items , "subscribe" : added_items}
        
        #     for method in methods:
        #         if methods[method] != None :
        #             subscribe_message = {
        #                 "type": method,
        #                 "topic": methods[method]
        #             }
        #             self.ws.send(json.dumps(subscribe_message))

        #             print(subscribe_message, self.exchange)

        #             if method == "unsubscribe":
        #                 print(remove_symbols, "remove_symbols", self.exchange)
        #                 for rm_symbol in remove_symbols:
        #                     if self.orderbook["KUCOIN"].get(rm_symbol):
        #                         del self.orderbook["KUCOIN"][rm_symbol]
                

    # catch errors
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