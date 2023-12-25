import uvicorn
import asyncio
from binance import Binance
from getio import Getio
from wazirx import Wazirx
from kucoin import Kucoin
from mexc import Mexc
import threading
import requests
import socketio
import aiohttp
import time
import gzip
import json
import copy


sio = socketio.AsyncServer(async_mode="asgi" ,cors_allowed_origins='*')
app = socketio.ASGIApp(sio, static_files={
    "/": "./arb/public"
})

trigger_pr = 1.5
trigger_profit = 10
symboldata = {}
isExchangeSelected = False
active_threads = {}
orderbooks = {}
current_symbol_dict = {}
symbols_list = []
token = ""
client = ""
Exchanges = ["MEXC", "KUCOIN", "GATEIO"]
Exchange_index = 0
pair_with = "USDT"
Queue_jsondata = {}
interval = 1
prohibited_substring = ["DOWN", "UP", "3S", "2S", "3L", "2L", "QUICK", "MXC"]
trigger_symbol_list = []
current_symbol_list = []


exchange_detailes = {
    "BINANCE" : {
        "url": "https://api.binance.com/api/v3/exchangeInfo",
        "prefix": pair_with,
        "symbols_key": "symbols",
        "symbol_key": "symbol",
        "class" : Binance, 
        "websocket" :  f"wss://stream.binance.com:9443/ws"
    },
    "WAZIRX" : {
        "url": "https://api.wazirx.com/api/v2/tickers",
        "prefix": pair_with.lower(),
        "class" : Wazirx , 
        "websocket" : "wss://stream.wazirx.com/stream"
    },
    "GATEIO" : {
        "url": "https://api.gateio.ws/api2/1/pairs",
        "prefix": f"_{pair_with}",
        "class" : Getio,
        "websocket" : "wss://api.gateio.ws/ws/v4/",
        "top" : "https://api.gateio.ws/api/v4/spot/tickers",
        "top_path" : [],
        "volume" : "base_volume",
        "pr_change" : "change_percentage",
        "remove_prefix" : f"_{pair_with}",
        "retrive_symbol" : "currency_pair",
        "get_per" : 1

    },
    "MEXC" : {
        "url": "https://www.mexc.com/open/api/v2/market/symbols",
        "prefix": f"_{pair_with.upper()}",
        "symbols_key": "data",
        "symbol_key": "symbol",
        "class" : Mexc, 
        "top" : "https://api.mexc.com/api/v3/ticker/24hr",
        "websocket" : "wss://wbs.mexc.com/ws",
        "top_path" : [],
        "volume" : "volume",
        "pr_change" : "priceChangePercent",
        "remove_prefix" : pair_with.upper(),
        "retrive_symbol" : "symbol",
        "get_per" : 100
    },
    "KUCOIN" : {
        "url": "https://api.kucoin.com/api/v1/symbols",
        "prefix": f"-{pair_with.upper()}",
        "symbols_key": "data",
        "symbol_key": "symbol",
        "class" : Kucoin, 
        "websocket" : "",
        "top" : "https://api.kucoin.com/api/v1/market/allTickers",
        "top_path" : ["data", "ticker"],
        "volume" : "volValue",
        "pr_change" : "changeRate",
        "remove_prefix" : f"-{pair_with.upper()}",
        "retrive_symbol" : "symbol",
        "get_per" : 100
    },
}



async def sendMessege():
    global Queue_jsondata
    global client 
    
    if len(client) != 0:
        jsondata = gzip.compress(json.dumps(Queue_jsondata).encode())
        await sio.emit("result", jsondata, to=client)
        await asyncio.sleep(1)

def play_notification_sound():
    # Replace "notification_sound.wav" with the path to your sound file
    wave_obj = sa.WaveObject.from_wave_file("pcr_notify.wav")
    play_obj = wave_obj.play()
    play_obj.wait_done()     

def run_sendMessege():
    global interval
    global active_threads

    if len(active_threads) != 0 :
        asyncio.run(sendMessege())
    threading.Timer(interval, run_sendMessege).start()

class Mythread(threading.Thread):
    def __init__(self, lock, sid, coin = [], sorted_data= {}):
        threading.Thread.__init__(self)
        self.lock = lock
        self.symbol = coin
        self.sid = sid
        self.sorted_data = sorted_data
        global orderbooks
        global active_threads
        global Exchanges
        global Exchange_index
        global exchange_detailes
        global pair_with
        global token
        global current_symbol_dict
        global Queue_jsondata

        self.selected_exchange = Exchanges[Exchange_index]
        self.pair_with = pair_with
        self.orderbooks = orderbooks
        self.exchange_last_update = {}
        self.current_symbol_dict = current_symbol_dict
        self.Queue_jsondata = Queue_jsondata
        
        Exchanges =["MEXC", "KUCOIN", "GATEIO"] 
    
        for exchange in Exchanges:

            if exchange == "KUCOIN" :
                kucoinToken()
                exchange_detailes["KUCOIN"]["websocket"] = f"wss://ws-api.kucoin.com/endpoint?token={token}"

            class_arg = exchange_detailes.get(exchange)
            t1 = class_arg["class"](url = class_arg['websocket'], pair_with = self.pair_with , exchange = exchange.capitalize() , orderbooks = self.orderbooks, is_top =  (True if self.selected_exchange == exchange else False),  sorted_data =  self.sorted_data, current_symbol_dict = self.current_symbol_dict, top_exchange = self.selected_exchange)
            t1.daemon = True
            t1.start()
        
    async def get_address(self):
        host = "https://api.gateio.ws"
        prefix = "/api/v4"
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json'}

        url = '/wallet/currency_chains'
        query_param = f'currency={self.symbol.lower()}'
        r = await requests.request('GET', host + prefix + url +
                             "?" + query_param, headers=headers)
        chain_data = r.json()
        currency_info = {self.symbol: {}}
        for i in chain_data:
            currency_info[self.symbol][i['chain']] = {"Deposit": 'True' if i['is_deposit_disabled'] ==
                                                      0 else 'False', "Withdraw": 'True' if i['is_withdraw_disabled'] == 0 else 'False'}
        print(f"{currency_info}\n**********")

    async def calculate_price_delta(self, Exchanges):
        
        global Queue_jsondata
        global current_symbol_dict
        global trigger_symbol_list
        global current_symbol_list
        global trigger_pr

        calculated_data = {"orderbooks" : {}, "symbol_json_data" : {}, "trigger_list" : [], "current_symbol_list" : []}
        copy_orderbooks = copy.deepcopy(self.orderbooks)
        
        for symbol , data  in copy_orderbooks[self.selected_exchange].items():

            buy_dict = {}
            sell_dict = {}
            
            for exchange in copy_orderbooks:
               
                if copy_orderbooks[exchange].get(symbol):
                    
                    buy_dict[exchange] = float(copy_orderbooks[exchange][symbol]["data"]["buy"])
                    sell_dict[exchange] = float(copy_orderbooks[exchange][symbol]["data"]["sell"])
                    
            sorted_buy_dict = dict(sorted(buy_dict.items(), key=lambda item: item[1])) 
            sorted_sell_dict = dict(sorted(sell_dict.items(), key=lambda item: item[1] , reverse=True)) 
            
            if len(list(sorted_buy_dict.keys())) > 1 and len(list(sorted_sell_dict.keys())) > 1:

                buy_exchange ,buy = list(sorted_buy_dict.items())[0] 
                sell_exchange , sell = list(sorted_sell_dict.items())[0]

                # print(list(sorted_buy_dict.items()))
                percentage_calculate = round((((sell - buy )/buy)*100)-0.4 , 2)

                if buy_exchange != sell_exchange and sell > buy:
                    
                    trigger_symbol_list.append(symbol) if percentage_calculate > trigger_pr and not symbol in trigger_symbol_list else (trigger_symbol_list.remove(symbol) if percentage_calculate < trigger_pr and symbol in trigger_symbol_list else None)

                calculated_data["symbol_json_data"][symbol] = {"per" : percentage_calculate , "buy_exchange" : buy_exchange if sell > buy and buy_exchange != sell_exchange else None , "sell_exchange": sell_exchange if sell > buy and buy_exchange != sell_exchange else None, "trigger" : True if percentage_calculate > trigger_pr else False }
        


        for exchange in copy_orderbooks:
            for symbol in current_symbol_dict[exchange]:
                if not symbol in current_symbol_list :
                    current_symbol_list.append(symbol)

        calculated_data = {"symbol_json_data" :dict(sorted(calculated_data["symbol_json_data"].items(), key=lambda item: item[1]["per"] , reverse=True)) }
        calculated_data["orderbooks"] = self.orderbooks
        calculated_data["trigger_list"] = trigger_symbol_list
        calculated_data["current_symbol_list"] = current_symbol_list
        Queue_jsondata = calculated_data

    async def stop(self):
        self.stop_event = True

    def run (self):
        global Exchanges
        global Exchange_index


        selected_exchange = Exchanges[Exchange_index]

        while active_threads.get("current_thread"):   
    
            try:

                # symbol_in_exchanges = [exchange for exchange in Exchanges if self.orderbooks[exchange].get(self.symbol)]
             
                # symbol_in_exchange_dict = {}

                # for exchange in symbol_in_exchanges:
                #     symbol_in_exchange_dict[exchange] = self.orderbooks[exchange][self.symbol]

                # is_list_non_empty = all(bool(symbol_in_exchange_dict[key]["data"]) for key in symbol_in_exchange_dict)
                # is_update = any( True for key in symbol_in_exchange_dict if symbol_in_exchange_dict[key]["lastupdate"] >  self.exchange_last_update[key])

                # if is_list_non_empty and is_update:

                #     self.exchange_last_update = {key : symbol_in_exchange_dict[key]["lastupdate"] for key in self.exchange_last_update}
                    
                with self.lock:
                    t1 = threading.Thread(target=asyncio.run , args=(self.calculate_price_delta(Exchanges),), daemon=True)
                    t1.start()

                time.sleep(2)

            except Exception:
                pass

async def pairsRetrive(exchange_detailes, data):

    global Exchange_index
    global prohibited_substring
    global current_symbol_dict

    pair_dict = {}
    selected_exchange = data[Exchange_index]
    final_list = []

    data = ["KUCOIN", "MEXC" , "GATEIO"]

    for i in data:
        async with aiohttp.ClientSession() as session:
            async with session.get(exchange_detailes[i]["top"]) as response:
                symbols_list = await response.json()

                for key in  exchange_detailes[i]["top_path"]:    
                    symbols_list = symbols_list.get(key)
                  
                pair_dict[i] = {symbol_pair[exchange_detailes[i]["retrive_symbol"]].replace(exchange_detailes[i]["remove_prefix"], "") : {"volume" : round(float(symbol_pair[exchange_detailes[i]["volume"]]), 2) , "per_change" : round(float(symbol_pair[exchange_detailes[i]["pr_change"]]) * exchange_detailes[i]["get_per"], 2)} for symbol_pair in symbols_list  if exchange_detailes[i]["remove_prefix"] in symbol_pair[exchange_detailes[i]["retrive_symbol"]] and not  any(item in symbol_pair[exchange_detailes[i]["retrive_symbol"]] for item in prohibited_substring)}



    sorted_data = {
        exchange: dict(sorted(coins.items(), key=lambda item: item[1]["per_change"], reverse=True))
        for exchange, coins in pair_dict.items()
    }


    for item in sorted_data[selected_exchange]:

        lists_to_compare = list(sorted_data.keys())
        lists_to_compare.remove(selected_exchange)

        if any(item in sorted_data[sublist] for sublist in lists_to_compare):
            final_list.append(item)

    top_10 = final_list[:10]
    for item in top_10:
        for exchange in sorted_data:
           if item in sorted_data[exchange]:
               current_symbol_dict[exchange].append(item)

    return top_10 ,  sorted_data

def symbolDetails():
    url = "https://x.wazirx.com/api/v3/market-status"

    response = requests.get(url=url)
    res = response.json()
    assets = res.get("assets")
    symbol_details = {}
    for symbol in assets:
        all_addreses = []
        if symbol.get("networkList"):
            for address in symbol["networkList"]:
                all_addreses.append(address["shortName"])
        symbol_details[(symbol["type"]).upper()] = {"deposite" : symbol["deposit"], "withdrawal" : symbol["withdrawal"], "all_addreses" : all_addreses}

    return symbol_details

def kucoinToken():
    token_generate = requests.post(
    "https://api.kucoin.com/api/v1/bullet-public")
    global token
    token = (token_generate.json())["data"]["token"]

async def task(sid, symbol_list = [], add_symbol = True):
    lock = threading.Lock()

    global pair_with
    global Exchange_index
    global Exchanges
    global symbols_list
    global orderbooks
    global active_threads
    global exchange_detailes
    global symboldata
    global current_symbol_dict
    


    orderbooks = {item: {} for item in Exchanges}

    current_symbol_dict  = {exchange: [] for exchange in Exchanges}
    top_10, sorted_data =  await pairsRetrive(exchange_detailes, Exchanges)

 

    await sio.emit("symbol_list" , top_10, to=sid)
    
    run_sendMessege()

    t2 = Mythread(lock, sid, coin=current_symbol_dict, sorted_data=sorted_data)
    active_threads["current_thread"] = t2
    t2.start()
    await asyncio.sleep(0.3)

@sio.event
async def connect(sid, environ):
    if len(sid) != 0:
        print(sid, 'connected')
        global client
        global Exchanges

        client = sid
        
        await sio.emit("exchange_data", Exchanges)

@sio.event
async def readyToConnect(sid, data):
    global Exchange_index
    global active_threads
    global orderbooks
    global Queue_jsondata
    global isExchangeSelected
    global symboldata

    isExchangeSelected = True

    Exchange_index = int(data)


    if len(active_threads) != 0 and Exchange_index != int(data):
        Queue_jsondata = {}
        active_threads = {}

        thread = threading.Thread(target = asyncio.run, args = (task(sid), ))
        thread.daemon = True
        thread.start()
        

    elif len(active_threads) == 0:
        thread = threading.Thread(target = asyncio.run, args = (task(sid), ))
        thread.daemon = True
        thread.start()
        


@sio.event
async def clientMessage(sid, data):
    print('Received message from client:', data)
    global symbols_both_list
    global active_threads
    global isExchangeSelected
    global current_symbol_dict
    global trigger_pr

    if isExchangeSelected:
        if "%" in data : 
            trigger_pr = float(data.replace("%" , ""))
        # else:
        #     coin = data.upper()
        #     if coin in symbols_both_list and not active_threads.get(coin):
        #         # await sio.emit("text_symbol" , coin , to=sid)           
        #         lock = threading.Lock()
        #         t1 = Mythread(lock, sid, coin = [coin])
        #         active_threads[coin] = t1
        #         t1.start()
        #         current_symbol_dict["all_exchange"].append(coin)
        #         await sio.emit("text_symbol",coin, to=sid)
        #         await asyncio.sleep(0.5)

@sio.event
async def selectedSymbolList(sid, data):
    global active_threads
    global isExchangeSelected
    global current_symbol_dict


    if isExchangeSelected:
        for symbol in data:
            if not active_threads.get(symbol):
                lock = threading.Lock()
                t1 = Mythread(lock, sid, coin=[symbol])
                active_threads[symbol] = t1
                t1.start()
                current_symbol_dict["all_exchange"].append(symbol)
                await asyncio.sleep(0.3)
        del_symbol_list = list(filter(lambda symbol :  symbol if symbol not in  data else None,list(active_threads.keys())))
        
        if len(del_symbol_list) != 0:
            await delsymbol(sid, del_symbol_list)

@sio.event
async def delsymbol(sid, data):
    global active_threads
    global Queue_jsondata
    del_symbol_list = []
    for symbol in data:
        del_symbol = symbol.replace(f"/{pair_with}","")
        if active_threads.get(del_symbol):
            if active_threads.get(del_symbol):
                del active_threads[del_symbol]
            if Queue_jsondata.get(del_symbol):
                del Queue_jsondata[del_symbol]

            del_symbol_list.append(del_symbol)
    await sio.emit("delsymbol", del_symbol_list)

@sio.event
async def symbolPair(sid, data):
    global active_threads
    global pair_with
    global Queue_jsondata

    pair_with = data
    symbol_list = [key for key in active_threads if key != pair_with]

    await sio.emit("delsymbol", symbol_list)

    active_threads = {}
    Queue_jsondata = {}

    thread = threading.Thread(target = asyncio.run, args = (task(sid, symbol_list = symbol_list), ))
    thread.start()


@sio.event
async def disconnect(sid):
    print(sid, 'disconnected')
    global active_threads
    global Queue_jsondata
    global trigger_symbol_list

    Queue_jsondata = {}
    active_threads = {}
    trigger_symbol_list =[]
    

# process websocket data

if __name__ == "__main__":
    uvicorn.run("main:app", port = 8080,log_level="info", host= "0.0.0.0", proxy_headers=True)