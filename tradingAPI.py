import pandas as pd
import numpy as np
import datetime
import requests, hashlib, urllib, hmac
import logging

class TRADING_API:
    def __init__(self, key, secret, log_path) -> None:
        self.key = key
        self.secret = secret
        logging.basicConfig(filename= log_path, level= logging.DEBUG)
        pass

    def um_klines(
            self, 
            symbol: str, 
            interval: str, 
            limit: int, 
            start, 
            end
        ) -> pd.DataFrame:
        logging.debug("um_klines: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/klines"
        api_secret = self.secret
        api_key = self.key
        logging.debug("um_klines: stage: initializate request: {}".format(datetime.datetime.now()))
        error_bool = True
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                if start != None and end != None:
                    params = {  "symbol": symbol, 
                                "interval": interval, 
                                "limit": limit,
                                "startTime": start,
                                "endTime": end
                    }
                else:
                    params = {  "symbol": symbol, 
                                "interval": interval, 
                                "limit": limit
                    }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_klines: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.get(url= endpoint, params= params, headers= headers)
                response.json()[0]
                candels = pd.DataFrame(response.json())
                response.close()
                logging.debug("um_klines: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        candels = candels.rename(columns = {0: 'open_time', 1:'open', 2:'high', 3:'low', 4:'close', 5:'volume', 6: 'close_time'})
        candels = candels.drop([7,8,9,10,11], axis='columns')
        candels = candels.astype({'open':'float', 'high':'float','low':'float', 'close':'float','volume':'float'})
        candels["open_time"] = pd.to_datetime(candels['open_time'], unit= "ms", utc= True)
        candels["close_time"] = pd.to_datetime(candels['close_time'], unit= "ms", utc= True)
        logging.debug("um_klines: stage: end execution: {}".format(datetime.datetime.now()))
        return candels

    def um_mark_klines(self, 
                       symbol: str, 
                       interval: str, 
                       limit: int, 
                       start, 
                       end
        ) -> pd.DataFrame:
        logging.debug("um_mark_klines: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/markPriceKlines"
        api_secret = self.secret
        api_key = self.key
        logging.debug("um_mark_klines: stage: initializate request: {}".format(datetime.datetime.now()))
        error_bool = True
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                if start != None and end != None:
                    params = {  "symbol": symbol, 
                                "interval": interval, 
                                "limit": limit,
                                "startTime": start, 
                                "endTime": end
                    }
                else:
                    params = {  "symbol": symbol, 
                                "interval": interval, 
                                "limit": limit
                    }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_mark_klines: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.get(url= endpoint, params= params, headers= headers)
                response.json()[0]
                candels = pd.DataFrame(response.json())
                response.close()
                logging.debug("um_mark_klines: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        candels = candels.rename(columns = {0: 'open_time', 1:'open', 2:'high', 3:'low', 4:'close', 5:'volume', 6: 'close_time'})
        candels = candels.drop([7,8,9,10,11], axis='columns')
        candels = candels.astype({'open':'float', 'high':'float','low':'float', 'close':'float','volume':'float'})
        candels["open_time"] = pd.to_datetime(candels['open_time'], unit= "ms", utc= True)
        candels["close_time"] = pd.to_datetime(candels['close_time'], unit= "ms", utc= True)
        logging.debug("um_mark_klines: stage: end execution: {}".format(datetime.datetime.now()))
        return candels

    def um_funding(
            self, 
            symbol: str
        ) -> pd.DataFrame:
        logging.debug("um_funding: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/premiumIndex"
        api_secret = self.secret
        api_key = self.key
        logging.debug("um_funding: stage: initializate request: {}".format(datetime.datetime.now()))
        error_bool = True
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                params = {  
                    "symbol": symbol
                }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_funding: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.get(url= endpoint, params= params, headers= headers)
                data = response.json()
                data["symbol"]
                response.close()
                logging.debug("um_funding: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        funding_rate = pd.DataFrame([data["symbol"], float(data["lastFundingRate"]), float(data["interestRate"]), int(data["nextFundingTime"]), int(data["time"])]).T
        funding_rate = funding_rate.rename(columns={0: "symbol", 1: "lastFundingRate", 2: "interestRate", 3: "nextFundingTime", 4: "time"})
        funding_rate["time"], funding_rate["nextFundingTime"] = pd.to_datetime(funding_rate["time"], utc=True, unit="ms"), pd.to_datetime(funding_rate["nextFundingTime"], utc=True, unit="ms")
        logging.debug("um_funding: stage: end execution: {}".format(datetime.datetime.now()))
        return funding_rate

    def um_position(
            self, 
            symbol: str
        ) -> pd.DataFrame:
        logging.debug("um_position: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v2/positionRisk"
        api_secret = self.secret
        api_key = self.key
        error_bool = True
        logging.debug("um_position: stage: initializate request: {}".format(datetime.datetime.now()))
        while error_bool:
            try:
                utcnow = int(datetime.datetime.now().timestamp() * 1000)
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                params = {  
                    "symbol": symbol,
                    "recvWindow": 10000,
                    "timestamp": utcnow
                }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_position: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.get(endpoint, params= params, headers= headers)
                response.json()[0]["symbol"]
                position = pd.DataFrame(response.json())
                response.close()
                logging.debug("um_position: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        position = position.drop(columns=["maxNotionalValue", "isolatedMargin", "isAutoAddMargin", 
                                        "isolatedWallet", "markPrice", "marginType", "liquidationPrice"])
        position = position.astype({'entryPrice' : 'float', 'leverage' : 'float', 'unRealizedProfit': 'float', 'positionAmt': 'float' })
        position = position.loc[position['positionSide']!='BOTH']
        position['Sum_poss'] = abs(position['positionAmt'] * position['entryPrice'])
        position['PNL%'] = position['unRealizedProfit']/position['Sum_poss'] *100
        position["updateTime"] = pd.to_datetime(position['updateTime'], unit= "ms", utc= True)
        position = position.loc[position.last_valid_index()-1: position.last_valid_index()].reset_index().drop(columns="index")
        logging.debug("um_position: stage: end execution: {}".format(datetime.datetime.now()))
        return position

    def um_info(
            self, 
            symbol: str
        ) -> pd.DataFrame:
        logging.debug("um_info: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        api_key = self.key
        error_bool = True
        logging.debug("um_info: stage: initializate request: {}".format(datetime.datetime.now()))
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                logging.debug("um_info: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.get(url= endpoint, headers= headers)
                symbol_info = {}
                info = response.json()
                info["symbols"]
                logging.debug("um_info: stage: close request: {}".format(datetime.datetime.now()))
                for i in info["symbols"]:
                    if i["symbol"] == symbol:
                        symbol_info = i
                        break
                response.close()
            except KeyError:
                print(response.json())
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        logging.debug("um_info: stage: end execution: {}".format(datetime.datetime.now()))
        return symbol_info
   
    def um_open_orders(
            self, 
            symbol: str
        ) -> pd.DataFrame:
        logging.debug("um_open_orders: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/openOrders"
        api_secret = self.secret
        api_key = self.key
        logging.debug("um_open_orders: stage: initializate request: {}".format(datetime.datetime.now()))
        error_bool = True
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                params = {  
                    "symbol": symbol,
                    "timestamp": int(datetime.datetime.now().timestamp() * 1000)
                }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_open_orders: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.get(url= endpoint, params= params, headers= headers)
                if response.json() != []:
                    #print(response.json())
                    response.json()[0]["orderId"]
                open_orders = pd.DataFrame(response.json())
                response.close()
                logging.debug("um_open_orders: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False    
        logging.debug("um_open_orders: stage: end execution: {}".format(datetime.datetime.now()))
        return open_orders
   
    def um_modify_margin(
            self, 
            symbol: str, 
            type: int, 
            amt: float, 
            position: str
        ) -> None:
        logging.debug("um_modify_margin: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/positionMargin"
        api_secret = self.secret
        api_key = self.key
        logging.debug("um_modify_margin: stage: initializate request: {}".format(datetime.datetime.now()))
        error_bool = True
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                params = {  
                    "symbol": symbol,
                    "type": type,
                    "amount": amt, 
                    "positionSide": position,
                    "timestamp": int(datetime.datetime.now().timestamp() * 1000)
                }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_modify_margin: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.post(url= endpoint, params= params, headers= headers)
                if response.json()["msg"] == "Successfully modify position margin.":
                    error_bool = False
                else:
                    response.json()["0"]
                response.close()
                logging.debug("um_modify_margin: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False    
        logging.debug("um_open_orders: stage: end execution: {}".format(datetime.datetime.now()))
        return None

    def um_search_order(
            self, 
            symbol: str, 
            client_order_id: str
        ) -> pd.DataFrame:
        open_orders = self.um_open_orders(symbol= symbol)
        searched = open_orders
        if open_orders.empty == False:
            searched = open_orders[open_orders["clientOrderId"] == client_order_id]
        return searched
  
    def um_market_order(
            self, 
            symbol: str, 
            side: str, 
            qty: float, 
            position: str, 
            client_order_id: str
        ) -> None:
        logging.debug("um_market_order: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/order"
        api_secret = self.secret
        api_key = self.key
        error_bool = True
        l = float(self.um_info(symbol)["filters"][1]["stepSize"])
        qty_precision = 0
        while l != 1.0:
            l *= 10.0
            qty_precision += 1
        logging.debug("um_market_order: stage: initializate request: {}".format(datetime.datetime.now()))
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                params = {  
                    "symbol": symbol,
                    "quantity": np.round(qty, qty_precision),
                    "side": side,
                    "positionSide": position,
                    "type": "MARKET",
                    "newClientOrderId": client_order_id,
                    "timestamp": int(datetime.datetime.now().timestamp()*1000)
                }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_market_order: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.post(url= endpoint, params= params, headers= headers)
                response.json()["orderId"]
                response.close()
                logging.debug("um_market_order: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        logging.debug("um_market_order: stage: end execution: {}".format(datetime.datetime.now()))
        return None
    
    def um_limit_order(
            self, 
            symbol: str, 
            side: str, 
            price: float, 
            qty: float, 
            position: str,
            client_order_id: str
        ) -> None:
        logging.debug("um_limit_order: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/order"
        api_secret = self.secret
        api_key = self.key
        info = self.um_info(symbol)
        k = float(info["filters"][0]["tickSize"])
        l = float(info["filters"][1]["stepSize"])
        price_precision = 0
        while k != 1.0:
            k *= 10.0
            price_precision += 1
        qty_precision = 0
        while l != 1.0:
            l *= 10.0
            qty_precision += 1
        logging.debug("um_limit_order: stage: initializate request: {}".format(datetime.datetime.now()))
        error_bool = True
        while error_bool:
            try: 
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                params = {  
                    "symbol": symbol,
                    "quantity": np.round(qty, qty_precision),
                    "price": np.round(price, price_precision),
                    "side": side,
                    "positionSide": position,
                    "type": "LIMIT",
                    "timeInForce": "GTC",
                    "newClientOrderId": client_order_id,
                    "timestamp": int(datetime.datetime.now().timestamp()*1000)
                }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_limit_order: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.post(url= endpoint, params= params, headers= headers)
                response.json()["orderId"]
                response.close()
                logging.debug("um_limit_order: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        logging.debug("um_limit_order: stage: end execution: {}".format(datetime.datetime.now()))
        return None
   
    def um_stop_order(
            self, 
            symbol: str, 
            side: str, 
            price: float, 
            qty: float, 
            position: str, 
            client_order_id: str
        ) -> None:
        logging.debug("um_stop_order: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/order"
        api_secret = self.secret
        api_key = self.key
        info = self.um_info(symbol)
        k = float(info["filters"][0]["tickSize"])
        l = float(info["filters"][1]["stepSize"])
        price_precision = 0
        while k != 1.0:
            k *= 10.0
            price_precision += 1
        qty_precision = 0
        while l != 1.0:
            l *= 10.0
            qty_precision += 1
        logging.debug("um_stop_order: stage: initializate request: {}".format(datetime.datetime.now()))
        error_bool = True
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                params = {  
                    "symbol": symbol,
                    "quantity": np.round(qty, qty_precision),
                    "stopPrice": np.round(price, price_precision),
                    "side": side,
                    "positionSide": position,
                    "type": "STOP_MARKET",
                    "newClientOrderId": client_order_id,
                    "timestamp": int(datetime.datetime.now().timestamp()*1000)
                }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_stop_order: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.post(url= endpoint, params= params, headers= headers)
                response.json()["orderId"]
                response.close()
                logging.debug("um_stop_order: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        logging.debug("um_stop_order: stage: end execution: {}".format(datetime.datetime.now()))
        return None

    def um_take_order(
            self, 
            symbol: str, 
            side: str, 
            price: float, 
            qty: float, 
            position: str, 
            client_order_id: str
        ) -> None:
        logging.debug("um_take_order: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/order"
        api_secret = self.secret
        api_key = self.key
        info = self.um_info(symbol)
        k = float(info["filters"][0]["tickSize"])
        l = float(info["filters"][1]["stepSize"])
        price_precision = 0
        while k != 1.0:
            k *= 10.0
            price_precision += 1
        qty_precision = 0
        while l != 1.0:
            l *= 10.0
            qty_precision += 1
        logging.debug("um_take_order: stage: initializate request: {}".format(datetime.datetime.now()))
        error_bool = True
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                params = {  
                    "symbol": symbol,
                    "quantity": np.round(qty, qty_precision),
                    "stopPrice": np.round(price, price_precision),
                    "side": side,
                    "positionSide": position,
                    "type": "TAKE_PROFIT_MARKET",
                    "newClientOrderId": client_order_id,
                    "timestamp": int(datetime.datetime.now().timestamp()*1000)
                }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_take_order: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.post(url= endpoint, params= params, headers= headers)
                response.json()["orderId"]
                response.close()
                logging.debug("um_take_order: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        logging.debug("um_take_order: stage: end execution: {}".format(datetime.datetime.now()))
        return None

    def um_cancel_order(
            self, 
            symbol: str, 
            order_id: int
        ) -> None:
        logging.debug("um_cancel_order: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/order"
        api_secret = self.secret
        api_key = self.key
        logging.debug("um_cancel_order: stage: initializate request: {}".format(datetime.datetime.now()))
        error_bool = True
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                params = {  
                    "symbol": symbol,
                    "orderId": order_id,
                    "timestamp": int(datetime.datetime.now().timestamp()*1000)
                }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_cancel_order: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.delete(url= endpoint, params= params, headers= headers)
                response.json()["orderId"]
                response.close()
                logging.debug("um_cancel_order: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        logging.debug("um_cancel_order: stage: end execution: {}".format(datetime.datetime.now()))
        return None
   
    def um_cancel_all(
            self, 
            symbol: str
        ) -> None:
        logging.debug("um_cancel_all: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/allOpenOrders"
        api_secret = self.secret
        api_key = self.key
        logging.debug("um_cancel_all: stage: initializate request: {}".format(datetime.datetime.now()))
        error_bool = True
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                params = {  
                    "symbol": symbol,
                    "timestamp": int(datetime.datetime.now().timestamp()*1000)
                }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_cancel_all: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.delete(url= endpoint, params= params, headers= headers)
                if response.json()["msg"] == "The operation of cancel all open order is done.":
                    error_bool == False
                else:
                    response.json["0"] 
                response.close()
                logging.debug("um_cancel_all: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: args: {}, errno: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        logging.debug("um_cancel_all: stage: end execution: {}".format(datetime.datetime.now()))
        return None

    def um_modify_order(self,
                        client_order_id: str,
                        symbol: str,
                        side: str,
                        qty: float,
                        price: float
        ) -> None:
        logging.debug("um_modify_order: stage: start execution: {}".format(datetime.datetime.now()))
        endpoint = "https://fapi.binance.com/fapi/v1/order"
        api_secret = self.secret
        api_key = self.key
        info = self.um_info(symbol)
        k = float(info["filters"][0]["tickSize"])
        l = float(info["filters"][1]["stepSize"])
        price_precision = 0
        while k != 1.0:
            k *= 10.0
            price_precision += 1
        qty_precision = 0
        while l != 1.0:
            l *= 10.0
            qty_precision += 1
        logging.debug("um_modify_order: stage: initializate request: {}".format(datetime.datetime.now()))
        error_bool = True
        while error_bool:
            try:
                headers = {
                    "X-MBX-APIKEY": api_key
                }
                params = {  
                    "symbol": symbol,
                    "quantity": np.round(qty, qty_precision),
                    "price": np.round(price, price_precision),
                    "side": side,
                    "origClientOrderId": client_order_id,
                    "timestamp": int(datetime.datetime.now().timestamp()*1000)
                }
                query_string = urllib.parse.urlencode(params)
                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                params["signature"] = signature
                logging.debug("um_modify_order: stage: execute request: {}".format(datetime.datetime.now()))
                response = requests.put(url= endpoint, params= params, headers= headers)
                response.json()["clientOrderId"]
                response.close()
                logging.debug("um_modify_order: stage: close request: {}".format(datetime.datetime.now()))
            except KeyError:
                logging.debug("ClientExceptionFound: code: {}, msg: {}".format(response.json()["code"], response.json()["msg"]))
                continue
            except requests.exceptions.Timeout as error:
                logging.debug("TimeoutErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                continue
            except requests.exceptions.RequestException as error:
                logging.debug("RequestExceptionFound: response: {}".format(error.response))
                continue
            except OSError as error:
                logging.debug("OSErrorFound: code: {}, msg: {}".format(error.errno, error.strerror))
                break
            except KeyboardInterrupt:
                break
            else:
                error_bool = False
        logging.debug("um_modify_order: stage: end execution: {}".format(datetime.datetime.now()))
        return None
