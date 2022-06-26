import time
import requests
import pandas as pd
from prometheus_client import start_http_server, Gauge


class Binance():
    def __init__(self):
        self.api_url = 'https://api.binance.com'
        self.prom_gauge = Gauge('price_spread_delta',
                        'Price spread delta value of the symbols', ['symbol'])

    def get_top_symbols(self, asset, sort_by_field, symbols_top_count):
        """
        Return the top symbols over the last 24 hours
        """
        uri = "/api/v3/ticker/24hr"

        resp = requests.get(self.api_url + uri)
        df = pd.DataFrame(resp.json())
        df = df[df.symbol.str.endswith(asset)]
        df[sort_by_field] = pd.to_numeric(df[sort_by_field])
        df = df.sort_values(by=[sort_by_field], ascending=False).head(symbols_top_count)
        df = df[['symbol', sort_by_field]]

        return df


    def get_total_notional_value_list(self, symbols, top_n_notional):
        """
        Returns the total notional value of top 200 bids and asks
        """
        uri = "/api/v3/depth"
        total_notional_value_list = {}

        for s in symbols['symbol']:
            payload = {'symbol':s}
            resp = requests.get(self.api_url + uri, params=payload)
            for col in ["bids", "asks"]:
                df = pd.DataFrame(data=resp.json()[col], columns=["price", "quantity"], dtype=float)
                df['notional'] = df['price'] * df['quantity']
                df = df.sort_values(by=['notional'], ascending=False).head(top_n_notional)
                total_notional_value_list[s + '_' + col] = df['notional'].sum()

        return total_notional_value_list


    def get_price_spread_list(self, symbols):
        """
        Returns the price spread for each symbol
        """
        uri = '/api/v3/ticker/bookTicker'
        price_spread_list = {}

        for s in symbols['symbol']:
            payload = {'symbol':s}
            resp = requests.get(self.api_url + uri, params=payload)
            price_spread = float(resp.json()['askPrice']) - float(resp.json()['bidPrice'])
            price_spread_list[s] = price_spread

        return price_spread_list


    def get_price_spread_delta(self, symbols):
        """
        Prints price delta for each 10 seconds and exports as Prometheus metrics
        """
        price_spread_delta = {}
        previous_price_spread_data = self.get_price_spread_list(symbols)
        time.sleep(10)
        new_price_spread_data = self.get_price_spread_list(symbols)

        for key, value in previous_price_spread_data.items():
            price_spread_delta[key] = abs(new_price_spread_data[key] - value)
        for key, value in price_spread_delta.items():
            self.prom_gauge.labels(key).set(value)

        print('Price delta from the previous value for target symbols')
        print(price_spread_delta)
        print()


def main():
    binance = Binance()
    start_http_server(8081)

    btc_asset_top_symbols = binance.get_top_symbols(asset='BTC', sort_by_field='volume', symbols_top_count=5)
    print('Top 5 symbols with quote asset BTC by Volume')
    print(btc_asset_top_symbols)
    print()

    usdt_asset_top_symbols = binance.get_top_symbols(asset='USDT', sort_by_field='count', symbols_top_count=5)
    print('Top 5 symbols with quote asset USDT by Trade Number')
    print(usdt_asset_top_symbols)
    print()

    total_notional_value = binance.get_total_notional_value_list(symbols=btc_asset_top_symbols, top_n_notional=200)
    print('Total notional value of top 200 bids-and-asks of BTC by Volume')
    print(total_notional_value)
    print()

    usdt_price_spread_list = binance.get_price_spread_list(symbols=usdt_asset_top_symbols)
    print('Price spread for USDT by Trade Number')
    print(usdt_price_spread_list)
    print()

    # Print delta value of price spread and export as prometheus metrics
    while True:
        binance.get_price_spread_delta(symbols=usdt_asset_top_symbols)


if __name__ == "__main__":
    main()
