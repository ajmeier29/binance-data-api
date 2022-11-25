from flask import Flask
from pickle import BINBYTES
import pandas as pd
import logging
from binance.um_futures import UMFutures
from binance.lib.utils import config_logging
from binance.error import ClientError
import pandas as pd
import json
import datetime as dt
import numpy as np
from babel.numbers import format_currency
from flask_jsonpify import jsonpify
from flask import request

import os

app = Flask(__name__)

# Usage:
#   /?coin=<binance coin ticker>
#   /?coin=BTCUSDT
#   /?coin=MATICUSDT
@app.route('/')
def hello_world():
    key = os.environ['BINANCEKEY']
    secret = os.environ['BINANCESECRET']

    um_futures_client = UMFutures(key=key, secret=secret)

    orderId = 'OrderID'
    orderType = 'OrderType'
    symbol = 'Symbol'
    side = 'Side'
    price = 'Price'
    qty = 'Qty'
    commission = 'Commission'
    realizedPnl = 'RealizedPnl'
    total_pnl = 'TotalPnL'
    equity_curve = 'EquityCurve'
    coin_list = ['BTCUSDT', 'MATICUSDT', 'LRCUSDT', 'GALAUSDT']

    coin = request.args.get('coin')
    
    try:
        account_resp = um_futures_client.account(recvWindow=6000)
        unrealized_bal = account_resp['totalUnrealizedProfit']
        bal_resp = um_futures_client.balance(recvWindow=6000)
        response = um_futures_client.get_account_trades(symbol=coin, recvWindow=6000, limit= 1000)
        df = pd.DataFrame.from_records(response)
        # df.to_csv('/Users/andy/Nextcloud/Trading/Scripts/Binance/TradeImport/out.csv')
        # drop columns not needed
        df = df.drop(['marginAsset', 'commissionAsset', 'quoteQty', 'positionSide', 'buyer', 'maker'], axis=1)
        # arrange in order of google sheet
        df = df.rename(columns={'orderId': orderId, 'symbol': symbol, 'side': side, 'price': price, 'qty': qty, 'realizedPnl': realizedPnl, 'commission': commission})
        df = df[['id', orderId, symbol, side, price, qty, commission, 'time', realizedPnl]]
        df[realizedPnl] = df[realizedPnl].astype(float)
        df[commission] = df[commission].astype(float)
        df[qty] = df[qty].astype(float)
        df[price] = df[price].astype(float)
        # create new dataframe with avgeraged rows from orderId
        # final_df = df.groupby([orderId, symbol, side, 'time'], as_index=False)[realizedPnl].sum()
        binance_df = df.groupby([orderId], as_index=False).agg({price:'mean',qty:'sum',commission:'sum',realizedPnl:'sum'})

        # final_df = df.groupby(['time',orderId, symbol, side], as_index=False).agg({price:'mean',qty:'sum',commission:'sum',realizedPnl:'sum'})
        binance_df = df.groupby([orderId, symbol, side], as_index=False).agg({price:'mean',qty:'sum',commission:'sum',realizedPnl:'sum'})
        # final_df = df.groupby([orderId, symbol, side, 'time'], as_index=False)["commission", "realizedPnl"].apply(lambda x : x.sum())
        # convert int timestamp to date time.
        # final_df['time'] = final_df['time'].apply(lambda x : pd.to_datetime(x, utc=True, unit='ms'))
        # final_df['time'] = final_df['time'].astype(str)
        # drop rows that have no realized Pnl
        #final_df = final_df.drop(final_df[final_df.realizedPnl == 0].index)

        # update column order_type
        binance_df[orderType] = ""
        binance_df.loc[binance_df[realizedPnl] == 0, orderType] = 'Open'
        binance_df.loc[binance_df[realizedPnl] != 0, orderType] = 'Close'

        # make commision col a negative number
        binance_df[commission] = binance_df[commission].apply(lambda x: x * -1)
        # create total Pnl
        binance_df[total_pnl] = binance_df[commission] + binance_df[realizedPnl]
        # format cols to USD currency
        # binance_df[total_pnl] = binance_df[total_pnl].apply(lambda x: format_currency(x, currency="USD", locale="en_US"))
        # binance_df[commission] = binance_df[commission].apply(lambda x: format_currency(x, currency="USD", locale="en_US"))
        # binance_df[realizedPnl] = binance_df[realizedPnl].apply(lambda x: format_currency(x, currency="USD", locale="en_US"))
        # binance_df[price] = binance_df[price].apply(lambda x: format_currency(x, currency="USD", locale="en_US"))

        # rearange cols
        binance_df = binance_df[[orderId, orderType, symbol, side, price, qty, commission, realizedPnl, total_pnl]]
        binance_df[price] = binance_df[price].astype(float)
        binance_df[commission] = binance_df[commission].astype(float)
        binance_df[realizedPnl] = binance_df[realizedPnl].astype(float)
        binance_df[total_pnl] = binance_df[total_pnl].astype(float)
        logging.info(response)
        df_list = binance_df.values.tolist()
        JSONP_data = jsonpify(df_list)
        return JSONP_data
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )

# Program Start
# GetBinanceData()