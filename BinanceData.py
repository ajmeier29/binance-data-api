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

#------------------------------------------------------------------------
# Binance API Section
#------------------------------------------------------------------------

def GetBinanceData():
    key = ''
    secret = ''

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
    current_coin = 0
    try:
        account_resp = um_futures_client.account(recvWindow=6000)
        unrealized_bal = account_resp['totalUnrealizedProfit']
        bal_resp = um_futures_client.balance(recvWindow=6000)
        response = um_futures_client.get_account_trades(symbol=coin_list[current_coin], recvWindow=6000, limit= 1000)
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

        return binance_df
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )

# Program Start
GetBinanceData()

    #------------------------------------------------------------------------
    # End Binance API Section
    #------------------------------------------------------------------------

    #------------------------------------------------------------------------
    # Google Sheets Section
    #------------------------------------------------------------------------
    # # define the scope
    # scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

    # # add credentials to the account
    # creds = ServiceAccountCredentials.from_json_keyfile_name('binance-google-sheets-user.json', scope)

    # # authorize the clientsheet
    # client = gspread.authorize(creds)

    # # get the instance of the Spreadsheet
    # sheet = client.open('Binance-Test')

    # # get the first sheet of the Spreadsheet
    # sheet_instance = sheet.get_worksheet(1)

    # records_data = sheet_instance.get_all_values()

    # # convert the json to dataframe
    # records_df = pd.DataFrame.from_dict(records_data)

    # records_df = records_df.dropna()
    # # drop first row
    # records_df = records_df.iloc[1: , :]
    # records_df = records_df.reset_index(drop=True)
    # #rename cols
    # records_df = records_df.rename(columns={0: 'Date', 1: orderId, 2: orderType, 3: symbol, 4: side, 5: price, 6: qty, 7: commission, 8: realizedPnl, 9: total_pnl, 10: equity_curve})
    # # remove $ and convert to float
    # #records_df[total_pnl] = records_df[total_pnl].str.replace(r'\\$', '')

    # records_df[price] = records_df[price].str.replace('$', '')
    # records_df[commission] = records_df[commission].str.replace('$', '')
    # records_df[realizedPnl] = records_df[realizedPnl].str.replace('$', '')
    # records_df[total_pnl] = records_df[total_pnl].str.replace('$', '')
    # records_df[equity_curve] = records_df[equity_curve].str.replace('$', '')

    # records_df[price] = records_df[price].str.replace(',', '')
    # records_df[commission] = records_df[commission].str.replace(',', '')
    # records_df[realizedPnl] = records_df[realizedPnl].str.replace(',', '')
    # records_df[total_pnl] = records_df[total_pnl].str.replace(',', '')
    # records_df[equity_curve] = records_df[equity_curve].str.replace(',', '')

    # records_df[price] = records_df[price].astype(float)
    # records_df[commission] = records_df[commission].astype(float)
    # records_df[realizedPnl] = records_df[realizedPnl].astype(float)
    # records_df[total_pnl] = records_df[total_pnl].astype(float)
    # records_df[equity_curve] = records_df[equity_curve].astype(float)

    # data = {'ImportDate': [],
    #          orderId: [],
    # 	     orderType: [],
    # 	     symbol: [],
    # 	     side: [],
    #          price: [],
    #          qty: [],
    #          commission: [],
    #          realizedPnl: [],
    #          total_pnl: []}#,
    #         #equity_curve: []}
    # missing_rows_df = pd.DataFrame(data)

    # missing_idx = 0

    # for ind in binance_df.index:
    #     current_order_id = str(binance_df[orderId][ind])
    #     found_index = records_df.index[records_df[orderId] == current_order_id].tolist()

    #     # current_equity = 0
    #     # previous_equity = 0

    #     # if none is found, add to new df
    #     if not found_index:

    #         # ------------------------------- DEPRICATED EQUITY CURVE DATA ------------------------------- #
    #         # check if the equity curve col is empty, if so, it needs to be the value of the totalpnl
    #         # if ind == 0: # and pd.isnull(records_df.loc[ind, total_pnl]):
    #         #     # set equity_curve to total_pnl
    #         #     current_equity = binance_df[total_pnl][ind]
    #         #     previous_equity = current_equity
    #         # else:
    #         #     # if pd.isnull(records_df.loc[ind, equity_curve]):
    #         #     #     current_equity = binance_df[total_pnl][ind] + previous_equity
    #         #     # else:
    #         #     if missing_rows_df.empty:
    #         #         b = binance_df[total_pnl][ind]
    #         #         r = records_df[equity_curve][ind]
    #         #         r2 = records_df[equity_curve][ind-1]
    #         #         current_equity = binance_df[total_pnl][ind] + records_df[equity_curve][ind-1]
    #         #     else:
    #         #         # add last total_pnl to current total_pnl and set it to equity_curve
    #         #         current_equity = binance_df[total_pnl][ind] + missing_rows_df[equity_curve][missing_idx]
    #         #         missing_idx += 1
    #         # ------------------------------- DEPRICATED EQUITY CURVE DATA ------------------------------- #
    #         new_row = {'ImportDate': dt.datetime.today().strftime("%m/%d/%Y"), orderId: binance_df[orderId][ind], orderType:binance_df[orderType][ind], symbol: binance_df[symbol][ind], side: binance_df[side][ind], price:binance_df[price][ind], qty: binance_df[qty][ind], commission:binance_df[commission][ind], realizedPnl: binance_df[realizedPnl][ind], total_pnl: binance_df[total_pnl][ind]}#, equity_curve:current_equity}
    #         missing_rows_df = missing_rows_df.append(new_row, ignore_index=True)


    # # difference_df = records_df != binance_df

    # # sheets_output_df = records_df.compare(binance_df, keep_equal=True)
    # # gets the next emtpy row in the worksheet
    # def next_available_row(worksheet):
    #     str_list = list(filter(None, worksheet.col_values(1)))
    #     return len(str_list)+1

    # # add all missing values to google sheets
    # if len(missing_rows_df.index) != 0:
    #     firstemptyrow = len(sheet_instance.get_all_values()) + 1
    #     next_row = next_available_row(sheet_instance)
    #     sheet_instance.insert_rows(missing_rows_df.values.tolist(), row=next_row, inherit_from_before=True)

    # #------------------------------------------------------------------------
    # # End Google Sheets Section
    # #------------------------------------------------------------------------

    # print('eh')

