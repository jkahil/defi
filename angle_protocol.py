"""
Retrieving Data from Angle Protocol

Note Stock SLP is in $
StockUser is in Euro
Available assets in $
'totalHedgeAmount' is in Euro

"""
from utils import client,todayTimestamp
from gql import gql
from utils import TIMESTAMP_PER_YEAR
import pandas as pd
import numpy as np

SAVE_DIR="C:\\Users\\joelk\\Documents\\Notional\\Data\\"

ANGLE_API="https://api.thegraph.com/subgraphs/name/picodes/transaction"
TIMESTAMP_INI=1635724800 # when version was launched i.e. Nov 1st
DECIMALS_DEFAULT=18
class Angle:
    def __init__(self,url=ANGLE_API):
        self.client=client(url)

    def getPoolHist(self,collatname):
        """
        :param collatname: code of collateral either DAI, FRAX , USDC
        :return: list of pool data
        """
        params = {
            "currency_code": collatname,
            "tstart": TIMESTAMP_INI,
        }
        query = '''query($currency_code:ID!,$tstart:Int!){
                  poolHistoricalDatas(first: 1000,
                    orderBy: timestamp
                    orderDirection: asc
                    where: {collatName: $currency_code,timestamp_gt:$tstart}
                  ) {
                    availableAsset
                    apr
                    collatName
                    blockNumber
                    stockSLP
                    stockUser
                    decimals
                    targetHAHedge
                    totalAsset
                    totalHedgeAmount
                    timestamp
                  }
                }'''
        is_finished = 0
        df_trades = pd.DataFrame()
        while is_finished == 0:
            client = self.client
            response = client.execute(gql(query), variable_values=params)
            trade_data = pd.json_normalize(response['poolHistoricalDatas'])
            if len(trade_data) > 0:  # no more data to download
                max_timestamp = trade_data['timestamp'].max()
                params["tstart"] = int(max_timestamp)
                df_trades = df_trades.append(trade_data)
            else:
                is_finished = 1
        decimals=int(df_trades.iloc[0]['decimals'])
        adj_decimals=10**decimals
        #format the fields correctly
        for field in ['availableAsset','totalAsset']:
            df_trades[field]=df_trades[field].astype(float)
            df_trades[field]=df_trades[field]/adj_decimals
        for field in ['stockUser','totalHedgeAmount']:
            df_trades[field]=df_trades[field].astype('float64')
            df_trades[field]=df_trades[field]/(10**DECIMALS_DEFAULT)
        for field in ['stockSLP']:
            df_trades[field]=df_trades[field].astype('float64')
            df_trades[field]=df_trades[field]/(10**(DECIMALS_DEFAULT+decimals))


        return df_trades

u=Angle()
r=u.getPoolHist("USDC")
r.to_csv(SAVE_DIR+'usdc_angle_pool.csv')
r=u.getPoolHist("DAI")
r.to_csv(SAVE_DIR+'dai_angle_pool.csv')
print('jk')