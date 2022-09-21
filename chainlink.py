"""
Download price data from Chainlink

"""
import time
from utils import client,todayTimestamp
from gql import gql
from utils import TIMESTAMP_PER_YEAR
import pandas as pd
import numpy as np

SAVE_DIR="C:\\Users\\joelk\\Documents\\Notional\\Data\\Prices"
START_DATE='1601924900'
CHAINLINK_API="https://api.thegraph.com/subgraphs/name/edoapp/chainlink-prices"

def getPrice(token,gclient,startdate=START_DATE,decimals=8,tokenbase='USD'):
    pair=str(token)+'/'+str(tokenbase)
    df_trades = pd.DataFrame()
    timestamp=round(time.time())
    while True:
        query = '''
        query {
          prices(first: 1000, where:{assetPair:''' + str('"') + str(pair) + str('"') + ''', timestamp_lt:''' + str(
            timestamp) + ''', timestamp_gt:'''+str(startdate)+'''}, orderBy: timestamp, orderDirection: desc,subgraphError: allow) {
            assetPair {
              id
            }
            timestamp
            price
          }
        }
        '''

        try:
            response = gclient.execute(gql(query))
        except:
            continue
        trade_data = pd.json_normalize(response['prices'])
        if len(trade_data)==0:
            break
        trade_data['price']=trade_data['price'].astype('float')
        trade_data['price']=trade_data['price']/10**decimals
        trade_data['timestamp']=trade_data['timestamp'].astype('int')

        df_trades=df_trades.append(trade_data)
        timestamp = df_trades.iloc[-1]['timestamp']
    return df_trades


class Chainlink:
    def __init__(self,url=CHAINLINK_API):
        self.client=client(url)
"""
c=Chainlink()
r=pd.concat(getPrice(token,c.client,tokenbase='USD') for token in ['stETH','BTC','ETH','USDC','DAI'])
print('u')
"""