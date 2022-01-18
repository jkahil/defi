"""
TO ADD filter transactions for only Lend and Borrow remove Settle Debt Cash


"""


from utils import client,todayTimestamp
from gql import gql
from utils import TIMESTAMP_PER_YEAR
import pandas as pd
import numpy as np

NOTIONAL_API="https://api.thegraph.com/subgraphs/name/notional-finance/mainnet-v2"
TIMESTAMP_INI=1635724800 # when version was launched i.e. Nov 1st
CURRENCY_MAP={"ETH":"1","DAI":"2","USDC":"3","WBTC":"4"}
ADJ_FACTOR_1=10**8 #Most variables in Notional’s subgraph are 8 decimals (10^8). This includes the following variables: depositShares, balances, portfolio assets, asset Cash, fCash, TVL and many more.

def tradingFee(timestamp):
    """
    Returns the Annualized Trading fee at a given timestamp
    :param timestamp: unix time stamp
    :return: tarding fee
    """
    if timestamp<1640649600:
        return 0.005
    else:
        return 0.003

class Notional:
    def __init__(self,url=NOTIONAL_API):
        self.client=client(url)

    def getTrades(self,currency,timestart=TIMESTAMP_INI,timeend=todayTimestamp()):
        """
        Get all the trades for a currency
        :param timestart: timestamp to start downloading data
        :param timeend: end of timestamp
        :param currency: symbol of currency
        :return: list of trades with fields
        """
        currency_code=CURRENCY_MAP[currency]
        params = {
            "currency_code": currency_code,
            "tstart": timestart,
            "tend": timeend
        }

        query='''query($currency_code:ID!,$tstart:Int!,$tend:Int!){trades(first: 1000,where:{currency:$currency_code,timestamp_gt:$tstart,
                  timestamp_lte:$tend},orderBy: timestamp,orderDirection:asc)
        {
            netfCash
            netUnderlyingCash
            currency
                    {
            underlyingSymbol
                    }
            market
            {
            marketIndex
            }
            account
            {
            id
            }
            tradeType
            maturity
            timestamp
            id
            }
            }'''

        is_finished=0
        df_trades=pd.DataFrame()
        while is_finished==0:
            client=self.client
            response = client.execute(gql(query),variable_values=params)
            trade_data=pd.json_normalize(response['trades'])
            if len(trade_data)>0: # no more data to download
                max_timestamp=trade_data['timestamp'].max()
                params["tstart"]=int(max_timestamp)
                df_trades=df_trades.append(trade_data)
            else:
                is_finished=1
        # Convert and adjust columns with decimals
        df_trades[['netUnderlyingCash','netfCash']]=df_trades[['netUnderlyingCash','netfCash']].astype('float')/ADJ_FACTOR_1
        df_trades['maturity']=df_trades['maturity'].astype('int64')
        return df_trades

    def getHistoricalRates(self,currency,timestart=TIMESTAMP_INI,timeend=todayTimestamp()):
        trades=self.getTrades(currency,timestart=TIMESTAMP_INI,timeend=todayTimestamp())
        trades['time to expiry']=trades['maturity']-trades['timestamp']
        trades['Ann Fee']=trades['timestamp'].apply(tradingFee)
        trades['return']=abs(trades['netfCash']/trades['netUnderlyingCash'])-1
        #trades ['APY']=(1+trades['return'])**(TIMESTAMP_PER_YEAR/trades['time to expiry'])-1
        trades ['APY']=np.log(trades['return']+1)/(trades['time to expiry'] / TIMESTAMP_PER_YEAR)
        # adjust for trading fee
        trades['Market Rate']=np.where(trades['tradeType']=='Borrow',trades['APY']-trades['Ann Fee'],trades['APY']+trades['Ann Fee'])
        return trades


note=Notional()
tdata=note.getTrades("USDC")
rates=note.getHistoricalRates("USDC")
print('HOURRA')