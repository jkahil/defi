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
ADJ_FACTOR_2=10**18
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
    def getAccounts(self,account_types='all'):
        """
        :param account_types: either 'all' for all accounts,'lend' for lenders only, 'borrow'
        :return: accounts holdings with borrows
        """
        sid=0
        params = {
            "account_id": sid,
        }
        if account_types=='all':
            add_on=''
        else:
            if account_types=='lend':
                add_on= 'hasPortfolioAssetDebt: false'
            else:
                if account_types=='borrow':
                    add_on = 'hasPortfolioAssetDebt: true'
                else:
                    ValueError('account type needs to be either all,lend,borrow')
        query='''
         query($account_id:ID!){
            accounts(
            first: 1000,
            orderBy: id
            orderDirection: asc
            where: {id_gt:$account_id,'''+add_on+'''}
          ) {
            id
            hasCashDebt
            hasPortfolioAssetDebt
            portfolio {
              maturity
              notional
              id
              assetType
              currency {
                symbol
              }
            }
            balances {
              assetCashBalance
              nTokenBalance
              accountIncentiveDebt
              id
              currency {
                symbol
              }
            }
          }
        }
        
        '''
        is_finished=0
        df_account=pd.DataFrame()
        while is_finished==0:
            client=self.client
            response = client.execute(gql(query),variable_values=params)
            account_data=pd.json_normalize(response['accounts'])
            if len(account_data)>0: # no more data to download
                max_id=account_data['id'].max()
                params["account_id"]=max_id
                df_account=df_account.append(account_data)
            else:
                is_finished=1
        return df_account


def extractField(dport,field):
    """
    Extracts portfolio for a portfolio field
    :param dport: portfolio nested field
    field either portfolio or balances field we want to unpack
    :return:
    """
    dp=pd.json_normalize(dport[field])
    df_ini=pd.DataFrame()
    for col in dp.columns:
        df_ini2=pd.DataFrame(index=dport['id'])
        pt=pd.json_normalize(pd.json_normalize(dport[field])[col])
        pt.index=dport['id']
        df_ini=df_ini.append(pt)
    return df_ini.dropna(how='all')






"""
u=Notional()
da=u.getAccounts('borrow')
px=u.getAssetPrices()
portfolio=extractField(da,'portfolio')
balance=extractField(da,'balances')
print('ll')
"""