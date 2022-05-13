from utils import client,todayTimestamp
from gql import gql
import json
from utils import TIMESTAMP_PER_YEAR
import pandas as pd
import numpy as np

AAVE_API="https://api.thegraph.com/subgraphs/name/aave/protocol-v2"

def lookDecimals(pool,symbol):
    return pool.loc[symbol]['decimals']

class Aave:
    def __init__(self,url=AAVE_API):
        self.client = client(url)

        query='''query{
            reserves(first: 1000) {
            id
        underlyingAsset
        symbol
        decimals
        }
        }'''
        cli = self.client
        response = cli.execute(gql(query))
        data=pd.json_normalize(response['reserves'])
        data=data.set_index('symbol')
        self.pools=data

    def currentUsers(self,symbol):
        cli=self.client
        reserveid=self.pools.loc[symbol]['id']
        decimals=self.pools.loc[symbol]['decimals']
        lastid=''
        query=''' query($reserve_id: ID!,$lastID:ID!){
            userReserves(first: 1000,
            where: {reserve: $reserve_id,user_gt:$lastID})
        {

            id
        reserve
        {
            id
        }
        user
        {
            id
        }
        currentATokenBalance
        currentStableDebt
        currentVariableDebt
        currentTotalDebt
        }
        }'''

        finished = 0
        agg = pd.DataFrame()
        while (finished == 0):
            try:
                params = {
                    "reserve_id": reserveid,
                    "lastID": lastid,
                }
                res = cli.execute(gql(query),variable_values=params)
                res=pd.json_normalize(res['userReserves']).drop('reserve.id',axis=1)
                # Rebase all the columns to decimals
                col_to_rebase=[col for col in res.columns if col[:3]=='cur']
                res[col_to_rebase]=res[col_to_rebase].astype('float')
                res[col_to_rebase]=res[col_to_rebase]/10**decimals
                if len(res) == 0:
                    return agg
                else:
                    res.index = res.id
                    lastid = res.iloc[-1].id  # .values
                    res = res.drop('id', axis=1)
                    res=res.set_index('user.id')
                    agg = agg.append(res)
            except:
                return agg
        return agg

    def userPosition(self,userids):
        """

        :param self:
        :param userids: list of userids we want to analyse
        :return:all the user positions broken down by token
        """
        lastid = ''
        cli=self.client
        finished=0
        agg = pd.DataFrame()
        query=''' query($user_list:[ID!]!,$lastID:ID!){
            userReserves(first: 1000, where: {user_in: $user_list,user_gt:$lastID})
        {

            id
        reserve
        {
            id
        symbol
        }
        user
        {
            id
        }
        currentATokenBalance
        scaledATokenBalance
        currentStableDebt
        currentVariableDebt
        currentTotalDebt
        }
        }'''
        while (finished == 0):
            try:
                params = {
                    "user_list": list(userids),
                    "lastID":lastid
                }
                res = cli.execute(gql(query), variable_values=params)
                res = pd.json_normalize(res['userReserves']).drop('reserve.id', axis=1)
                res['Decimals'] = res.apply(lambda x: lookDecimals(self.pools, x['reserve.symbol']),axis=1)
                # Rebase all the columns to decimals
                col_to_rebase = [col for col in res.columns if col[:3] == 'cur']
                for c in col_to_rebase:
                    res[c] = res[c].astype('float')
                    res[c] = res[c] / 10 ** res['Decimals']
                if len(res) == 0:
                    return agg
                else:
                    res.index = res.id
                    lastid = res.iloc[-1].id  # .values
                    res = res.drop('id', axis=1)
                    res = res.set_index('user.id')
                    agg = agg.append(res)
            except:
                return agg
        return agg

    def getPositions(self, ids, steps=100):
        """
        Need to cut the pool in small bits to retrieve all the information
        Args:
            ids: list of id we want to get all the positions
            steps: how many accounts to retrieve data simultaneously

        """
        nb_gp = int(np.ceil(len(ids) / steps))
        groups = [ids[(i) * steps:(i + 1) * steps] for i in range(0, nb_gp)]
        return pd.concat([self.userPosition(gp) for gp in groups])

    def getTransactions(self):
        cli = self.client
        query = '''{
          userTransactions (first: 1000,where:{timestamp_lte:1635793127,user:"0x0000006daea1723962647b7e189d311d757fb793"},orderBy:timestamp,orderDirection:asc){
            id
            pool {
              id
            }
            user {
              id
              borrowedReservesCount
            }
            timestamp
            __typename 
            ...on Deposit {
              pool {
                id
              }
              reserve {
                id
                symbol
                name
                decimals
              }
              amount
            }

          ...on RedeemUnderlying{
              pool {
                id
              }
              reserve {
                id
                symbol
                name
                decimals
              }
              amount
            }

        ...on Borrow{
              pool {
                id
              }
              reserve {
                id
                symbol
                name
                decimals
              }
              amount
            }
        ...on UsageAsCollateral{
              pool {
                id
              }
              reserve {
                id
                symbol
                name
                decimals
              }
              fromState
          		toState
            }   

        ...on Repay{
          pool {
            id
          }
          reserve {
            id
            symbol
            name
            decimals
          }
          amount
        }
          }
        } '''
        res = cli.execute(gql(query))
        return res

pool=Aave()
tr=pool.getTransactions()
res = pd.json_normalize(tr['userTransactions'])
res.to_csv('transactions_wintermute.csv')
print('l')
"""
userdata=pool.currentUsers('DAI')
pos=pool.userPosition(userdata.index[0:100])
print('finished')
"""