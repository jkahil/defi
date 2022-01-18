from utils import client,todayTimestamp
from gql import gql
from utils import TIMESTAMP_PER_YEAR
import pandas as pd
import numpy as np

AAVE_API="https://api.thegraph.com/subgraphs/name/aave/protocol-v2"

class Aave:
    def __init__(self,url=AAVE_API):
        self.client = client(url)

        query='''query{
            reserves(first: 1000) {
            id
        underlyingAsset
        symbol
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
                res=pd.json_normalize(res['userReserves'])
                if len(res) == 0:
                    return agg
                else:
                    res.index = res.id
                    lastid = res.iloc[-1].id  # .values
                    print(lastid)
                    res = res.drop('id', axis=1)
                    agg = agg.append(res)
            except:
                return agg
        return agg

pool=Aave()
userdata=pool.currentUsers('DAI')
print('finished')