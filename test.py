def lookDecimals(pool,symbol):
    return pool.loc[symbol]['decimals']

class Aave:
    def __init__(self,url=AAVE_API):
        self.client = client(url)

        query='''query{
            reserves(first: 1000,subgraphError: allow) {
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

    def getLenders(self, reserveid):
        query='''query($reserveID:ID!,$lastID:ID!) {
        userReserves(
        first: 1000
        where: {reserve_contains: $reserveID, id_gt:$lastID}
        orderBy: currentATokenBalance
        orderDirection: desc
        ) {
        reserve {
        id
        name
        symbol
        aToken {
          id
          pool {
            id
            lendingPool
          }
        }
      }
      currentATokenBalance
      currentStableDebt
      currentTotalDebt
      user {
        id
      }
    }
    }'''
        cli=self.client
        lastid=0
        finished = 0
        agg = pd.DataFrame()
        while (finished == 0):
            try:
                params = {
                    "reserveID": reserveid,
                    "lastID":lastid
                                  }
                res = cli.execute(gql(query),variable_values=params)
                res=pd.json_normalize(res['userReserves'])
                if (len(res) == 0):
                    return agg
                agg=agg.append(res)
                lastid=res.iloc[-1]['id']
            except:
                return agg