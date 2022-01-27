"""
Library to extract all the relevant market data on all Curve pools

"""

import pandas as pd
import requests
import numpy as np
from pandas.core.dtypes.inference import is_number
from pycoingecko import CoinGeckoAPI
from config import COMPOUND_ADDRESSES

POOL_LIST=['compound','susd','saave','usdt','3pool','mim','cvxcrv','y']
DECIMALS=18



def format_decimals(df,col_names,decimals=DECIMALS):
    for c in col_names:
        df[c]=df[c].astype(float)/10**decimals
    return df

def get_first_data(darray):
    if type(darray) != list:
        return 0
    else:
        return darray[0]

def format_prices_volumes(hist_data):
    #hist_data=hist_data.fillna([0,0])
    volume_cols=[cols for cols in hist_data.columns if cols[:6]=='volume']
    price_cols=[cols for cols in hist_data.columns if cols[:6]=='prices']
    for vcol in volume_cols:
        hist_data[vcol]=hist_data[vcol].apply(get_first_data).astype('float')
    for pcol in price_cols:
        hist_data[pcol] = hist_data[pcol].apply(get_first_data).astype('float')
    return hist_data[volume_cols+price_cols]

def aggregate_volumes(hist_data,decimals=DECIMALS):
    cols = [cols.split('.')[1] for cols in hist_data.columns if cols[:6] == 'volume']
    volume_usd=pd.concat([hist_data['volume.'+col]/10**decimals*hist_data['prices.'+col] for col in cols],axis=1)
    return volume_usd.sum(axis=1)


def historical_pool_data(pool):
    """
    Returns historical data on a given pool
    Args:
        pool: name of pool we want to extract data
    """

    url='https://stats.curve.fi/raw-stats/'+pool+'-1440m.json'
    data = requests.get(url)
    data=pd.json_normalize(data.json())
    data.index=data['timestamp']
    data=data.drop('timestamp',axis=1)
    # Reformat by taking out the decimal
    data=format_decimals(data,['supply','virtual_price'])

    return data

def getGaugesWeights():
    """"
    Extract the Gauge rewards received for each pool
    Are they future ones or the current ones?
    """
    url = "https://api.curve.fi/api/getGauges"
    data = requests.get(url)
    data = pd.json_normalize(data.json(), max_level=2)
    gauge_summary = pd.DataFrame()
    for x in [col for col in data.columns if col[:11] == "data.gauges"]:
        dfx = pd.json_normalize(data[x])
        try:
            dfx = dfx[["name", "gauge_controller.gauge_relative_weight", "gauge_data.working_supply",
                       "gauge_data.inflation_rate"]]
            dfx[["gauge_controller.gauge_relative_weight", "gauge_data.working_supply",
                 "gauge_data.inflation_rate"]] = (dfx[
                ["gauge_controller.gauge_relative_weight", "gauge_data.working_supply",
                 "gauge_data.inflation_rate"]]).astype('float32')
            dfx[["gauge_controller.gauge_relative_weight", "gauge_data.working_supply", "gauge_data.inflation_rate"]] = \
            dfx[["gauge_controller.gauge_relative_weight", "gauge_data.working_supply",
                 "gauge_data.inflation_rate"]] / 10 ** DECIMALS
        except:
            dfx = dfx[["name", "gauge_controller.gauge_relative_weight"]]
            dfx["gauge_controller.gauge_relative_weight"] = dfx["gauge_controller.gauge_relative_weight"].astype(
                'float32')
            dfx["gauge_controller.gauge_relative_weight"] = dfx[
                                                                "gauge_controller.gauge_relative_weight"] / 10 ** DECIMALS

        dfx.index = dfx["name"]
        dfx = dfx.drop("name", axis=1)
        gauge_summary = gauge_summary.append(dfx)
    return gauge_summary

def getAPY():
    # apys without gauge
    url = "https://stats.curve.fi/raw-stats/apys.json"
    data = requests.get(url)
    data = pd.json_normalize(data.json())
    pools= set([cols.split('.')[-1] for cols in data.columns if cols[:3] == "apy"])
    dfapy = pd.DataFrame()
    fields = ['Day', 'Week', 'Month', 'Total']
    for p in pools:
        dpool = pd.DataFrame(index=[p], columns=fields)
        for field in fields:
            dpool.at[p, field] = float(data['apy.'+field.lower()+'.' + p])
        dfapy = dfapy.append(dpool)
    return dfapy


def getAPY2():
    """
    Second version of getting all PAy for each pool
    This one gets the the split between Curve and trading fees
    Returns weekly returns for trading fees or APY.
    """
    url = "https://api.curve.fi/api/getApys"
    data = requests.get(url)
    data = pd.json_normalize(data.json())
    for x in data.columns:
      if x.split('.')[-1]=='baseApy':
        data[x]=data[x].astype('float')
    pools = set([cols.split('.')[1] for cols in data.columns if cols[:4] == "data"])
    pools.remove('generatedTimeMs')
    dfapy = pd.DataFrame()
    fields = ['baseApy', 'crvApy', 'crvBoost', 'additionalRewards']
    for p in pools:
        dpool = pd.DataFrame(index=[p], columns=fields)
        for field in fields:
            if is_number(data['data.' + p+'.'+field].loc[0]):
              dpool.at[p, field] = float(data['data.' + p+'.'+field].loc[0])
            else:
              dpool.at[p, field] = 0
        dfapy = dfapy.append(dpool)
    return dfapy

def getTokenPrice(coingecko_id):
    """

    :param coingecko_id: ids of token we want to get price
    :return: returns
    """
    # create a connection

    cg = CoinGeckoAPI()
    prices=cg.get_price(ids=coingecko_id, vs_currencies='usd')
    return pd.DataFrame.from_records(prices)

def getHistPrice(coingecko_id,start_date,end_date):
    """

    :param coingecko_id: ids of token we want to get price
    :return: returns
    """
    # create a connection

    cg = CoinGeckoAPI()
    px = cg.get_coin_market_chart_range_by_id(coingecko_id, 'usd', start_date, end_date)
    px = pd.DataFrame(px['prices'])
    px.index = px[0]
    px = px.drop(0, axis=1)
    px.columns = [coingecko_id]
    return px

def getHistTokensPrice(coingecko_id,start_date,end_date):
    """

    :param coingecko_id: ids of token we want to get price
    :return: returns
    """
    # create a connection

    px=pd.concat([getHistPrice(x,start_date,end_date) for x in coingecko_id],axis=1)
    return px

def compoundRate(symbol,start_date,end_date,nbuckets=367):
    """
    Extract the compound lending rate for one currency

    :param symbol: symbol of the market we are interested (USDC,DAI,USDT,ETH)
        start_date: block date to start retrieving data,
        end_date: block date to stop data
    :return: returns
    """

    field_list=['borrow_rates', 'supply_rates', 'exchange_rates', 'prices_usd','total_borrows_history', \
                'total_supply_history'
     ]

    def cleanData(df,field):
        df1=pd.json_normalize(df[field][0], max_level=2)
        df1 = df1.set_index('block_timestamp').drop('block_number',axis=1)
        df1.columns=[field]
        return df1

    try:
        address=COMPOUND_ADDRESSES[symbol]
        url="https://api.compound.finance/api/v2/market_history/graph?asset="+str(address)+\
            '&min_block_timestamp='+str(start_date)+'&max_block_timestamp='+str(end_date)+\
            "&num_buckets="+str(nbuckets)+"&network=mainnet"
        data = requests.get(url)
        data = pd.json_normalize(data.json(),max_level=2)
    except:
        raise ValueError(symbol + " not supported")

    aggregate_data=pd.concat([cleanData(data,f) for f in field_list],axis=1)
    aggregate_data=aggregate_data.astype('float32')
    aggregate_data['Supply_USD']=aggregate_data['total_supply_history']*aggregate_data['exchange_rates']
    aggregate_data['utilization'] = aggregate_data['total_borrows_history'] / aggregate_data['Supply_USD']

    # supply multiply by exchange rate= USD supplied
    #borrow history already in usd

    return aggregate_data
