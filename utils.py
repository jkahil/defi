from gql import gql, Client
from datetime import datetime
from gql.transport.requests import RequestsHTTPTransport
from config import ETHER_KEY
import pandas as pd
import numpy as np
import etherscan
import time

TIMESTAMP_PER_YEAR=86400*360

def todayTimestamp():
    ts = int(time.time())
    return (ts)

def timestampToDate(ts):
    """

    :param ts: unix time stamp
    :return: date
    """
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

def client(api_url):
    """
    Initializes a connection to subgraph
    :param api_url: subgraph url for connection
    is_secure: do we need to use https
    :return:
    """
    sample_transport = RequestsHTTPTransport(
        url=api_url,
        headers= {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'},
        verify=True,
        retries=10,
    )
    client = Client(
        transport=sample_transport,
    fetch_schema_from_transport = True
    )
    return client


def etherscanClient():
    """
    Initializes a connection to Etherscan
    :return: client connection
    """
    es = etherscan.Client(
        api_key=ETHER_KEY,
        cache_expire_after=5,
    )
    return es

def getEthTransactions(client,address,start_block):
    """

    :param client:
    :param address:
    :param start_block:
    :return:
    """
    download=1
    start_block=0
    total_tx=pd.DataFrame()
    while download==1:
        transactions = client.get_transactions_by_address(address,start_block=start_block,page=1)
        transactions=pd.DataFrame.from_dict(transactions)
        total_tx=total_tx.append(transactions)
        if len (transactions)==0:
            download=0
        else:
            start_block=(transactions['block_number'].astype('int').max())+1

    return transactions

def getERC20HistTx(cli,address,erc_address=None):
    """
    Returns the historic ERC20 balance of an address
    :param cli: etherscan_client
    :param address: wallet address we want to analyse
    :param erc_address: token we want to get if not all
    :return:
    """
    download = 1
    start_block=0
    total_tx = pd.DataFrame()
    address=address.lower()# put lower case to be able to compare
    def checkAddress(add,add1):
        if add==add1:
            return 1
        else:
            return -1

    while download == 1:

        tx=cli.get_token_transactions(
                address=address,
            contract_address=erc_address,page=1,start_block=start_block
            )
        transactions = pd.DataFrame.from_dict(tx)
        total_tx = total_tx.append(transactions)
        if len (transactions)==0:
            download=0
        else:
            start_block=(transactions['block_number'].astype('int').max())+1
    total_tx['token_decimal']=total_tx['token_decimal'].astype('int64')
    total_tx['from'] ==total_tx['from'].astype('str')
    total_tx['to'] == total_tx['to'].astype('str')
    total_tx['value']=total_tx['value'].astype('float')
    total_tx['Sign']=total_tx.apply(lambda row: checkAddress(row['to'],address),axis=1)
    total_tx['TX_Value']=total_tx['value']/10**(total_tx['token_decimal'])*total_tx['Sign']
    total_tx['TX_Value']=total_tx['TX_Value']
    total_tx.index=total_tx['timestamp']
    return total_tx[['TX_Value','token_symbol','to','from']]

def getERC20HistBalance(cli,address,erc_address=None):
    df_tx= getERC20HistTx(cli, address, erc_address)
    df=pd.pivot_table(df_tx,index=df_tx.index,columns='token_symbol',values='TX_Value',aggfunc='sum')
    df=df.fillna(0)
    return df.cumsum()


"""
eth_cli=etherscanClient()
token_transations = getERC20HistBalance(eth_cli,'0x3d4Cc8A61c7528Fd86C55cfe061a78dCBA48EDd1',erc_address=None)
token_transations.index=token_transations.index.map(timestampToDate)
"""