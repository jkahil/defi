import requests
import pandas as pd
import time
from duneanalytics import DuneAnalytics

def getQuery(id,password,query_id):
    dune = DuneAnalytics(id, password)
    dune.login()
    dune.fetch_auth_token()
    result_id=dune.query_result_id(query_id=query_id)
    df = dune.query_result(result_id)
    df_clean = pd.json_normalize(df['data']['get_result_by_result_id'])
    df_clean = df_clean.drop('__typename', axis=1)
    return df_clean

def cleanAddress(address):
    new_address=address.replace('\\', "0")
    return new_address

def getWalletInfo(address,pause=5):
  """
  returns wallet info of a wallet using Debank API
  """
  print(address)
  to_keep=['data.is_contract','data.create_at','data.is_multisig_addr',
       'data.usd_value', 'data.used_chains', 'data.wallet_usd_value']
  url='https://api.debank.com/user/addr?addr='+str(address)
  data = requests.get(url)
  data=pd.json_normalize(data.json())
  wallet_summary=data.set_index('data.id')
  # some wallets will not have values
  missing_columns=[x for x in to_keep if x not in wallet_summary.columns]
  for x in missing_columns:
    wallet_summary[x]=0
  time.sleep(pause)
  return wallet_summary[to_keep]