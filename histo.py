import requests
import datetime
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/robin/Desktop/pea-tracker-5b072ff475b4.json"
import pandas as pd
from google.cloud import bigquery


# Change coins here
coins = ['ethereum', 'bitcoin', 'convex-finance']

# Change dates here. Go to https://www.epochconverter.com/ to convert date to unix ts
date_from = '1609462861' # '2021-01-01'
date_to = '1637854758' # '2021-11-25'

list = list()
loaded_at = loaded_at = datetime.datetime.now()

for coin in coins:
  res = requests.get("https://api.coingecko.com/api/v3/coins/%s/market_chart/range?vs_currency=usd&from=%s&to=%s" % (coin, date_from, date_to))
  json = res.json()
  for i in json['prices']:
    try:
      list.append([datetime.datetime.fromtimestamp(i[0]/1000), coin, i[1], loaded_at]) # [datetime, coin, price, loaded_at]
    except ValueError as err:
      print(err)

bqclient = bigquery.Client()

dataset_ref = bqclient.dataset('sources')
table_ref = dataset_ref.table('prices_crypto')

df = pd.DataFrame(list, columns =['date', 'coinGecko_name', 'price', 'loaded_at'])
bqclient.load_table_from_dataframe(df, table_ref).result()
