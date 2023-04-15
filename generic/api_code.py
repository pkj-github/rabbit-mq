import requests
import json


api_key='9d71eada-31ee-4f15-b762-255a94317741'

headers={
    'X-CMC_PRO_API_KEY':api_key,
    'Accepts': 'application/json'
}

params={
    'start':'1',
    'limit':'5',
    'convert':'USD'
}

url='https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

response=requests.get(url,params=params,headers=headers).json()

print('API response:',response)

coins=response['data']

crypto={}
for i in coins:
    if i['symbol']=='BTC':
        crypto['name']=i['name']
        crypto['symbol']=i['symbol']
        crypto['price']=i['quote']['USD']['price']

    #print (i['name'],i['symbol'],i['quote']['USD']['price'])


print('Inside main code :',crypto)






