import requests

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
url = 'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&apikey=2V971IYPRGC914E6'
r = requests.get(url)
data = r.json()

open("export.csv", "wb").write(data)

print(data)