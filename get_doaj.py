import requests
import time
from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.bvs
collection = db.DOAJ

url1 = 'https://doaj.org/api/v1/search/journals/*?page='
url2 = '&pageSize=100'

#113
for i in range(1, 113):
    resp = requests.get(url=url1+str(i)+url2)
    data = resp.json()  # Check the JSON Response Content documentation below
    results = data['results']
    collection.insert(results)
    time.sleep(30)