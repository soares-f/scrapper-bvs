import requests
from pprint import pprint
from bs4 import BeautifulSoup
page = requests.get("http://portal.revistas.bvs.br/search.php?search=&text_lang%5B%5D=all_&text_country%5B%5D=all_&count=50&lang=pt&from=1&from=51")

soup = BeautifulSoup(page.content, 'html.parser')

items_box = soup.find_all(class_='result list_other_color')

for item in items_box:
    try:
        dicio_tmp = dict()
        dicio_tmp['title'] = item.find(class_='title').text.strip('\n').strip()
        dicio_tmp['issn'] = item.find(class_='issn').text.strip('\n').strip()
        dicio_tmp['controle'] = item.find(class_='onlineaccess').find_all('li')[-1].find('img')._attr_value_as_string('alt')
        pprint(dicio_tmp)
    except:
        pass

items_box = soup.find_all(class_='result')

for item in items_box:
    try:
        dicio_tmp = dict()
        dicio_tmp['title'] = item.find(class_='title').text.strip('\n').strip()
        dicio_tmp['issn'] = item.find(class_='issn').text.strip('\n').strip()
        dicio_tmp['controle'] = item.find(class_='onlineaccess').find_all('li')[-1].find('img')._attr_value_as_string('alt')
        pprint(dicio_tmp)
    except:
        pass





