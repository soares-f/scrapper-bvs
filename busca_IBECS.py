import urllib
import time

#time.sleep(60)

url_base1 = 'http://pesquisa.bvsalud.org/portal/?output=xml&lang=pt&from='
#tmp_inicio = 21
url_base2 = '&sort=&format=summary&count=500&fb=&page='
#tmp_pagina = 2
url_base3 = '&filter%5Bdb%5D%5B%5D=IBECS&filter%5Btype%5D%5B%5D=article&q=&index=tw'

por_pagina = 500

#tmp_inicio = 1
#tmp_pagina = 1
#342
for i in range(342):
    tmp_inicio = i*por_pagina + 1
    tmp_pagina = (i+1)
    print tmp_pagina
    url_tmp = url_base1 + str(tmp_inicio) + url_base2 + str(tmp_pagina) + url_base3
    destino_tmp = '/home/soarescmsa/IBECS/scrapped/abs_IBECS_pg_' + str(tmp_pagina) + 's.xml'
    urllib.urlretrieve(url_tmp, destino_tmp)
    time.sleep(150)
