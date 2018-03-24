import xml.etree.ElementTree as ET
from datetime import datetime
from pymongo import MongoClient

def extrai_infos(campo, dicionario):
    try:
        if campo in ['id']:
            dicionario[campo] = doc_tmp.find('str[@name="'+campo+'"]').text

        elif campo =='da':
            valor = doc_tmp.find('str[@name="' + campo + '"]').text
            if (int(valor[-2:]) > 0) and (int(valor[-2:]) < 13):
                dicionario[campo] = datetime.strptime(valor,'%Y%m')
            else:
                dicionario[campo] = datetime.strptime(valor[:4], '%Y')

        elif campo == 'entry_date':
            try:
                dicionario[campo] = datetime.strptime(
                    doc_tmp.find('str[@name="' + campo + '"]').text,'%Y-%m-%d')
            except:
                dicionario[campo] = datetime.strptime(
                    doc_tmp.find('str[@name="' + campo + '"]').text[:7], '%Y-%m')

        elif campo == 'version':
            dicionario[campo] = doc_tmp.find('long').text

        elif campo in ['type','ti_es','ti_pt','fo','cp','ab_pt','ab_en','ab_es','cc']:
            dicionario[campo] = doc_tmp.find('arr[@name="'+campo+'"]')[0].text

        elif campo in ['ur','au','afiliacao_autor','ti','ta','is','la','ab','ct','mh','sh','is']:
            campo_tmp = []
            for campo_unit in doc_tmp.find('arr[@name="'+campo+'"]'):
                campo_tmp.append(campo_unit.text)
            dicionario[campo] = campo_tmp
        elif campo in ['mark_ab_es','mark_ab_pt','mark_ab_en']:

            list_tmp = doc_tmp.find('arr[@name="'+campo+'"]')[0].text.split('<h2>')


    except (TypeError, AttributeError) as e:
        #print doc_valores['id']
        #print 'erro em: ' + campo
        pass


client = MongoClient('localhost', 27017)
db = client.db_teste
collection = db.dados_lilacs
erros = db.erros
# rever a 311


for i_arquivo in range(512, 643, 1):
    print str(i_arquivo)
    try:
        str_arquivo = './scrap/abs_lilacs_pg_' + str(i_arquivo+1) + 's.xml'
        fd = open(str_arquivo)
        arquivo = fd.read()

        tree = ET.fromstring(arquivo)
        documentos = tree.findall(".//doc")

        for doc_tmp in documentos:
            doc_valores = { 'id': None,
                            'type': None,
                            'ur': None,
                            'au': None,
                            'afiliacao_autor': None,
                            'ti_es': None,
                            'ti_pt': None,
                            'ti_en': None,
                            'ti': None,
                            'fo': None,
                            'ta': None,
                            'is': None,
                            'la': None,
                            'cp': None,
                            'da': None,
                            'ab_pt': None,
                            'ab_en': None,
                            'ab_es': None,
                            'ab': None,
                            'entry_date': None,
                            'version': None,
                            'ct': None,
                            'mh': None,
                            'sh': None,
                            'cc': None,
                            'is': None,
                            }
            for campo_doc in doc_valores:
                #print campo
                extrai_infos(campo_doc, doc_valores)
            doc_valores['_id'] = doc_valores.pop('id')
            doc_valores['arquivo'] = i_arquivo + 1

            try:
                collection.insert(doc_valores)
            except Exception as e:
                erros.insert({'arquivo': i_arquivo + 1,
                              'id': doc_valores['_id'],
                              'tipo': str(e)})
                pass
    except Exception as erro:
        print 'erro em: ' + str(i_arquivo + 1)
        raise erro
