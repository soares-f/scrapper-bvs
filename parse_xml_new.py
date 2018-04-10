import xml.etree.ElementTree as ET
from datetime import datetime
from pymongo import MongoClient

def extrai_infos(campo, dicionario, doc_tmp):
    try:
        if campo in ['id']:
            dicionario[campo] = doc_tmp.find('str[@name="'+campo+'"]').text

        elif campo =='da':
            try:
                valor = doc_tmp.find('str[@name="' + campo + '"]').text
                if (int(valor[-2:]) > 0) and (int(valor[-2:]) < 13):
                    dicionario[campo] = datetime.strptime(valor,'%Y%m')
                else:
                    dicionario[campo] = datetime.strptime(valor[:4], '%Y')
            except:
                pass

        elif campo == 'entry_date':
            try:
                dicionario[campo] = datetime.strptime(
                    doc_tmp.find('str[@name="' + campo + '"]').text,'%Y-%m-%d')
            except:
                try:
                    dicionario[campo] = datetime.strptime(
                        doc_tmp.find('str[@name="' + campo + '"]').text[:7], '%Y-%m')
                except:
                    try:
                        dicionario[campo] = datetime.strptime(
                            doc_tmp.find('str[@name="' + campo + '"]').text[:8], '%Y%m%d')
                    except:
                        pass

        elif campo == 'version':
            dicionario[campo] = doc_tmp.find('long').text

        elif campo in ['type','ti_es','ti_pt','fo','cp','ab_pt','ab_en','ab_es','cc','ti_en']:
            dicionario[campo] = doc_tmp.find('arr[@name="'+campo+'"]')[0].text

        elif campo in ['ur','au','afiliacao_autor','ti','ta','is','la','ab','ct','mh','sh']:
            campo_tmp = []
            for campo_unit in doc_tmp.find('arr[@name="'+campo+'"]'):
                campo_tmp.append(campo_unit.text)
            dicionario[campo] = campo_tmp
        elif campo in ['mark_ab_es','mark_ab_pt','mark_ab_en']:

            dict_tmp = dict()
            list_tmp = doc_tmp.find('arr[@name="'+campo+'"]')[0].text.split('<h2>')
            if len(list_tmp[0]) < 3:
                for item in list_tmp[1:]:
                    item_tmp = item.split('</h2>')
                    dict_tmp[item_tmp[0]] = item_tmp[1]

                dicionario[campo] = dict_tmp


    except (TypeError, AttributeError) as e:
        #print doc_valores['id']
        #print 'erro em: ' + campo
        pass



def processa_arquivos(inicio_arq):
    """

    :param inicio_arq:
    :param nome_collection:
    :return:
    """

    client = MongoClient('localhost', 27017)
    db = client.bvs
    collection = db.LILACS
    erros = db.erros

    for i_arquivo in range(0,1604):
        print str(i_arquivo)
        try:
            str_arquivo = inicio_arq + str(i_arquivo+1) + 's.xml'
            fd = open(str_arquivo)
            arquivo = fd.read()

            tree = ET.fromstring(arquivo)
            documentos = tree.findall(".//doc")

            for doc_tmp_ind in documentos:
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
                                'mark_ab_es': None,
                                'mark_ab_pt': None,
                                'mark_ab_en':  None,
                                'collection': 'LILACS'

                                }
                for campo_doc in doc_valores:
                    #print campo
                    extrai_infos(campo_doc, doc_valores, doc_tmp_ind)
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
            #raise erro



