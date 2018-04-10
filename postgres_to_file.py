
import psycopg2
from langdetect import detect
import codecs
import sys

# split a list into evenly sized chunks
def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]


if __name__ == "__main__":
    n_job = sys.argv[1]
    banco = 'bvs'
    user  = 'postgres'
    host = 'localhost'
    password = 'Fe748592'


    diretorio = './to_align/'+str(n_job)+'/'
    conn = psycopg2.connect('dbname={0} user={1} host={2} password={3}'.format((banco, user, host, password)))
    cursor = conn.cursor()

    ab_lang1 = 'ab_en'
    ab_lang2 = 'ab_pt'
    tabela  = 'merged_all'
    lang1 = 'en'
    lang2 = 'pt'
    sql = "SELECT _id,{0},{1} from {2}".format((ab_lang1, ab_lang2, tabela))

    cursor.execute(sql)

    dados = cursor.fetchall()
    g = chunks(dados, 277302)

    for item in g[int(n_job)]:
        #try:
                if (detect(item[1].lower()) == lang1 and detect(item[2].lower()) == lang2):
                    id_bd = item[0]
                    # escreve arquivo ingles
                    file_name = diretorio + id_bd + '_' + lang1 + '.txt'
                    file_obj = codecs.open(file_name, 'w', 'utf-8')
                    file_obj.write(item[1].replace('\n', '').replace('\t', '').replace('\r', ''))
                    file_obj.close()

                    # escreve arquivo portugues
                    file_name = diretorio + id_bd + '_' + lang2 + '.txt'
                    file_obj = codecs.open(file_name, 'w', 'utf-8')
                    file_obj.write(item[2].replace('\n', '').replace('\t', '').replace('\r', ''))
                    file_obj.close()
        #except:
        #    pass