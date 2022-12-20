import psycopg2
from psycopg2 import Error
import psycopg2.extras

class Banco:

    def __init__(self,user,password,ip):
        self.user = user
        self.password = password
        self.ip = ip

    def conexao(self, database = 'at_alfa'):
        try:
            conn = psycopg2.connect(user = self.user, password = self.password, host = self.ip, database = database)
            print('Conectado com sucesso')
            return conn
        except (Exception, Error) as error:
            print("Erro ao conectar no servidor PostgreSQL ", error)
            return

    def consultaBanco(self,sql):
        conn = self.conexao()
        cursor = conn.cursor()

        cursor.execute(sql)
        # Fetch result
        resultado = cursor.fetchone()
        if resultado:
            print('Resultado: ', resultado)
    

    def percorreCTES(self):
        sql = '''SELECT sco04_serial as serial,sco03_ag_loc as ag_loc FROM sco04 JOIN opr030 ON sco04.sco04_serial = opr030.opr030_codigo WHERE opr030.opr030_entregue = 'N' AND sco04_dtaent IS NOT NULL AND sco04_tiplan IN ('0','1','2') AND sco04_data > '01-01-2020' LIMIT 10000'''
        conn = self.conexao('alfa')
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(sql)

        resultado = cursor.fetchall()
        if resultado:
            for i in resultado:
                self.atualizarOpr030(i)

        cursor.close()
        conn.close()
    
    def atualizarOpr030(self, registros):
        conn = self.conexao('at_alfa')
        cursor = conn.cursor()

        sql = '''UPDATE opr030 SET opr030_entregue = 'Y', emp003_atual={}  WHERE opr030_entregue = 'N' AND opr030_codigo = {} '''.format(registros['ag_loc'],registros['serial'])
        print('query preparada: ', sql)
        try:
            cursor.execute(sql)
            conn.commit()
            print('\n Inserido com sucesso! ')
        except (Exception, Error) as error:
            print("Erro ao inserir no banco opr030 ", error)
            conn.rollback()
            return

        cursor.close()
        conn.close()



        
        