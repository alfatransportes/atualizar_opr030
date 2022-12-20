from banco import Banco

def funcao():
    print('Olá, mundo!!')


if __name__ == "__main__":
    banco = Banco('postgres','postgres','152.67.49.7')
    conn = banco.conexao()
    #consulta = input('Digite a sua query: ')
    banco.percorreCTES()