from random import randint

def get_raca():
    racas = ['branca', 'preta', 'parda', 'indígena', 'amarela']
    return racas[randint(0,len(racas) - 1)]

def get_raca_many(n):
    racas = []
    while len(racas) < int(n):
        racas.append(get_raca())
    return racas


qtd = input('Digite a quantidade entre 1 e 10000: ')
if int(qtd) <= 10000:
    racas = get_raca_many(qtd)
    for raca in racas:
        print(raca)
else:
    print('insira um número menor que 10000')
