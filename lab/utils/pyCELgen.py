from random import randint

def gen_ddd():
    DDDs = [11,12,13,14,15,16,17,18,19,21,22,24,27,28,31,32,33,34,35,37,38,41
        ,42,43,44,45,46,47,48,49,51,53,54,55,61,62,63,64,65,66,67,68,69,71,73
        ,74,75,77,79,81,82,83,84,85,86,87,88,89,91,92,93,94,95,96,97,98,99]
    return f'({DDDs[randint(5,len(DDDs)-1)]})'

def gen_cel():
    return f'9{str(randint(0,9999)).zfill(4)}-{str(randint(0,9999)).zfill(4)}'

def gen_full_cel():
    return f'{gen_ddd()} {gen_cel()}'

def cpf_full_cell_many(n):
    cel_validos = []
    while len(cel_validos) < int(n):
        cel = gen_full_cel()
        if cel not in cel_validos:
            cel_validos.append(cel)
    return cel_validos

qtd = input('Digite a quantidade entre 1 e 10000: ')
if int(qtd) <= 10000:
    cells = cpf_full_cell_many(qtd)
    for cell in cells:
        print(cell)
else:
    print('insira um número menor que 10000')
