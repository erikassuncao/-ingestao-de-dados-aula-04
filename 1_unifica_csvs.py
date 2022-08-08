# importa as bibliotecas necessrias
import pandas as pd
import os
import glob
import re
import urllib.request, json 

# pega todos os csv na pasta "raw"
path = r'/home/tasks/raw'
all_files = glob.glob(os.path.join(path, "*.csv"))

li = []

for filename in all_files:
    df = pd.read_csv(filename, index_col=None, sep=';', skiprows=[0], header=None, encoding ='ISO-8859-1')
    li.append(df)

# faz um merge entre os arquivos
dataset = pd.concat(li, axis=0, ignore_index=True)

cabecalho = ["Ano","Trimestre","Categoria","Tipo","CNPJ","Instituicao","Indice","Quantidade de reclamações reguladas procedentes","Quantidade de reclamações reguladas - outras","Quantidade de reclamações não reguladas","Quantidade total de reclamações","Quantidade total de clientes CCS e SCR","Quantidade de clientes CCS","Quantidade de clientes SCR",""]

# salva a lista em um novo csv dentro de trusted
df = pd.DataFrame(dataset)
df.to_csv('/home/tasks/trusted/unificado.csv', sep=';', index=False, header=cabecalho)