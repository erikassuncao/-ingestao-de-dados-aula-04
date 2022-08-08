# importa as bibliotecas necessrias
import pandas as pd
import csv

# realiza a leitura do arquivo unificado.csv contendo os CNPJ
dados = pd.read_csv("s3://ingestaodados/RAW/202001.csv",sep = ';',encoding='iso-8859-1')

csv_file =  open("s3://ingestaodados/Trusted/DIM_INSTITUICAO.csv", 'w')
csv_writer = csv.writer(csv_file, delimiter=";")

csv_writer.writerow(["CNPJ","Instituicao","Tipo"])

for i, instituicao in dados.iterrows():
  if instituicao['CNPJ'] != " ":
    csv_writer.writerow([instituicao['CNPJ'],instituicao['Instituicao'],instituicao['Tipo']])
  else:
    continue

csv_file.close()

df = pd.read_csv("s3://ingestaodados/Trusted/DIM_INSTITUICAO.csv", sep=";")

df.drop_duplicates(subset=None, inplace=True)
ordenado = df.sort_values(by=["Instituicao"], ascending=True)

# Grava resultado tratado
ordenado.to_csv("s3://ingestaodados/Trusted/DIM_INSTITUICAO.csv", sep=";", encoding='iso-8859-1', index=False)
