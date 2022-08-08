import argparse

from pyspark.sql import SparkSession

def dadoscsv(data_source, output_uri):
    with SparkSession.builder.appName("Criacao reclamacao").getOrCreate() as spark:
        # Load the reclamacao CSV data
        if data_source is not None:
            dadoscsv_df = spark.read.option("header", "true").option("delimiter", ";").csv(data_source)

        # Create an in-memory DataFrame to query
        dadoscsv_df.createOrReplaceTempView("dados_csv")

        # Create a DataFrame de reclamacao
        reclamacao = spark.sql("""select (CONCAT(ano,trimestre)) CODIGO_TEMPO, CNPJ, 
cast(replace(INDICE,',','.') as decimal(8,2)) INDICE, 
QT_RECL_REG_PROC QTD_RECLAMACOES_PROCEDENTES, 
QT_RECL_REG_OUTRAS QTD_RECLAMACOES_OUTRAS, 
QT_RECL_REG_NAO_REG QTD_RECLAMACOES_NAO_REGULADAS, 
QT_TOTAL_RECLAMACOES QTD_RECLAMACOES,
QT_TOTAL_CLIENTES_CCS_SCR QTD_CLIENTES, 
QT_CLIENTES_CCS QTD_CLIENTES_CCS, 
QT_CLIENTES_SCR QTD_CLIENTES_SCR dados_csv where CNPJ is not null and CNPJ <> ' '""")

        reclamacao2 = reclamacao.drop_duplicates()        
        
        # Write the results to the specified output URI
        reclamacao2.write.option("header", "true").option("delimiter", ";").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="s3://ingestaodados/RAW/dado_unificado_csv.csv")
    parser.add_argument(
        '--output_uri', help="s3://ingestaodados/Trusted/reclamacao/")
    args = parser.parse_args()

    dadoscsv(args.data_source,args.output_uri)