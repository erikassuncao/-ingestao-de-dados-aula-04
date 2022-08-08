import argparse

from pyspark.sql import SparkSession

def dadoscsv(data_source, output_uri):
    with SparkSession.builder.appName("Criacao tarifa").getOrCreate() as spark:
        # Load the tarifa CSV data
        if data_source is not None:
            dadoscsv_df = spark.read.option("header", "true").option("delimiter", ";").csv(data_source)

        # Create an in-memory DataFrame to query
        dadoscsv_df.createOrReplaceTempView("dados_csv")

        # Create a DataFrame de tarifa
        tarifa = spark.sql("""select CNPJ, 
(CODIGOSERVICO) CODIGO_SERVICO, 
(unidade) CODIGO_UNIDADE, 
DATAVIGENCIA DATA_VIGENCIA, 
VALORMAXIMO VALOR_MAXIMO, 
(case when TIPOVALOR ='Real' then 1 else 2 end) CODIGO_TIPO_VALOR from dados_csv where CNPJ is not null and CNPJ <> ' '""")

        tarifa2 = tarifa.drop_duplicates()        
        
        # Write the results to the specified output URI
        tarifa2.write.option("header", "true").option("delimiter", ";").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="s3://ingestaodados/RAW/dado_unificado_csv.csv")
    parser.add_argument(
        '--output_uri', help="s3://ingestaodados/Trusted/tarifa/")
    args = parser.parse_args()

    dadoscsv(args.data_source,args.output_uri)