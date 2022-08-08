import argparse

from pyspark.sql import SparkSession

def dadoscsv(data_source, output_uri):
    with SparkSession.builder.appName("Criacao tempo").getOrCreate() as spark:
        # Load the tempo CSV data
        if data_source is not None:
            dadoscsv_df = spark.read.option("header", "true").option("delimiter", ";").csv(data_source)

        # Create an in-memory DataFrame to query
        dadoscsv_df.createOrReplaceTempView("dados_csv")

        # Create a DataFrame de tempo
        tempo = spark.sql("""select ano, trimestre from dados_csv where CNPJ is not null and CNPJ <> ' '""")

        tempo2 = tempo.drop_duplicates()        
        
        # Write the results to the specified output URI
        tempo2.write.option("header", "true").option("delimiter", ";").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="s3://ingestaodados/RAW/dado_unificado_csv.csv")
    parser.add_argument(
        '--output_uri', help="s3://ingestaodados/Trusted/tempo/")
    args = parser.parse_args()

    dadoscsv(args.data_source,args.output_uri)