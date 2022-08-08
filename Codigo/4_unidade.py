import argparse

from pyspark.sql import SparkSession

def dadoscsv(data_source, output_uri):
    with SparkSession.builder.appName("Criacao unidade").getOrCreate() as spark:
        # Load the unidade CSV data
        if data_source is not None:
            dadoscsv_df = spark.read.option("header", "true").option("delimiter", ";").csv(data_source)

        # Create an in-memory DataFrame to query
        dadoscsv_df.createOrReplaceTempView("dados_csv")

        # Create a DataFrame de unidade
        unidade = spark.sql("""select distinct  unidade CODIGO_UNIDADE , UNIDADE  from myDataSource
where unidade is not null from dados_csv where CNPJ is not null and CNPJ <> ' '""")

        # Write the results to the specified output URI
        unidade.write.option("header", "true").option("delimiter", ";").mode("append").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="s3://ingestaodados/RAW/202001.csv")
    parser.add_argument(
        '--output_uri', help="s3://ingestaodados/Trusted/unidade/")
    args = parser.parse_args()

    dadoscsv(args.data_source,args.output_uri)
			