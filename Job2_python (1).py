import argparse

from pyspark.sql import SparkSession

def calculate_red_violations(data_source, output_uri):
    """
    Processes sample food establishment inspection data and queries the data to find the top 10 establishments
    with the most Red violations from 2006 to 2020.

    :param data_source: The URI of your food establishment data CSV, such as 's3://DOC-EXAMPLE-BUCKET/food-establishment-data.csv'.
    :param output_uri: The URI where output is written, such as 's3://DOC-EXAMPLE-BUCKET/restaurant_violation_results'.
    """
    with SparkSession.builder.appName("Calculate Red Health Violations").getOrCreate() as spark:
        # Load the restaurant violation CSV data
        if data_source is not None:
            restaurants_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        restaurants_df.createOrReplaceTempView("dados_csv")

        # Create a DataFrame of the top 10 restaurants with the most Red violations
        instituicao = spark.sql("""select DISTINCT CNPJ, INSTITUICAO,TIPO from  dados_csv WHERE CNPJ IS NOT NULL""")

        # Write the results to the specified output URI
        instituicao.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="s3://ingestaodados/RAW/202001.csv")
    parser.add_argument(
        '--output_uri', help="s3://ingestaodados/Trusted/")
    args = parser.parse_args()

    calculate_red_violations(args.data_source, args.output_uri)
			