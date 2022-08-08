import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Tarifas
Tarifas_node1659538690775 = glueContext.create_dynamic_frame.from_catalog(
    database="trusted",
    table_name="trdbingestaodados_tarifas_t",
    transformation_ctx="Tarifas_node1659538690775",
)

# Script generated for node ttarifas
SqlQuery1764 = """
select CNPJ, 
CRC32(CODIGOSERVICO) CODIGO_SERVICO, 
CRC32(unidade) CODIGO_UNIDADE, 
DATAVIGENCIA DATA_VIGENCIA, 
VALORMAXIMO VALOR_MAXIMO, 
(case when TIPOVALOR ='Real' then 1 else 2 end) CODIGO_TIPO_VALOR
from myDataSource

"""
ttarifas_node1659565201399 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1764,
    mapping={"myDataSource": Tarifas_node1659538690775},
    transformation_ctx="ttarifas_node1659565201399",
)

# Script generated for node Tunidade
SqlQuery1765 = """
select distinct  CRC32(unidade) CODIGO_UNIDADE , UNIDADE  from myDataSource
where unidade is not null
"""
Tunidade_node1659538272885 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1765,
    mapping={"myDataSource": Tarifas_node1659538690775},
    transformation_ctx="Tunidade_node1659538272885",
)

# Script generated for node Tservico
SqlQuery1766 = """
select distinct  CRC32(CODIGOSERVICO) CODIGO_SERVICO,SERVICO from  myDataSource

"""
Tservico_node1659546317728 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1766,
    mapping={"myDataSource": Tarifas_node1659538690775},
    transformation_ctx="Tservico_node1659546317728",
)

# Script generated for node FATOtarifa
FATOtarifa_node1659565217997 = glueContext.write_dynamic_frame.from_catalog(
    frame=ttarifas_node1659565201399,
    database="analytics",
    table_name="adbingestaodados_tarifas",
    transformation_ctx="FATOtarifa_node1659565217997",
)

# Script generated for node DIMunidade
DIMunidade_node1659539582795 = glueContext.write_dynamic_frame.from_catalog(
    frame=Tunidade_node1659538272885,
    database="analytics",
    table_name="adbingestaodados_unidade",
    transformation_ctx="DIMunidade_node1659539582795",
)

# Script generated for node DIMservico
DIMservico_node1659546350396 = glueContext.write_dynamic_frame.from_catalog(
    frame=Tservico_node1659546317728,
    database="analytics",
    table_name="adbingestaodados_servico",
    transformation_ctx="DIMservico_node1659546350396",
)

job.commit()
