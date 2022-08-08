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

# Script generated for node DadosCSV
DadosCSV_node1659543362935 = glueContext.create_dynamic_frame.from_catalog(
    database="trusted",
    table_name="trdbingestaodados_dadounificado",
    transformation_ctx="DadosCSV_node1659543362935",
)

# Script generated for node tinstituicao
SqlQuery1696 = """
select DISTINCT CNPJ, INSTITUICAO,TIPO from  myDataSource
WHERE CNPJ IS NOT NULL

"""
tinstituicao_node1659543398186 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1696,
    mapping={"myDataSource": DadosCSV_node1659543362935},
    transformation_ctx="tinstituicao_node1659543398186",
)

# Script generated for node ttempo
SqlQuery1697 = """
select distinct ano,trimestre, CRC32(CONCAT(ano,trimestre)) CODIGO_TEMPO 
from myDataSource
"""
ttempo_node1659557168901 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1697,
    mapping={"myDataSource": DadosCSV_node1659543362935},
    transformation_ctx="ttempo_node1659557168901",
)

# Script generated for node treclamacao
SqlQuery1698 = """
select CRC32(CONCAT(ano,trimestre)) CODIGO_TEMPO, CNPJ, 
cast(replace(INDICE,',','.') as decimal(8,2)) INDICE, 
QT_RECL_REG_PROC QTD_RECLAMACOES_PROCEDENTES, 
QT_RECL_REG_OUTRAS QTD_RECLAMACOES_OUTRAS, 
QT_RECL_REG_NAO_REG QTD_RECLAMACOES_NAO_REGULADAS, 
QT_TOTAL_RECLAMACOES QTD_RECLAMACOES,
QT_TOTAL_CLIENTES_CCS_SCR QTD_CLIENTES, 
QT_CLIENTES_CCS QTD_CLIENTES_CCS, 
QT_CLIENTES_SCR QTD_CLIENTES_SCR
from myDataSource
"""
treclamacao_node1659561330973 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1698,
    mapping={"myDataSource": DadosCSV_node1659543362935},
    transformation_ctx="treclamacao_node1659561330973",
)

# Script generated for node DIMinstituicao
DIMinstituicao_node1659543439598 = glueContext.write_dynamic_frame.from_catalog(
    frame=tinstituicao_node1659543398186,
    database="analytics",
    table_name="adbingestaodados_instituicao",
    transformation_ctx="DIMinstituicao_node1659543439598",
)

# Script generated for node DIMtempo
DIMtempo_node1659557176067 = glueContext.write_dynamic_frame.from_catalog(
    frame=ttempo_node1659557168901,
    database="analytics",
    table_name="adbingestaodados_tempo",
    transformation_ctx="DIMtempo_node1659557176067",
)

# Script generated for node FATOreclamacao
FATOreclamacao_node1659561355441 = glueContext.write_dynamic_frame.from_catalog(
    frame=treclamacao_node1659561330973,
    database="analytics",
    table_name="adbingestaodados_reclamacao",
    transformation_ctx="FATOreclamacao_node1659561355441",
)

job.commit()
