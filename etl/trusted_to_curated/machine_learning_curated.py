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

# Script generated for node Accelerometer - Trusted
AccelerometerTrusted_node1697900334481 = glueContext.create_dynamic_frame.from_catalog(
    database="evillarreal_stedi_trusted",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1697900334481",
)

# Script generated for node Step Trainer - Trusted
StepTrainerTrusted_node1697900382604 = glueContext.create_dynamic_frame.from_catalog(
    database="evillarreal_stedi_trusted",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1697900382604",
)

# Script generated for node SQL Query
SqlQuery1479 = """
select * from step inner join accel on step.sensorreadingtime = accel.timestamp
"""
SQLQuery_node1697902205982 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1479,
    mapping={
        "accel": AccelerometerTrusted_node1697900334481,
        "step": StepTrainerTrusted_node1697900382604,
    },
    transformation_ctx="SQLQuery_node1697902205982",
)

# Script generated for node Machine Learning - Curated
MachineLearningCurated_node2 = glueContext.getSink(
    path="s3://evillarreal-udacity-stedi-lakehouse/curated/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node2",
)
MachineLearningCurated_node2.setCatalogInfo(
    catalogDatabase="evillarreal_stedi_curated",
    catalogTableName="machine_learning_curated",
)
MachineLearningCurated_node2.setFormat("glueparquet")
MachineLearningCurated_node2.writeFrame(SQLQuery_node1697902205982)
job.commit()
