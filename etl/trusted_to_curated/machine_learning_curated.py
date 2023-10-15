import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer - Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://evillarreal-udacity-stedi-lakehouse/trusted/accelerometer_trusted/"
        ],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Step Trainer - Trusted
StepTrainerTrusted_node1697382832463 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://evillarreal-udacity-stedi-lakehouse/trusted/step_trainer_trusted/"
        ],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1697382832463",
)

# Script generated for node Join
Join_node1697382966839 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainerTrusted_node1697382832463,
    keys1=["timestamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1697382966839",
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
MachineLearningCurated_node2.writeFrame(Join_node1697382966839)
job.commit()
