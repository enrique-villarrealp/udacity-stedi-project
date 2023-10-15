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

# Script generated for node Customer - Curated
CustomerCurated_node1697382081643 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://evillarreal-udacity-stedi-lakehouse/curated/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1697382081643",
)

# Script generated for node StepTrainer - Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://evillarreal-udacity-stedi-lakehouse/landing/step_trainer_landing/"
        ],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Join - SerialNumber
JoinSerialNumber_node1697382124651 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerCurated_node1697382081643,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="JoinSerialNumber_node1697382124651",
)

# Script generated for node S3 Bucket
S3Bucket_node2 = glueContext.getSink(
    path="s3://evillarreal-udacity-stedi-lakehouse/trusted/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3Bucket_node2",
)
S3Bucket_node2.setCatalogInfo(
    catalogDatabase="evillarreal_stedi_trusted", catalogTableName="step_trainer_trusted"
)
S3Bucket_node2.setFormat("glueparquet")
S3Bucket_node2.writeFrame(JoinSerialNumber_node1697382124651)
job.commit()
