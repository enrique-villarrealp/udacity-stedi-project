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
AccelerometerTrusted_node1697380817629 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://evillarreal-udacity-stedi-lakehouse/trusted/accelerometer_trusted/"
        ],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1697380817629",
)

# Script generated for node Customer - Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://evillarreal-udacity-stedi-lakehouse/trusted/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Join - Emails
JoinEmails_node1697380977810 = Join.apply(
    frame1=AccelerometerTrusted_node1697380817629,
    frame2=CustomerTrusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinEmails_node1697380977810",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.getSink(
    path="s3://evillarreal-udacity-stedi-lakehouse/curated/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node2",
)
S3bucket_node2.setCatalogInfo(
    catalogDatabase="evillarreal_stedi_curated", catalogTableName="customer_curated"
)
S3bucket_node2.setFormat("glueparquet")
S3bucket_node2.writeFrame(JoinEmails_node1697380977810)
job.commit()
