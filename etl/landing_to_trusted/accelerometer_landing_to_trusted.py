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

# Script generated for node Accelerometer - Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://evillarreal-udacity-stedi-lakehouse/landing/accelerometer_landing/"
        ],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer - Trusted
CustomerTrusted_node1697379469769 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://evillarreal-udacity-stedi-lakehouse/trusted/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1697379469769",
)

# Script generated for node Inner Join - Email
InnerJoinEmail_node1697379506547 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1697379469769,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="InnerJoinEmail_node1697379506547",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.getSink(
    path="s3://evillarreal-udacity-stedi-lakehouse/trusted/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node2",
)
S3bucket_node2.setCatalogInfo(
    catalogDatabase="evillarreal_stedi_trusted",
    catalogTableName="accelerometer_trusted",
)
S3bucket_node2.setFormat("glueparquet")
S3bucket_node2.writeFrame(InnerJoinEmail_node1697379506547)
job.commit()
