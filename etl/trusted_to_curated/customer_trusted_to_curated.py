import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1697898804329 = glueContext.create_dynamic_frame.from_catalog(
    database="evillarreal_stedi_trusted",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1697898804329",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1697898808340 = glueContext.create_dynamic_frame.from_catalog(
    database="evillarreal_stedi_trusted",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1697898808340",
)

# Script generated for node Renamed keys for Join - Emails
RenamedkeysforJoinEmails_node1697899599389 = ApplyMapping.apply(
    frame=AccelerometerTrusted_node1697898808340,
    mappings=[
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("z", "double", "right_z", "double"),
        ("birthday", "string", "right_birthday", "string"),
        (
            "sharewithpublicasofdate",
            "double",
            "right_sharewithpublicasofdate",
            "double",
        ),
        (
            "sharewithresearchasofdate",
            "double",
            "right_sharewithresearchasofdate",
            "double",
        ),
        ("registrationdate", "long", "right_registrationdate", "long"),
        ("customername", "string", "right_customername", "string"),
        ("user", "string", "right_user", "string"),
        (
            "sharewithfriendsasofdate",
            "double",
            "right_sharewithfriendsasofdate",
            "double",
        ),
        ("y", "double", "right_y", "double"),
        ("x", "double", "right_x", "double"),
        ("timestamp", "long", "right_timestamp", "long"),
        ("email", "string", "right_email", "string"),
        ("lastupdatedate", "long", "right_lastupdatedate", "long"),
        ("phone", "long", "right_phone", "long"),
    ],
    transformation_ctx="RenamedkeysforJoinEmails_node1697899599389",
)

# Script generated for node Join - Emails
JoinEmails_node1697380977810 = Join.apply(
    frame1=CustomerTrusted_node1697898804329,
    frame2=RenamedkeysforJoinEmails_node1697899599389,
    keys1=["email"],
    keys2=["right_user"],
    transformation_ctx="JoinEmails_node1697380977810",
)

# Script generated for node Drop Fields
DropFields_node1697899562701 = DropFields.apply(
    frame=JoinEmails_node1697380977810,
    paths=[
        "right_serialnumber",
        "right_birthday",
        "right_sharewithpublicasofdate",
        "right_sharewithresearchasofdate",
        "right_registrationdate",
        "right_z",
        "right_customername",
        "right_user",
        "right_sharewithfriendsasofdate",
        "right_y",
        "right_x",
        "right_timestamp",
        "right_email",
        "right_lastupdatedate",
        "right_phone",
    ],
    transformation_ctx="DropFields_node1697899562701",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1697899635239 = DynamicFrame.fromDF(
    DropFields_node1697899562701.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1697899635239",
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
S3bucket_node2.writeFrame(DropDuplicates_node1697899635239)
job.commit()
