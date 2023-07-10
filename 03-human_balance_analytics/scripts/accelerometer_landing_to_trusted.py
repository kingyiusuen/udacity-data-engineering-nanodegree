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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1688968941059 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1688968941059",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1688968745495 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrustedZone_node1688968941059,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node1688968745495",
)

# Script generated for node Drop Fields
DropFields_node1688969032050 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1688968745495,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1688969032050",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688969032050,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-de-nano-degree/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrustedZone_node3",
)

job.commit()
