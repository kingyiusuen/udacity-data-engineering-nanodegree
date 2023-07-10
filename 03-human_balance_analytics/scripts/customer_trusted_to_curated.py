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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1688987483481 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerTrustedZone_node1688987483481",
    )
)

# Script generated for node Join
Join_node1688987504327 = Join.apply(
    frame1=CustomerTrustedZone_node1,
    frame2=AccelerometerTrustedZone_node1688987483481,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1688987504327",
)

# Script generated for node Customer Curated
CustomerCurated_node1688987536250 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1688987504327,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-de-nano-degree/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1688987536250",
)

job.commit()
