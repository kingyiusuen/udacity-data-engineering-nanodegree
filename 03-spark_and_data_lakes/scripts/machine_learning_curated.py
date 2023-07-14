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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1688988152173 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1688988152173",
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrustedZone_node1",
)

# Script generated for node Join
Join_node1688988165024 = Join.apply(
    frame1=StepTrainerTrustedZone_node1,
    frame2=AccelerometerTrusted_node1688988152173,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1688988165024",
)

# Script generated for node Drop Fields
DropFields_node1688988516371 = DropFields.apply(
    frame=Join_node1688988165024,
    paths=["sensorreadingtime", "serialnumber", "distancefromobject", "x", "y", "z"],
    transformation_ctx="DropFields_node1688988516371",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1688988243050 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688988516371,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-de-nano-degree/machine-learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1688988243050",
)

job.commit()
