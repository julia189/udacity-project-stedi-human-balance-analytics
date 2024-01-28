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

# Script generated for node Source - Customer Trusted
SourceCustomerTrusted_node1706456002494 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://raw-data-udacity-project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="SourceCustomerTrusted_node1706456002494",
)

# Script generated for node Source - Accelerometer Trusted
SourceAccelerometerTrusted_node1706456042193 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": True},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://raw-data-udacity-project/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="SourceAccelerometerTrusted_node1706456042193",
    )
)

# Script generated for node Join
Join_node1706456071185 = Join.apply(
    frame1=SourceAccelerometerTrusted_node1706456042193,
    frame2=SourceCustomerTrusted_node1706456002494,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1706456071185",
)

# Script generated for node Drop Fields
DropFields_node1706456169536 = DropFields.apply(
    frame=Join_node1706456071185,
    paths=["y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1706456169536",
)

# Script generated for node Target - Customer Curated
TargetCustomerCurated_node1706456190437 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1706456169536,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://raw-data-udacity-project/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="TargetCustomerCurated_node1706456190437",
)

job.commit()
