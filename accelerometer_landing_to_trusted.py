import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Source - Accelerometer Landing
SourceAccelerometerLanding_node1706454901641 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://raw-data-udacity-project/accelerometer/landing/"],
            "recurse": True,
        },
        transformation_ctx="SourceAccelerometerLanding_node1706454901641",
    )
)

# Script generated for node Source - Customer Trusted
SourceCustomerTrusted_node1706454963337 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://raw-data-udacity-project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="SourceCustomerTrusted_node1706454963337",
)

# Script generated for node Join
SourceCustomerTrusted_node1706454963337DF = (
    SourceCustomerTrusted_node1706454963337.toDF()
)
SourceAccelerometerLanding_node1706454901641DF = (
    SourceAccelerometerLanding_node1706454901641.toDF()
)
Join_node1706455037743 = DynamicFrame.fromDF(
    SourceCustomerTrusted_node1706454963337DF.join(
        SourceAccelerometerLanding_node1706454901641DF,
        (
            SourceCustomerTrusted_node1706454963337DF["email"]
            == SourceAccelerometerLanding_node1706454901641DF["user"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1706455037743",
)

# Script generated for node Drop Fields Customer Data
DropFieldsCustomerData_node1706455173311 = DropFields.apply(
    frame=Join_node1706455037743,
    paths=["y", "x", "z", "user", "timestamp"],
    transformation_ctx="DropFieldsCustomerData_node1706455173311",
)

# Script generated for node Target - Accelerometer Trusted
TargetAccelerometerTrusted_node1706455119127 = (
    glueContext.write_dynamic_frame.from_options(
        frame=DropFieldsCustomerData_node1706455173311,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://raw-data-udacity-project/accelerometer/trusted/",
            "partitionKeys": [],
        },
        transformation_ctx="TargetAccelerometerTrusted_node1706455119127",
    )
)

job.commit()
