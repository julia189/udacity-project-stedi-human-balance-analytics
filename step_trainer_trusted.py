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

# Script generated for node Source Step Trainer Landing
SourceStepTrainerLanding_node1706458718697 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://raw-data-udacity-project/step_trainer/landing/"],
            "recurse": True,
        },
        transformation_ctx="SourceStepTrainerLanding_node1706458718697",
    )
)

# Script generated for node Source Customer Curated
SourceCustomerCurated_node1706458675920 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://raw-data-udacity-project/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="SourceCustomerCurated_node1706458675920",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1706458748917 = ApplyMapping.apply(
    frame=SourceStepTrainerLanding_node1706458718697,
    mappings=[
        ("sensorreadingtime", "bigint", "sensorreadingtime", "long"),
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("distancefromobject", "int", "distancefromobject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1706458748917",
)

# Script generated for node Join
SourceCustomerCurated_node1706458675920DF = (
    SourceCustomerCurated_node1706458675920.toDF()
)
RenamedkeysforJoin_node1706458748917DF = RenamedkeysforJoin_node1706458748917.toDF()
Join_node1706458737791 = DynamicFrame.fromDF(
    SourceCustomerCurated_node1706458675920DF.join(
        RenamedkeysforJoin_node1706458748917DF,
        (
            SourceCustomerCurated_node1706458675920DF["serialnumber"]
            == RenamedkeysforJoin_node1706458748917DF["right_serialnumber"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1706458737791",
)

# Script generated for node Drop Fields
DropFields_node1706459124319 = DropFields.apply(
    frame=Join_node1706458737791,
    paths=[
        "email",
        "phone",
        "right_serialnumber",
        "lastupdatedate",
        "birthday",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1706459124319",
)

# Script generated for node Amazon S3
AmazonS3_node1706458804915 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1706459124319,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://raw-data-udacity-project/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1706458804915",
)

job.commit()
