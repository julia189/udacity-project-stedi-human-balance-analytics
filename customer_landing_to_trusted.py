import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing Source
CustomerLandingSource_node1706453794850 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://raw-data-udacity-project/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLandingSource_node1706453794850",
)

# Script generated for node Filter Research Purposes
FilterResearchPurposes_node1706453830337 = Filter.apply(
    frame=CustomerLandingSource_node1706453794850,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="FilterResearchPurposes_node1706453830337",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1706453943659 = glueContext.write_dynamic_frame.from_options(
    frame=FilterResearchPurposes_node1706453830337,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://raw-data-udacity-project/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1706453943659",
)

job.commit()
