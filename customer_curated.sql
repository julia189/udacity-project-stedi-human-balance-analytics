CREATE EXTERNAL TABLE `customer_curated`(
  `serialnumber` string COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `customername` string COMMENT 'from deserializer', 
  `user` string COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `lastupdatedate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://raw-data-udacity-project/customer/curated/'
TBLPROPERTIES (
  'CreatedByJob'='accelerometer_trusted_to_curated', 
  'CreatedByJobRun'='jr_06c41bdb599c2b49125f9e0a7b6f97c5f5dfbe65362d4d891321abd4b59e88ff', 
  'classification'='json')