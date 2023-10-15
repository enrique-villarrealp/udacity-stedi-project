CREATE EXTERNAL TABLE `evillarreal_stedi_landing.customer_landing`(
  `customername` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `phone` bigint COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `lastupdatedate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` double COMMENT 'from deserializer', 
  `sharewithpublicasofdate` double COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='birthDay,customerName,email,lastUpdateDate,phone,registrationDate,serialNumber,shareWithFriendsAsOfDate,shareWithPublicAsOfDate,shareWithResearchAsOfDate') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://evillarreal-udacity-stedi-lakehouse/landing/customer_landing/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='evillarreal_stedi_landing', 
  'averageRecordSize'='338', 
  'classification'='json', 
  'compressionType'='none', 
  'objectCount'='1', 
  'recordCount'='958', 
  'sizeKey'='323830', 
  'typeOfData'='file')
