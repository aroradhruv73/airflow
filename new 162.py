import argparse
import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import boto3

s3_res = boto3.resource('s3')

def mysparksql_script(filePath,startdate,enddate,execdate,env):
    
    parquetfilepath = f's3://gd-ckpetlbatch-{env}-analytic/advertising_analytic/cj_bonus_feed_upload_parquet/'
    s3_path = f"{filePath}{execdate}/"
    #filename = f"cj_bonus_feed_upload_{execdate}.csv"
    file_name = execdate + "_bonus.csv"
    bucket_name = f'gd-ckpetlbatch-{env}-analytic'   
    df = spark.sql(""" 
            SELECT 
                cj.ad_id                                                                            AS ad_id
                , cj.website_id                                                                     AS website_id
                , cj.cid                                                                            AS cid
                , cj.commission_usd_amt                                                             AS commission_usd_amt
                , cj.action_id                                                                      AS action_id
                , cj.sid                                                                            AS sid
                , FROM_UNIXTIME(UNIX_TIMESTAMP(cj.posting_date,'yyyy-MM-dd'), 'MM/dd/yyyy')         AS paid_order_ts
                , cj.order_id                                                                       AS order_id
            FROM 
                analytic_local.cj_bonus_feed cj
                LEFT OUTER JOIN analytic_local.cj_bonus_feed_transmitted transmitted
                    ON cj.order_id = transmitted.order_id 
            WHERE
                cj.posting_date >= DATE_ADD('{stdate}', -4)
                AND cj.posting_date < '{endate}'
                AND transmitted.order_id IS NULL
                AND cj.publisher_cid IN (3604767, 2901993)
                AND cj.posting_date >= '2021-01-27'
          """.format(stdate = startdate,endate = enddate))	

    df.write.mode("overwrite").format("parquet").save(parquetfilepath)
    
    spark.sql(f"""CREATE EXTERNAL TABLE IF NOT EXISTS analytic_local.cj_bonus_feed_upload(
                     ad_id string, 
                     website_id string, 
                     cid int, 
                     commission_usd_amt decimal(4,1), 
                     action_id string, 
                     sid string, 
                     paid_order_ts string, 
                     order_id string)
                 ROW FORMAT SERDE 
                    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
                 STORED AS INPUTFORMAT 
                    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
                 OUTPUTFORMAT 
                    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                 LOCATION
                    's3://gd-ckpetlbatch-{env}-analytic/advertising_analytic/cj_bonus_feed_upload_parquet/'""")
    
    #df.coalesce(1).write.csv(s3_path + filename, header="true", mode="overwrite")
    df.coalesce(1).write.csv(s3_path, header="false", mode="overwrite")
    
    s3 = boto3.client('s3')
    
    ## deleting success file for a date folder 
    successfile = f'advertising_analytic/cj_bonus_feed_upload_file/{execdate}/_SUCCESS'
    s3.delete_object(Bucket=bucket_name, Key=successfile)
    
    response = s3.list_objects(
        Bucket=bucket_name,
        MaxKeys=10,
        Prefix=f'advertising_analytic/cj_bonus_feed_upload_file/{execdate}/',
        Delimiter='/'
        )
    name = response["Contents"][0]["Key"]
    copy_source = {'Bucket': bucket_name, 'Key': name}

    s3.copy_object(Bucket=bucket_name, CopySource=copy_source, 
    Key=f'advertising_analytic/cj_bonus_feed_upload_file/{execdate}/' + file_name)

    s3.delete_object(Bucket=bucket_name, Key=name)
	
	## Add metadata in the beginning of the csv file
	fileObj = s3_res.Object(bucket_name, f'advertising_analytic/cj_bonus_feed_upload_file/{execdate}/' + file_name).get()['Body']
	chunk = fileObj.read()
	chunk = '&CID=1513033' + '\n' + '&SUBID=7675'  +  \n  +  '&DATEFMT=MM/DD/YYYY' + '\n' + chunk
	
	s3.put_object(Body=appended_data, Bucket=bucket_name, Key=f'advertising_analytic/cj_bonus_feed_upload_file/{execdate}/' + file_name)
    
if __name__ == '__main__':

    # Create the parser
    parser = argparse.ArgumentParser(description='Location of output file on S3')

    # Add the arguments
    parser.add_argument('-p', '--path', dest='filePath', type=str, help='S3 output path')
    parser.add_argument('--startdate', "-s", type=str, help='date', required=True)
    parser.add_argument('--enddate', "-e", type=str, help='date', required=True)
    parser.add_argument('--execdate', "-d", type=str, help='date', required=True)
    parser.add_argument('--env', "-en", type=str, help='environmentVariable', required=True)
    
    # Execute the parse_args() method
    args = parser.parse_args()
    environment_var = args.env
    
    spark = SparkSession.builder.enableHiveSupport().config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").getOrCreate()
    
    spark.sql(f"""CREATE EXTERNAL TABLE IF NOT EXISTS analytic_local.cj_bonus_feed_transmitted(
                    order_id string, 
                    posting_date string)
    ROW FORMAT SERDE 
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
    STORED AS INPUTFORMAT 
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
    OUTPUTFORMAT 
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      's3://gd-ckpetlbatch-{environment_var}-analytic/advertising_analytic/cj_bonus_feed_transmitted/'""")
          
    if not args.filePath.endswith('/'):
       args.filePath = args.filePath + '/'
    
    mysparksql_script(args.filePath,args.startdate,args.enddate,args.execdate,args.env)
