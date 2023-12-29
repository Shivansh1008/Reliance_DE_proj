import boto3
import csv
import sys
import os
# Ensure the project root is in the sys.path
sys.path.append('/config/workspace/Manish_DE_proj')
from resources.dev import config
from src.main.utility.logging_config import *


aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

class S3ClientProvider:
    def __init__(self, aws_access_key=None, aws_secret_key=None):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )
        self.s3_client = self.session.resource('s3')

    def get_client(self):
        return self.s3_client

S3Client= S3ClientProvider(aws_access_key,aws_secret_key)
s3_client=S3Client.get_client()
#response=s3_client.list_buckets()
#print(response)
#for bucket in s3_client.buckets.all():
    #print(bucket.name)
#
#s3_client.Bucket("reliance-mart-de-project").upload_file(Filename='/config/workspace/spark_project/file_from_s3/sales_data.csv',Key='sales_data')
#list_file=[]
#for obj in s3_client.Bucket(config.bucket_name).objects.filter(Prefix=config.s3_source_directory):
    #print(obj.key)
    #print("Hello")
    #if not obj.key.endswith('/'):
        #Print(obj.key)
        #list_file.append(obj.key)
#print(list_file)
#obj=s3_client.Bucket("reliance-mart-de-project").Object('sales_data').get()
#record=obj['Body'].read().decode('utf-8')

#print(record.splitlines()[:4])

#s3_client.Bucket("reliance-mart-de-project").download_file(Key='sales_data',Filename='/config/workspace/spark_project/file_from_s3/sales_data_s3')
#its for seession not resource s3_client.delete_object(Bucket="reliance-mart-de-project", Key='sales_data')
#obj = s3_client.Object("reliance-mart-de-project", 'sales_data')
#obj.delete()


#s3_client.Bucket("reliance-mart-de-project").download_file(Key='sales_data/Data Dictionary.csv',Filename='/config/workspace/spark_project/file_from_s3/Data Dictionary.csv')
bucket_name = "reliance-mart-de-project"
source_prefix = "sales_data/"
destination_prefix = "sales_data_error/"

objects_to_delete = []

def move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix):
    for obj in s3_client.Bucket(bucket_name).objects.filter(Prefix=source_prefix):
        source_key = obj.key
        print(source_key)
        source_object = s3_client.Object(bucket_name, source_key)

        # Construct the destination key based on the source key and destination prefix
        destination_key = destination_prefix + source_key[len(source_prefix):]
        destination_object = s3_client.Object(bucket_name, destination_key)

        # Copy the object from source to destination
        destination_object.copy_from(CopySource={'Bucket': bucket_name, 'Key': source_key})

        objects_to_delete.append({'Key': source_key})

         # Delete the source objects after successful copy
        if objects_to_delete:
            s3_client.meta.client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})

    return f"Data moved successfully from {source_prefix} to {destination_prefix}"



message=move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")