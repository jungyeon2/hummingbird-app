import pytz
import boto3
import pyodbc
from datetime import date, timedelta, datetime, time

aws_access_key_id = 'AKIAIVZGVMJMBVNKAIJA'
aws_secret_access_key = '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'
region ='us-east-1'

ec2Ids = ['i-cd330a66']

session = boto3.Session(profile_name='jyoon')
ec2Instance = session.client('ec2')

print ec2Instance.start_instances(InstanceIds=ec2Ids)

# print ec2Instance.stop_instances(InstanceIds=['i-cd330a66'])
print ec2Instance.describe_instances(InstanceIds=['i-cd330a66'])
#
# print s3.get_available_resources()
# print s3.get_available_services()
#
# jyoon = boto3.Session(profile_name='jyoon')
# s3 = boto3.resource('s3')
#
# for bucket in s3.buckets.all():
#     print bucket.name
#
