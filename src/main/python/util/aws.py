import pytz
import boto3
import pyodbc
from datetime import date, timedelta, datetime, time

aws_access_key_id = ''
aws_secret_access_key = ''
region ='us-east-1'

ec2Ids = ['i-cd330a66']

session = boto3.Session(profile_name='')
ec2Instance = session.client('ec2')

print ec2Instance.start_instances(InstanceIds=ec2Ids)

# print ec2Instance.stop_instances(InstanceIds=['i-cd330a66'])
print ec2Instance.describe_instances(InstanceIds=['i-cd330a66'])
