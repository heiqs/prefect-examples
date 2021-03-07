import os
import boto3
import pandas as pd
from io import StringIO
from prefect import task, Flow
import prefect


aws_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')

client = boto3.client(
    's3',
    aws_access_key_id=aws_id,
    aws_secret_access_key=aws_secret
)


@task
def read_csv(bucket_name: str, object_key: str):
    s3 = boto3.resource(u's3')
    bucket = s3.Bucket(bucket_name)
    obj = bucket.Object(object_key)
    csv_obj = obj.get()
    # csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_string))
    return df.head(5)


@task
def write_csv(df, bucket_name: str, outfile: str):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)

    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, outfile).put(Body=csv_buffer.getvalue())


with Flow('s3_read') as flow:
    df = read_csv('testmydask', 'data.csv')
    write_csv(df, 'testmydask', 'data-head.csv')
