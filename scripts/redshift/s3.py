import boto3
from os import path, environ

class S3():
    def __init__(self):
        self.client = boto3.client(
            's3',
            aws_access_key_id=environ['S3_ACCESS_KEY'],
            aws_secret_access_key=environ['S3_SECRET_KEY']
        )

        self.resource = boto3.resource(
            's3',
            aws_access_key_id=environ['S3_ACCESS_KEY'],
            aws_secret_access_key=environ['S3_SECRET_KEY']
        )

    def read_csv(self, csv_path):
        csv = self.client.get_object(Bucket='dataviva-etl', Key=csv_path)['Body']

        return csv

    def get_keys(self, prefix):
        keys = []

        for obj in self.client.list_objects(Bucket='dataviva-etl', Prefix=prefix)['Contents']:
            if obj['Size'] > 0:
                keys.append(obj['Key'])

        return keys
