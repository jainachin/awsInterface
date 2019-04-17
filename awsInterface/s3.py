"""
Copyright 2019 Achin Jain (achinj@seas.upenn.edu)

Interfaces AWS S3 with a local machine, can be used to:

    -- list all buckets
    -- create a new bucket
    -- empty an existing bucket
    -- delete an existing bucket
    -- add a file to an existing bucket
    -- download a file from an existing bucket

"""

import os
import boto3
from retrying import retry
from botocore import exceptions as s3excepts


def catch_s3_error(exception):
    """
    Return True if we should retry when it's one of the following S3 exceptions, False otherwise
    check: 
    """

    if isinstance(exception, s3excepts.ClientError):
        print("trying again, S3 is ImproperlyConfigured")
        return True

    else:
        return False


class s3Interface:
    """
    Main class to interface AWS s3.

    """

    def __init__(self, aws_access_key, aws_secret_key, print_updates=True):

        self.client = boto3.client('s3',
                               aws_access_key_id=aws_access_key,
                               aws_secret_access_key=aws_secret_key,
                               # aws_session_token=SESSION_TOKEN,
                               )
        self.resource = boto3.resource('s3',
                               aws_access_key_id=aws_access_key,
                               aws_secret_access_key=aws_secret_key,
                               # aws_session_token=SESSION_TOKEN,
                               )
        self.print_updates = print_updates


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_s3_error)
    def list_buckets(self):
        """
        get a list of all bucket names from the response
        """
        response = self.client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        
        return buckets


    def create_bucket(self, bucket_name):
        """
        create a new bucket
        """
        self.client.create_bucket(Bucket=bucket_name)

        if self.print_updates:
            print("created a new bucket:" + bucket_name)


    def empty_bucket(self, bucket_name):
        """
        delete everything in a new bucket
        """
        bucket = self.resource.Bucket(bucket_name)
        for key in bucket.objects.all():
            key.delete()

        if self.print_updates:
            print("bucket:" + bucket_name + "is now empty")


    def delete_bucket(self, bucket_name):
        """
        delete the bucket
        """
        self.empty_bucket(bucket_name)
        bucket = self.client.Bucket(bucket_name)
        bucket.delete()

        if self.print_updates:
            print("bucket:" + bucket_name + "deleted")


    def add_to_bucket(self, bucket_name, file_path, file_name_in_s3=None):
        """
        add new file to bucket
        """
        if file_name_in_s3 is None:
            file_name_in_s3 = os.path.basename(file_path)
        self.client.upload_file(file_path, bucket_name, file_name_in_s3)

        if self.print_updates:
            print(file_name_in_s3 + "added to " + bucket_name)


    def download_from_bucket(self, bucket_name, file_name_in_s3, file_name_local=None):
        """
        download specified file from bucket
        """
        if file_name_local is None:
            file_name_local = os.path.join(os.getcwd(),file_name_in_s3)
        bucket = self.resource.Bucket(bucket_name)
        bucket.download_file(file_name_in_s3, file_name_local)