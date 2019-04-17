**awsInterface** allows you connect seamlessly a local machine to [Elasticsearch](https://www.elastic.co/) database running on an EC2 instance and S3 in AWS with minimal code. With the host name set to localhost, it will also work with Elasticsearch database running on a local machine.

#### Table of contents
1. [esInterface](https://github.com/jainachin/awsInterface#esinterface)
2. [s3Interface](https://github.com/jainachin/awsInterface#s3interface)
3. [Installation](https://github.com/jainachin/awsInterface#installation)


### esInterface

This is a wrapper built around the Python client for Elasticsearch: [elasticsearch](https://elasticsearch-py.readthedocs.io/en/master/). It provided a Python interface to read and write data to Elasticsearch, running on either an EC2 intance in AWS or a local machine. The following methods are supported:

    INDEX: defined methods for es index
    -- create_index new index
    -- delete existing index
    -- check if an index exists in the database

    DOCUMENTS: defined methods for es documents for a given index
    -- read documents
    -- write documents
    -- update documents
    -- delete documents in a time range
    -- search most recent document
    -- search min or max over a specified field in a document
    -- search min or max over a specified nested field in a document
    -- download documents in bulk
    -- download documents as pandas DataFrame
    -- download bulk data by start and end time
    -- watch a local document for changes and push the modifications to elasticsearch

#### How to use?

Connect to the database. Enable `print_updates=True` to print actions to command line. It is assumed that documents are indexed with time, so you may want to specify your `time_format` and specify time in the same format in the following code.
```
from awsInterface import esInterface

host = 'localhost'				# or you could specify the IP of EC2 if ES is in EC2
username = 'your_user'			# user name for logging into Elasticsearch
password = 'your_password'		# password for logging into Elasticsearch
es = esInterface(host=host, user=username, password=password, print_updates=True, time_format="%Y-%m-%d %H:%M:%S")
```

Check if you are connected.
```
es.es.ping()
```

Create a new index using a json file specifying the mapping. `your_schema.json` should contain a field `'time'` in the same format as `'time_format'`.
```
es.create_index(index_name='your_index', schema_name='your_schema.json')
```

Delete an existing index.
```
es.delete_index(index_name='your_index')
```

Check if an index exists.
```
es.index_exists(index_name='your_index')

```

Find all docs in a time range.
```
docs = es.read(index_name='your_index', time_start='t1_as_string', time_end='t2_as_string')
```

Publish a new document, a `dict` in the format of `'your_schema.json'`. So it must also contain a field `'time'`. If there already exists a document at `'time'` in `'your_doc'`, the existing document in ElasticSearch will be updated. Otherwise, a new entry is created.

```
es.write(index_name='your_index', document='your_doc'):
```

Delete all docs in a time range.
```
es.delete(index_name='your_index', time_start='t1_as_string', time_end='t2_as_string')
```

### s3Interface

This is a wrapper built around the Python client for S3: [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html). It provided a Python interface to read and write data to S3. The following methods are supported:

    -- list all buckets
    -- create a new bucket
    -- empty an existing bucket
    -- delete an existing bucket
    -- add a file to an existing bucket
    -- download a file from an existing bucket

#### How to use?
Connect to S3 and enable `print_updates=True` (if you wish) to print actions to command line.
```
from awsInterface import s3Interface
s3 = s3Interface(aws_access_key='your_access_key', aws_secret_key='your_secret_key, print_updates=True)
```

List all buckets.
```
s3.list_buckets()
```

Create a new bucket.
```
s3.create_bucket(bucket_name='your_bucket_name')
```

Delete everything in a bucket.
```
s3.empty_bucket(bucket_name='your_bucket_name')
```

Delete a bucket.
```
s3.delete_bucket(bucket_name='your_bucket_name')
```

Add a file to bucket.
```
s3.add_to_bucket(bucket_name='your_bucket_name', file_path='path_to_file', file_name_in_s3='file_name')
```

Download a specfic file from bucket.
```
es.download_from_bucket(bucket_name='your_bucket_name', file_name_in_s3='file_name')
```

### Installation

Clone the repository and cd into it
```
git clone https://github.com/jainachin/awsInterface.git
cd awsInterface
```
Install using pip
```
pip install .
```