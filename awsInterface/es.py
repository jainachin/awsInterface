"""
Interfaces Elasticsearch with a local machine, can be used to:

    INDEX (methods for es index)
    -- create new index
    -- delete existing index
    -- check if an index exists in the database

    DOCUMENTS (methods for es documents for a given index)
    -- read documents
    -- write documents
    -- update documents
    -- delete documents in a time range
    -- search documents in a time range
    -- search most recent document
    -- search min or max over a specified field in a document
    -- search min or max over a specified nested field in a document
    -- download documents in bulk
    -- download documents as pandas DataFrame
    -- download bulk data by start and end time
    -- watch a local document for changes and push the modifications to elasticsearch

"""

import sys
import json
import time
import numpy as np
import pandas as pd
from retrying import retry
import datetime as datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from elasticsearch import exceptions as esexcepts
from elasticsearch.helpers import scan as esscan
from elasticsearch import Elasticsearch, RequestsHttpConnection

class NumpyEncoder(json.JSONEncoder):
    """
    Overrides existing 'JSONEncoder' to handle numpy ndarray objects while serializing json files.

    """

    def default(self, obj):
        if isinstance(obj, (np.int_, np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def catch_es_error(exception):
    """
    Return True if we should retry when it's one of the following Elasticsearch exceptions, False otherwise
    check: http://elasticsearch-py.readthedocs.io/en/master/exceptions.html
    """

    if isinstance(exception, esexcepts.ImproperlyConfigured):
        print("trying again, elasticsearch is ImproperlyConfigured")
        return False

    elif isinstance(exception, esexcepts.ElasticsearchException):
        print("trying again, faced ElasticsearchException")
        return False

    elif isinstance(exception, esexcepts.SerializationError):
        print("trying again, faced SerializationError")
        return False

    elif isinstance(exception, esexcepts.ConnectionError):
        print("trying again, faced ConnectionError")
        return True

    elif isinstance(exception, esexcepts.RequestError):
        print("trying again, faced RequestError")
        return False

    else:
        return False


class esInterface:
    """
    Main class to interface elasticsearch.

    """

    def __init__(self, host, username=None, password=None, print_updates=True, time_format="%Y-%m-%d %H:%M:%S"):


        self.es = Elasticsearch(
                    hosts = [{'host': host, 'port': 9200}],
                    http_auth = (username, password),
                    use_ssl = False,
                    verify_certs = False,
                    connection_class = RequestsHttpConnection)
                    # serializer=SetEncoder())
        self.print_updates = print_updates
        self.time_format = time_format


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def write(self, index_name, document, search_by="time"):
        """
        publish a document to a given index name
        """

        if search_by is "time":
            res = self.search_by_time(index_name, document['time'])
        else:
            raise NotImplementedError("haha, contact Achin :)")

        if res['hits']['total']>0:
            for doc in res['hits']['hits']:
                self.es.update(index=doc['_index'], doc_type=doc['_type'], id=doc['_id'], body={"doc": document})
                if self.print_updates: 
                    print("data updated in {} for at {}".format(index_name, document['time']))
                    print(document)
                    print('\n')
        else:
            self.es.index(index=index_name, doc_type="record", body=document)
            if self.print_updates: 
                print("new data published to {}".format(index_name))
                print(document)
                print('\n')


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def read(self, index_name, time_start, time_end=None, search_by="range"):
        """
        returns all docs between specified time stamps
        time is required as string
        default: [time_start, time_start+5s]
        """

        if time_end is None:
            delta = 5
            time_end = datetime.datetime.strptime(time_start, self.time_format) + datetime.timedelta(seconds=delta)
            time_end = time_end.__format__(self.time_format)

        if search_by is "time":
            res = self.search_by_time(index_name, time_start)
            documents = [doc["_source"] for doc in res['hits']['hits']]

        elif search_by is "range":
            res = self.search_by_range(index_name, time_start=time_start, time_end=time_end)
            documents = [doc["_source"] for doc in res['hits']['hits']]

        else:
            raise NotImplementedError("seach_by should be either 'time' or 'range'")
        
        if self.print_updates: 
            print("read the following documents from {}".format(index_name))
            print(documents)
            print('\n')

        return documents


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def _delete(self, index_name, time, search_by="time"):
        """
        time is required as string
        delete a document at specified time
        """

        if search_by is "time":
            res = self.search_by_time(index_name, time)
        else:
            raise NotImplementedError("haha, contact Achin :)")

        if res['hits']['total']>0:
            for doc in res['hits']['hits']:
                self.es.delete(index=doc['_index'], doc_type=doc['_type'], id=doc['_id'])
            if self.print_updates: 
                print("deleted {} documents from {}".format(res['hits']['total'], index_name))
                print('\n')
        else:
            if self.print_updates: 
                print("no documents to be deleted")
                print('\n')


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def delete(self, index_name, time_start, time_end, n_samples=1000):
        """
        time is required as string
        delete all documents in specified time range
        """

        time_start = datetime.datetime.strptime(time_start, self.time_format)
        time_end = datetime.datetime.strptime(time_end, self.time_format)
        lte = time_end
        gte = time_start
        res = self.es.search(index=index_name, doc_type="record", size=n_samples,
                            body={"query": {"range" : { "time" : { "gte": gte.__format__(self.time_format), "lte": lte.__format__(self.time_format)}}}})

        if res['hits']['total']>0:
            for doc in res['hits']['hits']:
                self.es.delete(index=doc['_index'], doc_type=doc['_type'], id=doc['_id'])
            if self.print_updates: 
                print("deleted {} documents from {}".format(res['hits']['total'], index_name))
                print('\n')
        else:
            if self.print_updates: 
                print("no documents to be deleted")
                print('\n')


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def search_by_time(self, index_name, time, delta=5):
        """
        time is required as string
        default delta is 5 seconds
        default search window is [time-delta : time]

        """

        time = datetime.datetime.strptime(time, self.time_format)
        lte = time
        gte = time - datetime.timedelta(seconds=delta)
        res = self.es.search(index=index_name, doc_type="record", body={"query": {"range" : { "time" : { "gte": gte.__format__(self.time_format), "lte": lte.__format__(self.time_format)}}}})
        # documents = [doc["_source"] for doc in res['hits']['hits']]
        return res


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def search_by_range(self, index_name, time_start, time_end):
        """
        time is required as string
        search window is [time_start : time_end]

        """

        time_start = datetime.datetime.strptime(time_start, self.time_format)
        time_end = datetime.datetime.strptime(time_end, self.time_format)
        lte = time_end
        gte = time_start
        res = self.es.search(index=index_name, doc_type="record", body={"query": {"range" : { "time" : { "gte": gte.__format__(self.time_format), "lte": lte.__format__(self.time_format)}}}})
        # documents = [doc["_source"] for doc in res['hits']['hits']]
        return res


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def search_latest_doc(self, index_name):
        """
        returns the most recent document for the specified index
        """

        search_body = {"size": 1, "sort": { "time": "desc"}, "query": { "match_all": {}}}
        res = self.es.search(index=index_name, doc_type="record", body=search_body)
        if res["hits"]["total"]>0:
            return res["hits"]["hits"][0]["_source"]
        else:
            return None


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def search_min_value(self, index_name, field_name):
        """
        returns the document with min value of a specified field
        """

        search_body = {"size": 1, "sort": { field_name: "asc"}, "query": { "match_all": {}}}
        res = self.es.search(index=index_name, doc_type="record", body=search_body)
        if res["hits"]["total"]>0:
            return res["hits"]["hits"][0]["_source"]
        else:
            return None            


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def search_nested_min_value(self, index_name, field_name, path):
        """
        returns the document with min value of a specified nested field
        """

        search_body = {"size": 1, "sort": { field_name: {"order":"asc", "nested_path":path}}, "query": { "nested": { "path": path, "query": {"match_all": {}}}}}
        res = self.es.search(index=index_name, doc_type="record", body=search_body)
        if res["hits"]["total"]>0:
            return res["hits"]["hits"][0]["_source"]
        else:
            return None


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def search_max_by_array(self, index_name, path, value, metric):
        """
        searches the document for a specified array
        TODO: this code just checks for the first value and length of the array
        """

        query =  {
                "bool": {
                    "must": {
                        "term": {
                            path: str(value[0])
                            }
                        },
                    "filter" : {
                        "script" : {
                            "script" : "doc['" + path + "'].values.size() == " + str(len(value))
                        }
                    }
                }
              }

        search_body = {"size": 1, "sort": { metric: "desc"}, "query": query}
        res = self.es.search(index=index_name, doc_type="record", body=search_body)
        if res["hits"]["total"]>0:
            return res["hits"]["hits"][0]["_source"]
        else:
            return None


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def search_max_value(self, index_name, field_name):
        """
        returns the document with max value of a specified field
        """

        search_body = {"size": 1, "sort": { field_name: "desc"}, "query": { "match_all": {}}}
        res = self.es.search(index=index_name, doc_type="record", body=search_body)
        if res["hits"]["total"]>0:
            return res["hits"]["hits"][0]["_source"]
        else:
            return None


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def search_nested_max_value(self, index_name, field_name, path):
        """
        searches the document with max value of a nested specified field
        """

        search_body = {"size": 1, "sort": { field_name: {"order":"desc", "nested_path":path}}, "query": { "nested": { "path": path, "query": {"match_all": {}}}}}
        res = self.es.search(index=index_name, doc_type="record", body=search_body)
        if res["hits"]["total"]>0:
            return res["hits"]["hits"][0]["_source"]
        else:
            return None  


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def create_index(self, index_name, schema_name, index_type="record"):
        """
        schema_name: name of JSON file containing the schema (with .json)
        """
        
        with open(schema_name, 'r') as f:
            schema = json.load(f)

            mapping = {
                "mappings": {
                    index_type : {
                        "properties": schema
                    }
              }
            }
        
        self.es.indices.create(index=index_name, body=mapping)
        if self.print_updates: 
            print("created a new index " + index_name + " with mapping:")
            print(mapping)
            print('\n')


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def delete_index(self, index_name):
        """
        deletes specified index
        """

        try:
            self.es.indices.delete(index=index_name)
            if self.print_updates: 
                print("deleted index {}".format(index_name))
                print('\n')
        except:
            print("{} not found, cannot be deleted".format(index_name))
            print('\n')


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def index_exists(self, index_name):
        return self.es.indices.exists(index=index_name)


    @retry(wait_fixed=2000, stop_max_delay=20000, retry_on_exception=catch_es_error)
    def download(self, index_name, time_start, time_end, n_samples=1000):

        """
        time_start      : starting time for the query (required as string)
        time_end        : end time for the query (required as string)
        n_samples       : max number of documents that will be downloaded

        """

        time_start = datetime.datetime.strptime(time_start, self.time_format)
        time_end = datetime.datetime.strptime(time_end, self.time_format)
        lte = time_end
        gte = time_start
        res = self.es.search(index=index_name, doc_type="record", size=n_samples,
                            body={"query": {"range" : { "time" : { "gte": gte.__format__(self.time_format), "lte": lte.__format__(self.time_format)}}}})
        
        documents = [doc["_source"] for doc in res['hits']['hits']]
        return documents


    def to_DataFrame(self, index_name, time_start, time_end):

        """
        returns all documents in pandas DataFrame
        time_start      : starting time for the query (required as string)
        time_end        : end time for the query (required as string)
        search window is [time_start : time_end]

        """

        time_start = datetime.datetime.strptime(time_start, self.time_format)
        time_end = datetime.datetime.strptime(time_end, self.time_format)
        lt = time_end
        gte = time_start
        query = {"query": {"range" : { "time" : { "gte": gte.__format__(self.time_format), "lte": lt.__format__(self.time_format)}}}}
        doc_type = "record"
        res = esscan(self.es, index=index_name, query=query)
        
        documents = []
        for item in res:
            documents.append(item["_source"])
        
        data = pd.DataFrame(documents)
        data.set_index(data["time"], inplace=True)
        data.drop(columns=["time"], inplace=True)
        data.sort_index(inplace=True)
        return data


    def watch_file(self, file_name, file_path, index_name):
        """
        This function watches a given 'file_name' for a change.
        The variable 'file_name' must correspond to the schema defined for the index 'index_name'.
        When 'file_name' is modified, the updated file is published as a doc to 'index_name'.

        """

        es = self
        class MyHandler(FileSystemEventHandler):
            def on_modified(self, event):
                if event.src_path == file_path + file_name:

                    with open(file_name, 'r') as f:
                        document = json.load(f)
                    es.write(index_name, document)

        event_handler = MyHandler()
        observer = Observer()
        observer.schedule(event_handler, path=file_path, recursive=True)
        observer.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()


    def serialize(self, document):
        """
        Converts numpy arrays into list of numbers so they can be serialized by elasticsearch.

        """

        document = json.dumps(document, cls=NumpyEncoder)
        document = json.loads(document)
        return document