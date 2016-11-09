#!/usr/bin/env python3

# Forked From MIT License
# Modified For Python3
# V1.0, created 2014-01-17
#
# Requires the Python Elasticsearch client
# http://www.elasticsearch.org/blog/unleash-the-clients-ruby-python-php-perl/#python
#

import elasticsearch
import csv
import random
import unicodedata
import json
import simplejson
from elasticsearch import Elasticsearch, RequestsHttpConnection
import sys
import time
import datetime


def timeStamped(fname, fmt='%Y-%m-%d-%H-%M-%S_{fname}'):
    return datetime.datetime.now().strftime(fmt).format(fname=fname)


#replace with the IP address of your Elasticsearch node
es = Elasticsearch(
        ['192.168.2.221:9200'],
        connection_class=RequestsHttpConnection,
        http_auth=('admin', '!^94AEzdb'),
        use_ssl=False,
        verify_certs=False)

# Replace the following Query with your own Elastic Search Query
res = es.search(index="myindex", timeout="20m", request_timeout=10000, body=
    {
"from" : 0,
"size" : 500000,
"fields" : ["@timestamp", "to_addr", "session_event", "mno", "from_addr"],
"query" : { "term" : {"session_event" : "new"} 
}}
),
size=20  #this is the number of rows to return from the query... to get all queries, run script, see total number of hits, then set euqual to number >= total hits
random.seed(1)
sample = res[0]['hits']['hits']     #(res["hits"][0]["hits"])
#comment previous line, and un-comment next line for a random sample instead
#randomsample = random.sample(res['hits']['hits'], 5);  #change int to RANDOMLY SAMPLE a certain number of rows from your query  

print ("Got %d Hits:" % res[0]['hits']['total'])

with open(timeStamped('esdata.csv'), 'w', newline='') as csvfile:   #set name of output file here
   filewriter = csv.writer(csvfile, delimiter='\t',  # we use TAB delimited, to handle cases where freeform text may have a comma
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
   # create header row
   filewriter.writerow(["_id", "to_addr", "from_addr", "session_event", "@timestamp", "mno"])    #change the column labels here
   for hit in sample:   #switch sample to randomsample if you want a random subset, instead of all rows
       try:             #try catch used to handle unstructured data, in cases where a field may not exist for a given hit
           col1 = hit["_id"]
       except Exception as e:
           col1 = ""
       try:
           col2 = hit["fields"]["to_addr"]  #replace these nested key names with your own
       except Exception as e:
           col2 = ""
       try:
           col3 = hit["fields"]["from_addr"]  #replace these nested key names with your own
       except Exception as e:
           col3 = ""
       try:
           col4 = hit["fields"]["session_event"]  #replace these nested key names with your own
       except Exception as e:
           col4 = ""
       try:
           col5 = hit["fields"]["@timestamp"]  #replace these nested key names with your own
       except Exception as e:
           col5 = ""
       try:
           col6 = hit["fields"]["mno"]  #replace these nested key names with your own
       except Exception as e:
           col6 = ""



       filewriter.writerow([col1,col2,col3,col4,col5,col6])

