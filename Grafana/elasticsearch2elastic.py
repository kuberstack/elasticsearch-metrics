#!/usr/bin/env python
import datetime
import time
import json
import os
import sys
import certifi

from elasticsearch import Elasticsearch

es_host = os.environ.get('ES_HOST', 'elasticsearch')
es_user = os.environ.get('ES_USER', 'elastic')
es_pwd = os.environ.get('ES_PWD', 'changeme!')

es = Elasticsearch(
    [es_host],
    http_auth=(es_user, es_pwd),
    port=443,
    use_ssl=True,
    verify_certs=True,
    ca_certs=certifi.where()
    )

interval = int(os.environ.get('ES_METRICS_INTERVAL', '60'))
elasticIndex = os.environ.get('ES_METRICS_INDEX_NAME', 'elasticsearch_metrics')

def fetch_clusterhealth():
    try:
        utc_datetime = datetime.datetime.utcnow()
        endpoint = "/_cluster/health"
        data = json.dumps(es.cluster.health())
        jsonData = json.loads(data)
        clusterName = jsonData['cluster_name']
        jsonData['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
        if jsonData['status'] == 'green':
            jsonData['status_code'] = 0
        elif jsonData['status'] == 'yellow':
            jsonData['status_code'] = 1
        elif jsonData['status'] == 'red':
            jsonData['status_code'] = 2
        post_data(jsonData)
        return clusterName
    except IOError as err:
        print "IOError: Maybe can't connect to elasticsearch."
        clusterName = "unknown"
        return clusterName

def fetch_nodestats(clusterName):
    utc_datetime = datetime.datetime.utcnow()
    data = json.dumps(es.cat.nodes(v='true', h='n'))
    nodes = json.loads(data)[1:-1].strip().split('\n')
    for node in nodes:
        data = json.dumps(es.nodes.stats())
        jsonData = json.loads(data)
        nodeID = jsonData['nodes'].keys()
        try:
            jsonData['nodes'][nodeID[0]]['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
            jsonData['nodes'][nodeID[0]]['cluster_name'] = clusterName
            newJsonData = jsonData['nodes'][nodeID[0]]
            post_data(newJsonData)
        except:
            continue

def fetch_indexstats(clusterName):
    utc_datetime = datetime.datetime.utcnow()
    data = json.dumps(es.indices.stats())
    jsonData = json.loads(data)
    jsonData['_all']['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
    jsonData['_all']['cluster_name'] = clusterName
    post_data(jsonData['_all'])

def post_data(data):
    utc_datetime = datetime.datetime.utcnow()
    index_period = utc_datetime.strftime("%Y.%m.%d")
    current_index = elasticIndex + "-" + index_period
    es.index(index=current_index, doc_type='stat', body=json.dumps(data))

def main():
    clusterName = fetch_clusterhealth()
    if clusterName != "unknown":
        fetch_nodestats(clusterName)
        fetch_indexstats(clusterName)


if __name__ == '__main__':
    try:
        nextRun = 0
        while True:
            if time.time() >= nextRun:
                nextRun = time.time() + interval
                now = time.time()
                main()
                elapsed = time.time() - now
                print "Total Elapsed Time: %s" % elapsed
                timeDiff = nextRun - time.time()

                # Check timediff , if timediff >=0 sleep, if < 0 send metrics to es
                if timeDiff >= 0:
                    time.sleep(timeDiff)

    except KeyboardInterrupt:
        print 'Interrupted'
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
