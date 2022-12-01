import base64
import http.client
import json
import logging
import os
import time
import urllib.parse

import azure.functions as func
import jmespath
import requests
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from gremlin_python.driver import client, protocol, serializer

subscription_key = os.environ['CognitiveServiceAPIKey']
api_endpoint = os.environ['CognitiveServiceEndPoint']
ENDPOINT = os.environ['CosmosEndPoint']
DATABASE = os.environ['CosmosGraphDB']
COLLECTION = os.environ['CosmosGraphCollection']
PRIMARY_KEY = os.environ['CosmosPrimaryKey']


# headers = {
#     # Request headers.
#     'Content-Type': 'application/json',
#     'Ocp-Apim-Subscription-Key': subscription_key,
# }

# params = urllib.parse.urlencode({
#     # Request parameters.
#     'api-version': '2022-10-01-preview',
# })

# Authenticate the client using your key and endpoint 
def authenticate_client():
    ta_credential = AzureKeyCredential(subscription_key)
    text_analytics_client = TextAnalyticsClient(
            endpoint=api_endpoint, 
            credential=ta_credential)
    return text_analytics_client

def cleanup_graph(gremlin_client):
    callback = gremlin_client.submitAsync("g.V().drop()")
    if callback.result() is not None:
        logging.info("Cleaned up the graph!")

def insert_vertices(gremlin_client, VERTICES):
    for vertex in VERTICES:
        callback = gremlin_client.submitAsync(vertex)
        if callback.result() is not None:
            logging.info("Inserted this vertex:\n{0}".format(callback.result().one()))
        else:
            logging.info("Something went wrong with this query: {0}".format(vertex))

def insert_edges(gremlin_client, EDGES):
    for edge in EDGES:
        callback = gremlin_client.submitAsync(edge)
        if callback.result() is not None:
            logging.info("Inserted this edge:\n{0}".format(callback.result().one()))
        else:
            logging.info("Something went wrong with this query:\n{0}".format(edge))

def execute_traversals(gremlin_client, TRAVERSALS):
    for key in TRAVERSALS:
        logging.info("{0}:".format(key))
        callback = gremlin_client.submitAsync(key)
        for result in callback.result():
            logging.info("\t{0}".format(str(result)))
    return result[0]

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    documents = [myblob.read().decode('utf-8')]

    #logging.info("documents:{}".format(documents))

    try:
        # api_url = "{0}language/analyze-text/jobs?{1}".format(api_endpoint, params)
        # logging.info("API URL:{}".format(api_url))

        # response = requests.post(api_url,
        #             headers=headers,
        #             data=documents)
        
        # logging.info("response:{}".format(response))
        # # Get operation-location in returned headers
        # operation_location = response.headers['operation-location']

        # logging.info("operation_location:{}".format(operation_location))

        # # delay 5 sec to process the GET request
        # time.sleep(5)
        # # Get request to get result of text analysis
        # response = requests.get(operation_location, headers=headers)

        # # store response
        # msft_response = response.json()
        # logging.info("msft_response:{}".format(type(msft_response)))

        # Initialise Germlin client
        logging.info('Initialising Germlin client...')
        gremlin_client = client.Client(
            ENDPOINT, 'g',
            username="/dbs/" + DATABASE + "/colls/" + COLLECTION,
            password=PRIMARY_KEY,
            message_serializer=serializer.GraphSONSerializersV2d0()
            )
        logging.info('Germlin Client initialised!')

        # Purge graph
        cleanup_graph(gremlin_client)

        logging.info('Initialising TA client...')
        TA_client = authenticate_client()
        poller = TA_client.begin_analyze_healthcare_entities(documents)
        result = poller.result()

        docs = [doc for doc in result if not doc.is_error]

        for idx, doc in enumerate(docs):
            i = 0
            for entity in doc.entities:

                VERTICES = [
                    "g.addV("+ f"'{entity.category}'" +").property('id', "+ f"'{str(i)}'" +").property('value', "+ f"'{entity.text}'" +").property('entity',"+ f"'{entity.category}'" +")"
                    ]
                i = i + 1

                logging.info("Entity: {}".format(entity.text))
                logging.info("...Normalized Text: {}".format(entity.normalized_text))
                logging.info("...Category: {}".format(entity.category))
                logging.info("...VERTICES: {}".format(VERTICES))

                # Insert vertices (i.e. nodes)
                insert_vertices(gremlin_client, VERTICES)

            for relation in doc.entity_relations:
                logging.info("Relation: {}".format(relation))
                logging.info("Relation of type: {} has the following roles".format(relation.relation_type))
                mapid = []
                for role in relation.roles:
                    TRAVERSALS = ["g.V().hasLabel("+ f"'{role.entity.category}'" +").has('value',"+f"'{role.entity.text}'"+").properties('id').value()"]
                    r = execute_traversals(gremlin_client, TRAVERSALS)
                    mapid.append(r)
                EDGES = [
                    "g.V(" + f"'{str(mapid[0])}'" + ").addE("+ f"'{relation.relation_type}'" +").to(g.V(" + f"'{str(mapid[1])}'" + "))"
                    ]
                logging.info("...Role '{}' with entity '{}'".format(role.entity.category, role.entity.text))
                logging.info("...EDGES: {}".format(EDGES))

                # Insert edges 
                insert_edges(gremlin_client, EDGES)
            logging.info("Process Finisned !!!!")

    except Exception as e:
        logging.error('Error: {}'.format(e))