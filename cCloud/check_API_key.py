## Created by - SIDDHARTH JAIN

## This code checks the life of API key and alerts if the API key is older than certain threshold timelines
## CRITICAL -> If API key creation date was older than 180 days, raise critical alert
## WARNING -> If API key creation date was older than 90 days, raise warning alert
## OK -> If API key was created lesser than 90 days, show OK status

import http.client
import os
import json
from datetime import datetime, timedelta


def sendGETAPIKeyCall(org_api_key):
    try:

        conn = http.client.HTTPSConnection("api.confluent.cloud")
        headers = { "Authorization": "Basic {}".format(org_api_key) }
        conn.request("GET", "/iam/v2/api-keys", headers=headers)

        res = conn.getresponse()
        data = res.read()

        json_data = json.loads(data.decode("utf-8"))
        return json_data

    except Exception as e:
        print(e)

def evaluateAPIKeyCall(org_api_key, confData):

    sendGETAPIKeyCallResult = sendGETAPIKeyCall(org_api_key)
    now = datetime.today()
    last180 = timedelta(days=180)
    last90 = timedelta(days=90)

    diff180days = now - last180
    diff90days = now - last90
    
    warningDict = dict()
    criticalDict = dict()
    okDict = dict()

    totalAPIKeys = sendGETAPIKeyCallResult["metadata"]["total_size"]

    if totalAPIKeys != 0:
        print("Total number API Keys found - {}".format(totalAPIKeys))
        print("---------------------------------------------------------------------------------------------------------")

        for apiKeyObject in sendGETAPIKeyCallResult['data']:

            created_at = datetime.strptime(apiKeyObject['metadata']['created_at'].replace("T", " ").replace("Z",""), "%Y-%m-%d %H:%M:%S.%f")

            if str(diff180days) > str(created_at):
                criticalDict[apiKeyObject['id']] =  { "api_key_id" : "{}".format(apiKeyObject['id']), "api_key_owner_id": "{}".format(apiKeyObject['spec']['owner']['id']), "api_key_cluster_id": "{}".format(apiKeyObject['spec']['resource']['id']), "api_key_description": "{}".format(apiKeyObject['spec']['description']) }

            elif str(created_at) > str(diff90days):
                okDict[apiKeyObject['id']] =  { "api_key_id" : "{}".format(apiKeyObject['id']), "api_key_owner_id": "{}".format(apiKeyObject['spec']['owner']['id']), "api_key_cluster_id": "{}".format(apiKeyObject['spec']['resource']['id']), "api_key_description": "{}".format(apiKeyObject['spec']['description']) }

            else:
                warningDict[apiKeyObject['id']] =  { "api_key_id" : "{}".format(apiKeyObject['id']), "api_key_owner_id": "{}".format(apiKeyObject['spec']['owner']['id']), "api_key_cluster_id": "{}".format(apiKeyObject['spec']['resource']['id']), "api_key_description": "{}".format(apiKeyObject['spec']['description']) }

        if len(criticalDict) != 0:
            print("CRITICAL - API Keys are older then Critical threshold of 180 days.")
            print("---------------------------------------------------------------------------------------------------------")
            print("Critical API Key List : \n{}".format(criticalDict))
            if len(warningDict) != 0:
                print("Warning API Key List : \n{}".format(warningDict))

        elif len(warningDict) != 0:
            print("WARNING - API Keys are older then Warning thresold of 90 days.")
            print("---------------------------------------------------------------------------------------------------------")
            print("Warning API Key List : \n{}".format(warningDict))
        
        else:
            print("OK - All API Keys are under 180 days threshold limit.")
            print("---------------------------------------------------------------------------------------------------------")
            print("API Key List : \n{}".format(okDict))

    else:
        print("No API Key found.")

    return

def main():

    ## Read Config file
    current_directory = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(current_directory, "env.json")

    conf = open(config_file)
    confData = json.load(conf)
    org_api_key = confData['org_api_key']
    
    evaluateAPIKeyCall(org_api_key, confData)
    return

if __name__ == '__main__':
    main()
