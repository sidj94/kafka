## Created by - SIDDHARTH JAIN

## This code checks the number of partitions in a cluster.
## If the number of partititons are >=80% of the total available capacity, a WARNING will be displayed
## If the number of partitions are >=100% of the total available capacity, a CRITICAL message will be displayed


import http.client
import json
import os

## CONSTANTS
basic_cluster_parition_max_limit = 2048
standard_cluster_parition_max_limit = 2048
dedicated_cluster_parition_max_limit_per_cku = 4500
dedicated_cluster_parition_max_limit = 100000

warn_basic_cluster_parition_max_limit = int(2048*80/100)
warn_standard_cluster_parition_max_limit = int(2048*80/100)


## Function to send API Call on organization level
def orgAPICall(org_api_key, env_id):
    try:
        conn = http.client.HTTPSConnection("api.confluent.cloud")
        headers = { "Authorization": "Basic {}".format(org_api_key) }
        conn.request("GET", "/cmk/v2/clusters?environment={}".format(env_id), headers=headers)

        res = conn.getresponse()
        data = res.read()

        json_data = json.loads(data.decode("utf-8"))
        return json_data

    except Exception as e:
        print(e)


## Function to send API Call on cluster level
def clusterAPICall(kafka_endpoint, cluster_api_key, cluster_id):
    try:
        conn = http.client.HTTPSConnection("{}".format(kafka_endpoint))
        headers = { "Authorization": "Basic {}".format(cluster_api_key) }
        conn.request("GET", "/kafka/v3/clusters/{}/topics".format(cluster_id), headers=headers)

        res = conn.getresponse()
        data = res.read()

        json_data = json.loads(data.decode("utf-8"))
        return json_data

    except Exception as e:
        print(e)
 

def createClusterObject(org_api_key, env_id):
    requestData = orgAPICall(org_api_key, env_id)
    clusterDetails = requestData['data']

    clusterObject = dict()

    for cluster in clusterDetails:
        value = dict()
        value['cluster_name'] = cluster['spec']['display_name']
        value['cluster_id'] = cluster['id']
        value['cluster_type'] = cluster['spec']['config']['kind']
        value['cluster_environment_id'] = cluster['spec']['environment']['id']

        kafka_bootstrap_endpoint = cluster['spec']['http_endpoint']
        kafka_endpoint = kafka_bootstrap_endpoint.split(':')[1].replace("//", '')
        value['kafka_endpoint'] = kafka_endpoint

        if "cku" not in cluster['status']:
            value['cluster_cku'] = 1
        else:
            value['cluster_cku'] = cluster['status']['cku']

        clusterObject[cluster['spec']['display_name']] = value

    return clusterObject


def calcaulateTotalParitions(kafka_endpoint, cluster_api_key, cluster_id):
    apiCallResult = clusterAPICall(kafka_endpoint, cluster_api_key, cluster_id)
    totalParition = 0
    partitionDict = dict()

    for numberOfPartition in apiCallResult['data']:
        partitionDict.update({numberOfPartition['topic_name'] : numberOfPartition['partitions_count']})
        totalParition += numberOfPartition['partitions_count']

    return totalParition


def evaluateClusterObject(org_api_key, confData):
    try:
        env_id = confData['env_id']
        clusterObject = createClusterObject(org_api_key, env_id)
        for k,v in clusterObject.items():
            cluster_id = v['cluster_id']
            kafka_endpoint = v['kafka_endpoint']
            cluster_api_key = confData['{}_api_key'.format(v['cluster_name'])]
            numberOfPartitions = calcaulateTotalParitions(kafka_endpoint, cluster_api_key, cluster_id)

            if v['cluster_cku'] == 1 and v['cluster_type'].upper() != 'DEDICATED':
                if numberOfPartitions >= basic_cluster_parition_max_limit:
                    print("CRITICAL - Number of partitions are exceeding maimum limit of {}.\nPartition Count - {}".format(basic_cluster_parition_max_limit, numberOfPartitions))
                elif numberOfPartitions <= warn_basic_cluster_parition_max_limit:
                    print("OK - Partitions count is under limits.\nPartition Count - {}".format(numberOfPartitions))
                else:
                    print("WARN - Number of partitions are exceeding warning threshold of {}.\nPartition Count - {}".format(warn_basic_cluster_parition_max_limit, numberOfPartitions))

            elif v['cluster_type'].upper() == 'DEDICATED':
                cluster_cku = v['cluster_cku']
                maxPartition = int(cluster_cku * dedicated_cluster_parition_max_limit_per_cku)

                if maxPartition > 100000:
                    maxPartition = 100000
                
                warn_dedicated_partition_limit = int(maxPartition*80/100)
                warn_max_dedicated_partition_limit = int(dedicated_cluster_parition_max_limit*80/100)

                if numberOfPartitions >= dedicated_cluster_parition_max_limit:
                    print("CRITICAL - Number of partitions are exceeding maximum limit of {}\nPartition Count - {}".format(dedicated_cluster_parition_max_limit, numberOfPartitions))
                elif numberOfPartitions >= warn_max_dedicated_partition_limit:
                    print("WARN - Number of parititions are exceeding warning threshold of {}.\nPartition Count - {}".format(warn_max_dedicated_partition_limit, numberOfPartitions))
                else:
                    if numberOfPartitions >= maxPartition:
                        print("CRITICAL - Number of partitions are exceeding limits maximum limit of {}".format(dedicated_cluster_parition_max_limit))
                    elif numberOfPartitions <= warn_dedicated_partition_limit:
                        print("OK - Partitions count is under limits.\nPartition Count - {}".format(numberOfPartitions))
                    else:
                        print("WARN - Number of partitions are exceeding warning threshold of {}.\nPartition Count - {}".format(warn_dedicated_partition_limit, numberOfPartitions))

            else:
                print("UNKNOWN - Failed due to some unexpected issue")
 
    except Exception as e:
        print(e)

    return

def main():

    ## Read Config file
    current_directory = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(current_directory, "env.json")

    conf = open(config_file)
    confData = json.load(conf)
    org_api_key = confData['org_api_key']
    
    evaluateClusterObject(org_api_key, confData)
    return

if __name__ == '__main__':
    main()
