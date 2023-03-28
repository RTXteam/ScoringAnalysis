import xlsxwriter
import pandas as pd
import json
import ijson

# Index in Array where to place the score of each ARA
# Index + 1 will store the normalized score of the ARA
# Index 0 will store Disease
# Index 1 will store Drug

ara_indexing = {
            "arax"                :  2,
            "explanatory_agent"   :  4,
            "ia"                  :  6,
            "unsecret_agent"      :  8,
            "aragorn"             :  10,
            "robokop"             :  12,
            "bte"                 :  14
             }


def get_ara_provider(row):
    """
    Function that returns the ARA a query was sent to
    Will return None if no ARA could be tagged to the query
    """
    if "message" not in row['data']:
        return
    message = row['data']['message']
    if 'results' not in message:
        return

    elif not message['results']:
        return

    elif "reasoner_id" in message['results'][0]:
        if message['results'][0]["reasoner_id"] == "ARAX":
            return "arax"
        elif message['results'][0]["reasoner_id"] == "Explanatory Agent":
            return "explanatory_agent"

    edges = message["knowledge_graph"]["edges"]
    key = list(edges.keys())[0]
    attribute_list = edges[key]['attributes']
    for attribute in attribute_list:
        if 'value' not in attribute:
            continue
        label = attribute['value']
        if isinstance(label, str):
            if label == "infores:openpredict":
                return "op"
            elif label == "infores:improving-agent":
                return "ia"
            elif label == "infores:molepro":
                return "molepro"
            elif label == "infores:unsecret-agent":
                return "unsecret_agent"
            elif label == "infores:genetics-data-provider":
                return "geneticskp"
            elif label == "infores:aragorn" in label:
                return "aragorn"
        elif isinstance(label, list):
            if "infores:biothings-explorer" in label:
                return "bte"
            elif "infores:automat-robokop" in label:
                return "robokop"
    #Print ids of queries which weren't tagged to any  ARA 
    print(row['id'])



def convert_to_dataframe(data):
    final_output = []
    for disease, drugs in data.items():
        for drug,values in drugs.items():
            final_output.append(values)
    return final_output


def write_to_excel(data):
    headers = ["Disease", "Drug", "ARAX", 	"ARAX NS", "EA", "EA NS", "IA", "IA NS", "UA", "UA NS",
                	"AR", "AR NS",	"ROBO", "ROBO NS", "BTE", "BTE NS"]
    writer = pd.ExcelWriter('results-script.xlsx', engine='xlsxwriter')
    data.to_excel(writer, sheet_name='Sheet1', header=headers, index=False)
    writer.save()


def score_parser(json_data,ara):
    query_node = json_data['data']['message']['query_graph']['nodes']
    if 'on' in query_node:
        disease_idx = query_node['on']['ids'][0]
    elif 'n0' in query_node:
        disease_idx = query_node['n0']['ids'][0]
    elif 'disease' in query_node:
        disease_idx = query_node['disease']['ids'][0]
    else:
        print(query_node)
        raise Exception("Could not get Disease")
            
    if disease_idx not in output_data:
        output_data[disease_idx] = dict()
    for result in json_data['data']['message']['results']:
        node_bindings = result['node_bindings']
        if 'sn' in node_bindings:
            drug_idx = node_bindings['sn'][0]['id']
        elif 'n1' in node_bindings:
            drug_idx = node_bindings['n1'][0]['id']
        elif 'drug' in node_bindings:
            drug_idx = node_bindings['drug'][0]['id']
        elif "chemical" in node_bindings:
            drug_idx = node_bindings['chemical'][0]['id']
        else:
            print(node_bindings)
            raise Exception("Could not get Drug")
            
        if drug_idx not in output_data[disease_idx]:
            output_data[disease_idx][drug_idx] = [disease_idx, drug_idx, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        
        #Noticed some Aragorn queries did not have scores in the results
        if 'score' not in result:
            return "error"
        score = result['score']
        if 'normalized_score' in result:
            normalized_score = result['normalized_score']
        else:
            normalized_score = 0
        output_data[disease_idx][drug_idx][ara_indexing[ara]] = score
        output_data[disease_idx][drug_idx][ara_indexing[ara] + 1] = normalized_score
    return "success"




if __name__ == "__main__":
    # output_data will store all the scores from all ARAs during processing
    output_data = dict()
    
    # data.json is the sqldump in json format
    with open('data.json') as fp:
        for x in ijson.items(fp, "item"):
            try:
                if 'message' not in x['data']:
                    continue

                #Some queries aren't about Drugs to treat Disease
                if "biolink:treats" not in str(x['data']['message']['query_graph']):
                    continue
                
                ara = get_ara_provider(x)
                if ara not in list(ara_indexing.keys()):
                    continue

                response = score_parser(x,ara)
                if "response" == "error":
                    continue
            except Exception as err:
                print(f"ID: {x['id']} Unexpected {err=}, {type(err)=}")
                raise
                
        y = convert_to_dataframe(output_data)
        df = pd.DataFrame(y)
        write_to_excel(df)
