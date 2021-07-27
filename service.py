import json
from datetime import timedelta
import pandas as pd


def swapValue(service, graph, prevgraph, location):
    curTemp = graph.loc[graph['Service'] == service].iloc[0]
    prevTemp = prevgraph.loc[prevgraph['Service'] == service].iloc[0]

    graph.iloc[graph.index[graph['Service'] == service].tolist()[0]] = graph.iloc[location].copy()
    prevgraph.iloc[prevgraph.index[prevgraph['Service'] == service].tolist()[0]] = prevgraph.iloc[location].copy()
    graph.iloc[location] = curTemp
    prevgraph.iloc[location] = prevTemp
    return graph, prevgraph

def lambda_handler(event, context):
    print(event['queryStringParameters'])
    curGraph = pd.DataFrame.from_records(json.loads(event['queryStringParameters']['curGraph']))
    prevGraph = pd.DataFrame.from_records(json.loads(event['queryStringParameters']['prevGraph']))
    print("graph1")
    if prevGraph.empty:
        common = curGraph
        prevGraph = pd.DataFrame(columns=curGraph.columns)
    else:
        common = curGraph.merge(prevGraph, on=['Service'])
    print(common)
    print(prevGraph.columns)
    if prevGraph.empty:
        prevdiff = common
        curdiff = pd.DataFrame(columns=curGraph.columns)
    else:
        prevdiff = curGraph[(~curGraph['Service'].isin(common['Service']))]
        curdiff = prevGraph[(~prevGraph['Service'].isin(common['Service']))]
    curGraph['Neither'] = 0
    prevGraph['Neither'] = 0
    print('differences')
    print(prevdiff)
    print(curdiff)

    for index, row in prevdiff.iterrows():
        emptyBar = pd.Series([row['Service'], 0, 0, 0, 100], index=prevGraph.columns)
        prevGraph = prevGraph.append(emptyBar, ignore_index=True)

    for index, row in curdiff.iterrows():
        emptyBar = pd.Series([row['Service'], 0, 0, 0, 100], index=curGraph.columns)
        curGraph = curGraph.append(emptyBar, ignore_index=True)

    curTemp = curGraph.loc[curGraph['Service'] == 'GEN'].iloc[0]
    prevTemp = prevGraph.loc[prevGraph['Service'] == 'GEN'].iloc[0]
    curGraph.iloc[curGraph.index[curGraph['Service'] == 'GEN'].tolist()[0]] = curGraph.iloc[0].copy()
    prevGraph.iloc[prevGraph.index[prevGraph['Service'] == 'GEN'].tolist()[0]] = prevGraph.iloc[0].copy()
    curGraph.iloc[0] = curTemp
    prevGraph.iloc[0] = prevTemp

    curTemp = curGraph.loc[curGraph['Service'] == 'GYN/OBS'].iloc[0]
    prevTemp = prevGraph.loc[prevGraph['Service'] == 'GYN/OBS'].iloc[0]
    curGraph.iloc[curGraph.index[curGraph['Service'] == 'GYN/OBS'].tolist()[0]] = curGraph.iloc[1].copy()
    prevGraph.iloc[prevGraph.index[prevGraph['Service'] == 'GYN/OBS'].tolist()[0]] = prevGraph.iloc[1].copy()
    curGraph.iloc[1] = curTemp
    prevGraph.iloc[1] = prevTemp

    if (curGraph['Service'] == 'ENDO').any() & (curGraph['Service'] == 'CYSTO').any():
        print('BOTH')
        (curGraph, prevGraph) = swapValue('ENDO', curGraph, prevGraph, -1)
        (curGraph, prevGraph) = swapValue('CYSTO', curGraph, prevGraph, -2)
        print(curGraph)
        print(prevGraph)
        curGraph[2: len(curGraph) - 2] = curGraph[2: len(curGraph) - 2].sort_values('Service')
        prevGraph[2: len(prevGraph) - 2] = prevGraph[2: len(curGraph) - 2].sort_values('Service')

    elif (curGraph['Service'] == 'ENDO').any():
        print('endo')
        (curGraph, prevGraph) = swapValue('ENDO', curGraph, prevGraph, -1)
        print(curGraph)
        print(prevGraph)
        curGraph[2:len(curGraph) - 1] = curGraph[2: len(curGraph) - 1].sort_values('Service')
        prevGraph[2:len(prevGraph) - 1] = prevGraph[2: len(prevGraph) - 1].sort_values('Service')

    elif (curGraph['Service'] == 'CYSTO').any():
        print('cysto')
        (curGraph, prevGraph) = swapValue('CYSTO', curGraph, prevGraph, -1)
        print(curGraph)
        print(prevGraph)
        curGraph[2:len(curGraph) - 1] = curGraph[2: len(curGraph) - 1].sort_values('Service')
        prevGraph[2:len(prevGraph) - 1] = prevGraph[2: len(prevGraph) - 1].sort_values('Service')

    else:
        print('neither')
        curGraph[2:] = curGraph[2:].sort_values('Service')
        prevGraph[2:] = prevGraph[2:].sort_values('Service')

    curGraph = curGraph.to_dict('records')
    prevGraph = prevGraph.to_dict('records')
    responseBody = {
        'curGraph': curGraph,
        'prevGraph': prevGraph
    }
    print(responseBody)
    return {
        'statusCode': 200,
        'headers': {

            'Access-Control-Allow-Headers': 'Content-Type',

            'Access-Control-Allow-Origin': '*',

            'Access-Control-Allow-Methods': 'OPTIONS,GET'

        },
        'body': json.dumps(responseBody)
    }
