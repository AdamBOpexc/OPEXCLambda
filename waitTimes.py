import json
import math
from datetime import timedelta
import pandas as pd
import pymysql
from sqlalchemy import create_engine


def lambda_handler(event, context):
    db_connection_str = 'mysql+pymysql://admin:SherpaPw123@sherpa-database.cj5ympuvmpqg.us-east-2.rds.amazonaws.com:' \
                        '3306/Sherpa'
    db_connection = create_engine(db_connection_str)
    startDate = event['queryStringParameters']['startdate']
    endDate = event['queryStringParameters']['enddate']
    sqlQuery = 'call getSurgeries(\'{}\', \'{}\')'.format(startDate, endDate)
    print(sqlQuery)
    Surgeries = pd.read_sql(sqlQuery, con=db_connection_str)
    Surgeries['WaitTime'] = (Surgeries['Surgery Date'] - Surgeries['Booking Date']).dt.days

    AverageWait = Surgeries['WaitTime'].mean()
    quantile = Surgeries['WaitTime'].quantile(q=0.9)
    print(AverageWait)
    print(quantile)
    responseBody = {
        'AvgWaitTime': math.floor(AverageWait),
        '90thPercentile': math.floor(quantile),
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

