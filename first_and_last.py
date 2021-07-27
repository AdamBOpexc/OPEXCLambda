import json
import calendar
from datetime import timedelta
import pandas as pd
import pymysql
from sqlalchemy import create_engine


def lambda_handler(event, context):
    db_connection_str = 'mysql+pymysql://admin:SherpaPw123@sherpa-database.cj5ympuvmpqg.us-east-2.rds.amazonaws.com:' \
                        '3306/Sherpa'
    db_connection = create_engine(db_connection_str)
    sqlQuery = 'call get_first_and_last()'
    Dates = pd.read_sql(sqlQuery, con=db_connection_str)
    print(Dates)
    print(type(Dates))

    responseBody = {
        'StartDate': Dates.iloc[0]['Min'],
        'EndDate': Dates.iloc[0]['Max']
    }
    return {
        'statusCode': 200,
        'headers': {

            'Access-Control-Allow-Headers': 'Content-Type',

            'Access-Control-Allow-Origin': '*',

            'Access-Control-Allow-Methods': 'OPTIONS,GET'

        },
        'body': json.dumps(responseBody)
    }
